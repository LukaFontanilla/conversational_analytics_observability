package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

// Config holds the application configuration from environment variables
type Config struct {
	LookerClientID     string
	LookerClientSecret string
	LookerBaseURL      string
	GCPProjectID       string
	GCPRegion          string
	BQDataset          string
	BQTable            string
	LookerUserQueryID  string
}

// Data structures for Looker API
type User struct {
	ID    int    `json:"user.id"`
	Email string `json:"user.email"`
}

type Conversation struct {
	ID                string      `json:"id"`
	UserID            string      `json:"user_id"`
	AgentID           string      `json:"agent_id"`
	Name              string      `json:"name"`
	Sources           interface{} `json:"sources"`
	CreatedAt         string      `json:"created_at"`
	UpdatedAt         string      `json:"updated_at"`
	Messages          interface{} `json:"messages"`
	ConversationAgent interface{} `json:"conversation_agent"`
}

type AuthToken struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

// BQInsertRequest is the payload for Tabledata: insertAll
type BQInsertRequest struct {
	Rows []BQRow `json:"rows"`
}

type BQRow struct {
	InsertID string      `json:"insertId"`
	Json     interface{} `json:"json"`
}

const (
	WorkerPoolSize = 50
	HTTPTimeout    = 30 * time.Second
)

var (
	config Config
	client = &http.Client{Timeout: HTTPTimeout}
)

func PruneAgentMessages(messages []any) []any {
	for _, m := range messages {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}

		// Access the "message" wrapper
		msgBody, ok := msg["message"].(map[string]any)
		if !ok {
			continue
		}

		// 1. Handle USER messages
		if _, isUser := msgBody["userMessage"]; isUser {
			delete(msgBody, "debugInfo")
			continue
		}

		// 2. Handle SYSTEM messages
		if msg["type"] == "system" {
			// Remove redundant top-level systemMessage
			delete(msgBody, "systemMessage")

			// Navigate: message -> debugInfo -> response -> data -> systemMessage
			debugInfo, ok := msgBody["debugInfo"].(map[string]any)
			if !ok {
				continue
			}
			delete(debugInfo, "request")

			response, ok := debugInfo["response"].(map[string]any)
			if !ok {
				continue
			}
			respData, ok := response["data"].(map[string]any)
			if !ok {
				continue
			}
			sysMsg, ok := respData["systemMessage"].(map[string]any)
			if !ok {
				continue
			}

			// Clean up thoughts and signatures
			if textObj, ok := sysMsg["text"].(map[string]any); ok {
				delete(textObj, "thoughtSignature")
			}
			delete(sysMsg, "thoughtSignature")

			// --- HEAVY OBJECT PRUNING ---

			// A. Handle Schema type messages
			if schemaObj, ok := sysMsg["schema"].(map[string]any); ok {
				delete(schemaObj, "result")
				delete(schemaObj, "datasources")
			}

			// B. Handle Data/Result type messages
			if dataObj, ok := sysMsg["data"].(map[string]any); ok {
				delete(dataObj, "result")
				delete(dataObj, "formattedData")

				// NEW: Handle schemas hidden inside "query" -> "datasources"
				if queryObj, ok := dataObj["query"].(map[string]any); ok {
					if ds, ok := queryObj["datasources"].([]any); ok {
						for _, item := range ds {
							if dsMap, ok := item.(map[string]any); ok {
								delete(dsMap, "schema") // Remove the schema from the datasource reference
							}
						}
					}
				}
			}

			// C. Handle Chart type messages
			if chartObj, ok := sysMsg["chart"].(map[string]any); ok {
				delete(chartObj, "result")
			}
		}
	}
	return messages
}


func main() {
	loadConfig()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/daily", handleDaily)
	http.HandleFunc("/historical", handleHistorical)

	log.Printf("Server listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleDaily(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dryRun := r.URL.Query().Get("dry-run") == "true"
	log.Printf("Triggering daily sync (Dry Run: %v)", dryRun)

	go func() {
		err := runSync("daily", dryRun)
		if err != nil {
			log.Printf("Daily sync failed: %v", err)
		}
	}()

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, "Daily sync triggered")
}

func handleHistorical(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dryRun := r.URL.Query().Get("dry-run") == "true"
	log.Printf("Triggering historical sync (Dry Run: %v)", dryRun)

	go func() {
		err := runSync("historical", dryRun)
		if err != nil {
			log.Printf("Historical sync failed: %v", err)
		}
	}()

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, "Historical sync triggered")
}

func runSync(mode string, dryRun bool) error {
	log.Printf("Starting conversion fetcher in %s mode (Dry Run: %v)", mode, dryRun)

	// 1. Login as Admin
	adminToken, err := lookerLogin(config.LookerClientID, config.LookerClientSecret)
	if err != nil {
		return fmt.Errorf("failed to login as admin: %v", err)
	}

	// 2. Search for Users
	users, err := searchUsers(adminToken)
	if err != nil {
		return fmt.Errorf("failed to fetch users: %v", err)
	}
	log.Printf("Found %d users to process", len(users))

	// 3. Process Users in Parallel
	userChan := make(chan User, len(users))
	convChan := make(chan []Conversation, len(users))
	var wg sync.WaitGroup

	// Start Workers
	for i := 0; i < WorkerPoolSize; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for user := range userChan {
				userID := fmt.Sprintf("%d", user.ID)
				conversations, err := processUser(adminToken, user, mode, userID)
				if err != nil {
					log.Printf("[Worker %d] Error processing user %d (%s): %v", workerID, user.ID, user.Email, err)
					continue
				}
				if len(conversations) > 0 {
					convChan <- conversations
				}
			}
		}(i)
	}

	// Feed users to channels
	for _, user := range users {
		userChan <- user
	}
	close(userChan)

	// Close convChan once all workers are done
	go func() {
		wg.Wait()
		close(convChan)
	}()

	// 4. Collect and store results
	userCount := 0
	totalConversations := 0

	if dryRun {
		for conversations := range convChan {
			userCount++
			totalConversations += len(conversations)
		}
		log.Printf("Dry Run: Would have uploaded %d total conversations from %d users in a single job", totalConversations, userCount)
	} else {
		var err error
		totalConversations, err = insertToBigQuery(convChan)
		if err != nil {
			log.Printf("Error during BigQuery batch upload: %v", err)
		}
		// Note: userCount is not easily tracked when passing channel, but totalConversations is returned.
	}

	log.Printf("Finished %s sync. Processed %d total conversations.", mode, totalConversations)
	return nil
}

func loadConfig() {
	if os.Getenv("LOCAL_DEV") == "true" {
		if err := godotenv.Load(); err != nil {
			log.Printf("Warning: error loading .env file: %v", err)
		} else {
			log.Println("Loaded environment variables from .env file")
		}
	}
	config = Config{
		LookerClientID:     os.Getenv("LOOKER_CLIENT_ID"),
		LookerClientSecret: os.Getenv("LOOKER_CLIENT_SECRET"),
		LookerBaseURL:      os.Getenv("LOOKER_BASE_URL"),
		GCPProjectID:       os.Getenv("GCP_PROJECT_ID"),
		GCPRegion:          os.Getenv("GCP_REGION"),
		BQDataset:          os.Getenv("BQ_DATASET"),
		BQTable:            os.Getenv("BQ_TABLE"),
		LookerUserQueryID:  os.Getenv("LOOKER_USER_QUERY_ID"),
	}

	if config.LookerClientID == "" || config.LookerClientSecret == "" || config.LookerBaseURL == "" || config.LookerUserQueryID == "" {
		log.Fatal("Looker credentials, Base URL, and User Query ID are required via environment variables")
	}
}

func lookerLogin(clientID, clientSecret string) (string, error) {
	url := fmt.Sprintf("%s/api/4.0/login", config.LookerBaseURL)
	payload := fmt.Sprintf("client_id=%s&client_secret=%s", clientID, clientSecret)
	req, _ := http.NewRequest("POST", url, bytes.NewBufferString(payload))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("login failed with status %d: %s", resp.StatusCode, string(b))
	}

	var token AuthToken
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return "", err
	}
	log.Printf("Looker login successful. Token prefix: %s...", token.AccessToken[:10])
	return token.AccessToken, nil
}

func searchUsers(token string) ([]User, error) {
	url := fmt.Sprintf("%s/api/4.0/queries/%s/run/json?limit=-1", config.LookerBaseURL, config.LookerUserQueryID)
	log.Printf("Running user search query at: %s", url)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", "token "+token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Printf("Search users failed with status %d: %s", resp.StatusCode, string(b))
		return nil, fmt.Errorf("search users failed with status %d", resp.StatusCode)
	}

	var users []User
	if err := json.NewDecoder(resp.Body).Decode(&users); err != nil {
		return nil, err
	}
	log.Printf("Successfully found %d users", len(users))
	return users, nil
}

func processUser(adminToken string, user User, mode string, userID string) ([]Conversation, error) {
	// 1. Sudo login for user
	userToken, err := sudoLogin(adminToken, userID)
	if err != nil {
		return nil, fmt.Errorf("sudo failed: %w", err)
	}
	defer logout(userToken) // Best practice to logout sudo session

	// 2. Search conversations
	conversations, err := searchConversations(userToken, mode)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	if len(conversations) == 0 {
		return nil, nil
	}

	// 3. Fetch details for each conversation
	var detailed []Conversation
	for _, c := range conversations {
		detail, err := getConversationDetail(userToken, c.ID)
		if err != nil {
			log.Printf("Failed to get detail for conversation %s: %v", c.ID, err)
			continue
		}
		detailed = append(detailed, detail)
	}

	return detailed, nil
}

func sudoLogin(adminToken string, userID string) (string, error) {
	url := fmt.Sprintf("%s/api/4.0/login/%s", config.LookerBaseURL, userID)
	req, _ := http.NewRequest("POST", url, nil)
	req.Header.Add("Authorization", "token "+adminToken)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("sudo failed with status %d: %s", resp.StatusCode, string(b))
	}

	var token AuthToken
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

func logout(token string) {
	url := fmt.Sprintf("%s/api/4.0/logout", config.LookerBaseURL)
	req, _ := http.NewRequest("DELETE", url, nil)
	req.Header.Add("Authorization", "token "+token)
	resp, err := client.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func searchConversations(token, mode string) ([]Conversation, error) {
	url := fmt.Sprintf("%s/api/4.0/conversations/search", config.LookerBaseURL)
	
	// Add date filtering based on mode
	if mode == "daily" {
		today := time.Now().Format("2006-01-02")
		url = fmt.Sprintf("%s?created_at=%s", url, today)
	}
	// Historical mode fetches everything (or could take a date flag)

	log.Printf("[searchConversations] Requesting URL: %s", url)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", "token "+token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	log.Printf("[searchConversations] Response Status: %d", resp.StatusCode)
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("[searchConversations] Error Body: %s", string(body))
		return nil, fmt.Errorf("search failed with status %d", resp.StatusCode)
	}

	var conversations []Conversation
	if err := json.Unmarshal(body, &conversations); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w (Body: %s)", err, string(body))
	}
	
	log.Printf("[searchConversations] Successfully decoded %d conversations", len(conversations))
	return conversations, nil
}

func getConversationDetail(token, id string) (Conversation, error) {
	fields := "id,agent_id,user_id,name,sources,created_at,updated_at,messages,conversation_agent"
	url := fmt.Sprintf("%s/api/4.0/conversations/%s?fields=%s", config.LookerBaseURL, id, fields)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", "token "+token)

	resp, err := client.Do(req)
	if err != nil {
		return Conversation{}, err
	}
	defer resp.Body.Close()

	var detail Conversation
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		return Conversation{}, err
	}

	if detail.Messages != nil {
		if msgs, ok := detail.Messages.([]any); ok {
			detail.Messages = PruneAgentMessages(msgs)
		}
	}

	return detail, nil
}


func insertToBigQuery(convChan <-chan []Conversation) (int, error) {
	totalRows := 0
	
	gcpToken, err := getGCPToken()
	if err != nil {
		return 0, fmt.Errorf("failed to get GCP token: %w", err)
	}

	// 1. Prepare NDJSON data from channel
	var ndjson bytes.Buffer
	for data := range convChan {
		for _, c := range data {
			b, err := json.Marshal(c)
			if err != nil {
				log.Printf("Warning: failed to marshal conversation %s: %v", c.ID, err)
				continue
			}
			ndjson.Write(b)
			ndjson.WriteByte('\n')
			totalRows++
		}
	}

	if totalRows == 0 {
		log.Printf("[insertToBigQuery] No rows to upload, skipping job creation")
		return 0, nil
	}

	log.Printf("[insertToBigQuery] Attempting single batch load for %d total rows", totalRows)

	// 2. Prepare Multipart Request
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	
	// Part 1: Metadata
	metadataHeader := make(textproto.MIMEHeader)
	metadataHeader.Set("Content-Type", "application/json; charset=UTF-8")
	metadataPart, err := writer.CreatePart(metadataHeader)
	if err != nil {
		return totalRows, err
	}
	
	jobConfig := map[string]interface{}{
		"configuration": map[string]interface{}{
			"load": map[string]interface{}{
				"destinationTable": map[string]string{
					"projectId": config.GCPProjectID,
					"datasetId": config.BQDataset,
					"tableId":   config.BQTable,
				},
				"sourceFormat":     "NEWLINE_DELIMITED_JSON",
				"writeDisposition": "WRITE_APPEND",
			},
		},
	}
	json.NewEncoder(metadataPart).Encode(jobConfig)

	// Part 2: Data
	dataHeader := make(textproto.MIMEHeader)
	dataHeader.Set("Content-Type", "application/octet-stream")
	dataPart, err := writer.CreatePart(dataHeader)
	if err != nil {
		return totalRows, err
	}
	dataPart.Write(ndjson.Bytes())
	
	writer.Close()

	// 3. Send Request
	url := fmt.Sprintf("https://bigquery.googleapis.com/upload/bigquery/v2/projects/%s/jobs?uploadType=multipart", config.GCPProjectID)
	req, _ := http.NewRequest("POST", url, &body)
	req.Header.Add("Authorization", "Bearer "+gcpToken)
	req.Header.Add("Content-Type", "multipart/related; boundary="+writer.Boundary())

	log.Printf("[insertToBigQuery] Creating load job at %s", url)
	resp, err := client.Do(req)
	if err != nil {
		return totalRows, err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return totalRows, fmt.Errorf("BQ job creation failed with status %d: %s", resp.StatusCode, string(b))
	}

	log.Printf("[insertToBigQuery] Successfully created job. Response: %s", string(b))
	return totalRows, nil
}

func getGCPToken() (string, error) {
	// Check for local development token
	if token := os.Getenv("GCP_ACCESS_TOKEN"); token != "" {
		log.Println("Using GCP access token from GCP_ACCESS_TOKEN environment variable")
		return token, nil
	}

	// Simple call to metadata server
	req, _ := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", nil)
	req.Header.Add("Metadata-Flavor", "Google")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.AccessToken, nil
}
