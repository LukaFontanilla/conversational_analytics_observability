# Looker Conversation Ingestion Service

This service automates the ingestion of AI agent conversation data from Looker into BigQuery. It is designed to run as a serverless container on Google Cloud Run, triggered by Cloud Scheduler.

## Key Features

- **Parallel Processing**: Utilizes a worker pool (50 workers) to process multiple users concurrently using Looker's `sudo` functionality.
- **Efficient Ingestion**: Consolidates all fetched conversations from all users into a **single BigQuery load job** using a streaming-channel pattern. This minimizes API overhead, avoids rate limits, and reduces costs.
- **Data Optimization**: Automatically prunes heavy objects from conversation messages (like `debugInfo`, redundant `systemMessage` copies, and large result sets) to minimize BigQuery storage costs while preserving analytical value.
- **Incremental Sync**: Supports both `/daily` (today's data) and `/historical` (all data) sync modes.

## Architecture

1. **Admin Login**: Authenticates with the Looker API as an admin.
2. **User Discovery (System Activity)**: Identifies users who have interacted with Conversational Analytics by running a predefined **Looker System Activity query**. This ensures we only process active users and maintains accurate usage logs.
3. **Parallel Sudo**: Workers impersonate identified users in parallel to access their private conversation history.
4. **Detail Fetching**: Retrieves full conversation details, including message history.
5. **Message Pruning**: The `PruneAgentMessages` function strips out internal metadata and heavy schemas before storage.
6. **Streaming Upload**: A Go channel streams all processed data into a single multipart BigQuery upload request.

## Setup & Deployment

### 1. BigQuery Table
Initialize your BigQuery table and explore analysis patterns using [bigquery_schema.sql](file:///usr/local/google/home/maluka/ca-eval-app/backend/dailyCron/bigquery_schema.sql). 

The SQL file includes:
- **Table Creation**: Partitioned by date and clustered by user/agent.
- **Analysis Queries**: 
  - Unnesting JSON messages for granular analysis.
  - Aggregating conversation counts by agent and date.
  - **Latency Analysis**: Calculating "Time to First Thought" and "Total User Wait Time".

## Local Development

1. Copy the example environment file:
   ```bash
   cp .env_example .env
   ```
2. Fill in your credentials and configuration in the `.env` file.
3. Run the service:

### 2. Environment Variables

| Variable | Description |
|----------|-------------|
| `LOOKER_CLIENT_ID` | Looker API Client ID |
| `LOOKER_CLIENT_SECRET` | Looker API Client Secret |
| `LOOKER_BASE_URL` | Looker Instance URL (e.g. `https://instance.looker.com`) |
| `GCP_PROJECT_ID` | Google Cloud Project ID |
| `BQ_DATASET` | BigQuery Dataset ID |
| `BQ_TABLE` | BigQuery Table ID |
| `LOOKER_USER_QUERY_ID` | Looker System Activity Query ID for User Discovery |
| `PORT` | Port for the server (default `8080`) |

### 3. Deploy to Cloud Run (Deploy from Source)

The easiest way to deploy is directly from your local source code. This command builds the image in the cloud and deploys it in one step:

```bash
gcloud run deploy daily-cron \
  --source . \
  --platform managed \
  --region [REGION] \
  --no-allow-unauthenticated \
  --update-env-vars LOOKER_CLIENT_ID=[ID],LOOKER_CLIENT_SECRET=[SECRET],LOOKER_BASE_URL=[URL],GCP_PROJECT_ID=[PROJECT],BQ_DATASET=[DATASET],BQ_TABLE=[TABLE]
```

### 4. Scheduling the Daily Job

Automate the `/daily` endpoint trigger using **Cloud Scheduler**:
```bash
gcloud scheduler jobs create http daily-conv-sync \
  --schedule="0 1 * * *" \
  --uri="https://[service-url]/daily" \
  --http-method=POST \
  --oidc-service-account-email=[SERVICE_ACCOUNT_EMAIL] \
  --time-zone="UTC"
```

## Endpoints

- `POST /daily`: Syncs conversations created today.
- `POST /historical`: Syncs all available conversations.
- **Dry Run**: Add `?dry-run=true` to any request to test the flow without inserting data into BigQuery.
