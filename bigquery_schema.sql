-- BigQuery SQL Script to create the conversations table
-- Replace [PROJECT_ID], [DATASET_ID], and [TABLE_NAME] with your actual values.

CREATE TABLE IF NOT EXISTS `[PROJECT_ID].[DATASET_ID].[TABLE_NAME]` (
  id STRING OPTIONS(description="The unique identifier for the conversation"),
  user_id INT64 OPTIONS(description="The ID of the user who owns the conversation"),
  agent_id STRING OPTIONS(description="The ID of the agent associated with the conversation"),
  name STRING OPTIONS(description="The name of the conversation"),
  sources JSON OPTIONS(description="Source data for the conversation in JSON format"),
  created_at TIMESTAMP OPTIONS(description="The timestamp when the conversation was created"),
  updated_at TIMESTAMP OPTIONS(description="The timestamp when the conversation was last updated"),
  messages JSON OPTIONS(description="The list of messages in the conversation in JSON format"),
  conversation_agent JSON OPTIONS(description="Detailed agent metadata for the conversation in JSON format")
)
PARTITION BY DATE(created_at)
CLUSTER BY user_id, agent_id;

-- Ex BigQuery SQL Script to unnest messages and look at individual message level detail for a conversation
-- Replace [PROJECT_ID], [DATASET_ID], and [TABLE_NAME] with your actual values. 
-- Replace [CONVERSATION_ID] with the actual conversation ID.

SELECT
  id AS conversation_id,
  JSON_VALUE(msg, '$.id') AS message_id,
  JSON_VALUE(msg, '$.type') AS message_type,
  CAST(JSON_VALUE(msg, '$.order') AS INT64) AS message_order,
  
  -- 1. Identify the 'kind' (text, schema, data, chart)
  JSON_VALUE(msg, '$.message.debugInfo.response.data.systemMessage.kind') AS system_kind,

  -- 2. Extract AI Thoughts/Logic
  JSON_VALUE(msg, '$.message.debugInfo.response.data.systemMessage.text.parts[0]') AS ai_thought_process,

  -- 3. Extract the Final Response provided to the user
  CASE 
    WHEN JSON_VALUE(msg, '$.message.debugInfo.response.data.systemMessage.text.textType') = 'FINAL_RESPONSE' 
    THEN JSON_VALUE(msg, '$.message.debugInfo.response.data.systemMessage.text.parts[0]')
    ELSE NULL 
  END AS final_answer,

  -- 4. Extract Looker specific metadata (Explore and Model used)
  JSON_VALUE(msg, '$.message.debugInfo.response.data.systemMessage.data.query.looker.model') AS looker_model,
  JSON_VALUE(msg, '$.message.debugInfo.response.data.systemMessage.data.query.looker.explore') AS looker_explore,

  -- --- ORIGINAL FIELDS ---
  msg AS raw_user_text,
  JSON_VALUE(msg, '$.message.timestamp') AS message_timestamp

FROM 
  `[PROJECT_ID].[DATASET_ID].[TABLE_NAME]`,
  UNNEST(JSON_EXTRACT_ARRAY(messages)) AS msg
WHERE 
  TIMESTAMP_TRUNC(created_at, DAY) <= TIMESTAMP("2026-02-09") 
  AND id = '[CONVERSATION_ID]'
ORDER BY 
  message_order ASC

-- Ex BigQuery SQL Script to count conversations overall
-- Replace [PROJECT_ID], [DATASET_ID], and [TABLE_NAME] with your actual values. 

SELECT 
  COUNT(*) AS total_conversations
FROM 
  `[PROJECT_ID].[DATASET_ID].[TABLE_NAME]`
WHERE 
  TIMESTAMP_TRUNC(created_at, DAY) <= CURRENT_TIMESTAMP()

-- Ex BigQuery SQL Script to count conversations overall by agent
-- Replace [PROJECT_ID], [DATASET_ID], and [TABLE_NAME] with your actual values. 

SELECT 
  agent_id, 
  COUNT(*) AS total_conversations
FROM 
  `[PROJECT_ID].[DATASET_ID].[TABLE_NAME]`
WHERE 
  TIMESTAMP_TRUNC(created_at, DAY) <= CURRENT_TIMESTAMP()
GROUP BY 
  agent_id

-- Ex BigQuery SQL Script to count conversations overall by agent and date
-- Replace [PROJECT_ID], [DATASET_ID], and [TABLE_NAME] with your actual values. 

SELECT 
  agent_id, 
  TIMESTAMP_TRUNC(created_at, DAY) AS conversation_date,
  COUNT(*) AS total_conversations
FROM 
  `[PROJECT_ID].[DATASET_ID].[TABLE_NAME]`
WHERE 
  TIMESTAMP_TRUNC(created_at, DAY) <= CURRENT_TIMESTAMP()
GROUP BY 
  agent_id, conversation_date

-- Ex BigQuery SQL Script looking at sessionization & timing across messages within a conversation
-- Replace [PROJECT_ID], [DATASET_ID], and [TABLE_NAME] with your actual values. 
-- Replace [CONVERSATION_ID] with the actual conversation ID.

WITH MessageBase AS (
  -- Flatten the messages and extract core metadata
  SELECT
    id AS conversation_id,
    CAST(JSON_VALUE(msg, '$.order') AS INT64) AS msg_order,
    JSON_VALUE(msg, '$.type') AS msg_type,
    JSON_VALUE(msg, '$.message.debugInfo.response.data.systemMessage.text.textType') AS text_type,
    CAST(JSON_VALUE(msg, '$.message.timestamp') AS TIMESTAMP) AS msg_timestamp
  FROM 
    `[PROJECT_ID].[DATASET_ID].[TABLE_NAME]`,
    UNNEST(JSON_EXTRACT_ARRAY(messages)) AS msg
  WHERE id = '[CONVERSATION_ID]' -- Filter for specific ID or remove for global
),
TurnGrouping AS (
  -- Create a 'turn_id' by counting how many user messages have appeared up to this point
  SELECT 
    *,
    COUNTIF(msg_type = 'user') OVER (PARTITION BY conversation_id ORDER BY msg_order) AS turn_id
  FROM MessageBase
),
TurnStats AS (
  -- For each turn, find the first and last system message timings
  SELECT
    conversation_id,
    turn_id,
    MIN(CASE WHEN msg_type = 'user' THEN msg_timestamp END) AS user_sent_at,
    MIN(CASE WHEN msg_type = 'system' THEN msg_timestamp END) AS system_start_at,
    MAX(CASE WHEN msg_type = 'system' AND text_type = 'FINAL_RESPONSE' THEN msg_timestamp END) AS final_response_at
  FROM TurnGrouping
  GROUP BY 1, 2
)
SELECT
  conversation_id,
  turn_id,
  user_sent_at,
  -- Time from User clicking 'Enter' to AI starting to think
  TIMESTAMP_DIFF(system_start_at, user_sent_at, SECOND) AS seconds_to_first_thought,
  -- Time from first System action to Final Response (Processing Latency)
  TIMESTAMP_DIFF(final_response_at, system_start_at, SECOND) AS processing_latency_seconds,
  -- Total wait time for the user
  TIMESTAMP_DIFF(final_response_at, user_sent_at, SECOND) AS total_user_wait_seconds
FROM TurnStats
WHERE system_start_at IS NOT NULL
ORDER BY conversation_id, turn_id;
