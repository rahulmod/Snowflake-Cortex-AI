CREATE OR REPLACE HYBRID TABLE customer_analytics (
    customer_id NUMBER PRIMARY KEY,
    transaction_data VARIANT,
    customer_profile VARIANT,
    ai_insights VARIANT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (customer_id);


-- Base query with pagination and AI processing
WITH paginated_data AS (
    SELECT 
        customer_id,
        transaction_data,
        customer_profile,
        SNOWFLAKE.CORTEX.SENTIMENT(
            GET(customer_profile, 'feedback_text')::STRING
        ) AS sentiment_score,
        SNOWFLAKE.CORTEX.SUMMARIZE(
            GET(transaction_data, 'purchase_history')::STRING
        ) AS purchase_summary,
        ROW_NUMBER() OVER (ORDER BY customer_id) AS row_num
    FROM customer_analytics
    WHERE created_timestamp >= ?
    AND created_timestamp < ?
)
SELECT 
    customer_id,
    transaction_data,
    customer_profile,
    sentiment_score,
    purchase_summary
FROM paginated_data
WHERE row_num > ? -- offset
AND row_num <= ? -- offset + limit
ORDER BY customer_id;

