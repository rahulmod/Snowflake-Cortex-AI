-- ====================================================================
-- Snowflake Cortex REST API Implementation
-- Building scalable REST API endpoints using Snowflake native features
-- ====================================================================

-- Create database and schema structure
CREATE DATABASE IF NOT EXISTS CORTEX_API_DB;
USE DATABASE CORTEX_API_DB;

CREATE SCHEMA IF NOT EXISTS API_SCHEMA;
USE SCHEMA API_SCHEMA;

-- ====================================================================
-- 1. CORE TABLES FOR API MANAGEMENT
-- ====================================================================

-- Table to store API endpoints configuration
CREATE OR REPLACE TABLE api_endpoints (
    endpoint_id VARCHAR(36) PRIMARY KEY,
    endpoint_name VARCHAR(255) NOT NULL,
    endpoint_path VARCHAR(500) NOT NULL,
    http_method VARCHAR(10) NOT NULL,
    query_template TEXT NOT NULL,
    description TEXT,
    parameters VARIANT,
    authentication_required BOOLEAN DEFAULT TRUE,
    rate_limit_per_minute INTEGER DEFAULT 100,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table to store query results with caching
CREATE OR REPLACE TABLE api_results_cache (
    cache_key VARCHAR(64) PRIMARY KEY,
    endpoint_id VARCHAR(36) NOT NULL,
    query_hash VARCHAR(64) NOT NULL,
    result_data VARIANT NOT NULL,
    metadata VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    expires_at TIMESTAMP_NTZ,
    access_count INTEGER DEFAULT 0,
    last_accessed TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table to store API access logs
CREATE OR REPLACE TABLE api_access_logs (
    log_id VARCHAR(36) PRIMARY KEY,
    endpoint_id VARCHAR(36),
    request_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    client_ip VARCHAR(45),
    user_agent TEXT,
    request_method VARCHAR(10),
    request_path VARCHAR(500),
    request_parameters VARIANT,
    response_status INTEGER,
    response_time_ms INTEGER,
    error_message TEXT,
    cache_hit BOOLEAN DEFAULT FALSE
);

-- Table for API authentication tokens
CREATE OR REPLACE TABLE api_tokens (
    token_id VARCHAR(36) PRIMARY KEY,
    token_hash VARCHAR(64) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    token_name VARCHAR(255),
    permissions VARIANT,
    expires_at TIMESTAMP_NTZ,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_used TIMESTAMP_NTZ
);

-- Table for rate limiting
CREATE OR REPLACE TABLE api_rate_limits (
    limit_id VARCHAR(36) PRIMARY KEY,
    client_identifier VARCHAR(255) NOT NULL,
    endpoint_id VARCHAR(36),
    request_count INTEGER DEFAULT 0,
    window_start TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    window_end TIMESTAMP_NTZ,
    is_blocked BOOLEAN DEFAULT FALSE
);

-- ====================================================================
-- 2. STORED PROCEDURES FOR API FUNCTIONALITY
-- ====================================================================

-- Procedure to register a new API endpoint
CREATE OR REPLACE PROCEDURE register_api_endpoint(
    endpoint_name VARCHAR(255),
    endpoint_path VARCHAR(500),
    http_method VARCHAR(10),
    query_template TEXT,
    description TEXT,
    parameters VARIANT,
    authentication_required BOOLEAN,
    rate_limit_per_minute INTEGER
)
RETURNS VARCHAR(36)
LANGUAGE SQL
AS
$$
DECLARE
    endpoint_id VARCHAR(36);
BEGIN
    endpoint_id := UUID_STRING();
    
    INSERT INTO api_endpoints (
        endpoint_id, endpoint_name, endpoint_path, http_method,
        query_template, description, parameters, authentication_required,
        rate_limit_per_minute
    )
    VALUES (
        endpoint_id, endpoint_name, endpoint_path, http_method,
        query_template, description, parameters, authentication_required,
        rate_limit_per_minute
    );
    
    RETURN endpoint_id;
END;
$$;

-- Procedure to execute API endpoint with caching
CREATE OR REPLACE PROCEDURE execute_api_endpoint(
    endpoint_path VARCHAR(500),
    http_method VARCHAR(10),
    request_parameters VARIANT,
    client_ip VARCHAR(45),
    user_agent TEXT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    endpoint_record VARIANT;
    cache_key VARCHAR(64);
    cached_result VARIANT;
    query_result VARIANT;
    final_query TEXT;
    execution_start TIMESTAMP_NTZ;
    execution_end TIMESTAMP_NTZ;
    response_time INTEGER;
    result_metadata VARIANT;
    log_id VARCHAR(36);
BEGIN
    execution_start := CURRENT_TIMESTAMP();
    log_id := UUID_STRING();
    
    -- Find the endpoint
    SELECT OBJECT_CONSTRUCT(
        'endpoint_id', endpoint_id,
        'query_template', query_template,
        'parameters', parameters,
        'authentication_required', authentication_required,
        'rate_limit_per_minute', rate_limit_per_minute
    ) INTO endpoint_record
    FROM api_endpoints
    WHERE endpoint_path = :endpoint_path 
    AND http_method = :http_method
    AND is_active = TRUE;
    
    -- Check if endpoint exists
    IF (endpoint_record IS NULL) THEN
        -- Log the failed request
        INSERT INTO api_access_logs (
            log_id, request_timestamp, client_ip, user_agent,
            request_method, request_path, request_parameters,
            response_status, error_message
        )
        VALUES (
            log_id, execution_start, client_ip, user_agent,
            http_method, endpoint_path, request_parameters,
            404, 'Endpoint not found'
        );
        
        RETURN OBJECT_CONSTRUCT('error', 'Endpoint not found', 'status', 404);
    END IF;
    
    -- Generate cache key
    cache_key := SHA2(CONCAT(endpoint_path, ':', TO_JSON(request_parameters)), 256);
    
    -- Check cache first
    SELECT result_data INTO cached_result
    FROM api_results_cache
    WHERE cache_key = :cache_key
    AND expires_at > CURRENT_TIMESTAMP();
    
    IF (cached_result IS NOT NULL) THEN
        -- Update cache access statistics
        UPDATE api_results_cache
        SET access_count = access_count + 1,
            last_accessed = CURRENT_TIMESTAMP()
        WHERE cache_key = :cache_key;
        
        execution_end := CURRENT_TIMESTAMP();
        response_time := DATEDIFF('millisecond', execution_start, execution_end);
        
        -- Log the cached response
        INSERT INTO api_access_logs (
            log_id, endpoint_id, request_timestamp, client_ip, user_agent,
            request_method, request_path, request_parameters,
            response_status, response_time_ms, cache_hit
        )
        VALUES (
            log_id, endpoint_record:endpoint_id::VARCHAR, execution_start, client_ip, user_agent,
            http_method, endpoint_path, request_parameters,
            200, response_time, TRUE
        );
        
        RETURN cached_result;
    END IF;
    
    -- Build and execute query
    final_query := endpoint_record:query_template::TEXT;
    
    -- Replace parameters in query template
    IF (request_parameters IS NOT NULL) THEN
        -- This is a simplified parameter replacement
        -- In production, you'd want more sophisticated parameter handling
        final_query := REPLACE(final_query, '{{limit}}', 
            COALESCE(request_parameters:limit::TEXT, '100'));
        final_query := REPLACE(final_query, '{{offset}}', 
            COALESCE(request_parameters:offset::TEXT, '0'));
    END IF;
    
    -- Execute the query using Cortex
    BEGIN
        EXECUTE IMMEDIATE final_query;
        LET result_cursor CURSOR FOR EXECUTE IMMEDIATE final_query;
        
        -- Convert cursor to JSON array
        LET result_array ARRAY := [];
        FOR record IN result_cursor DO
            result_array := ARRAY_APPEND(result_array, record);
        END FOR;
        
        query_result := OBJECT_CONSTRUCT('data', result_array);
        
    EXCEPTION
        WHEN OTHER THEN
            execution_end := CURRENT_TIMESTAMP();
            response_time := DATEDIFF('millisecond', execution_start, execution_end);
            
            -- Log the error
            INSERT INTO api_access_logs (
                log_id, endpoint_id, request_timestamp, client_ip, user_agent,
                request_method, request_path, request_parameters,
                response_status, response_time_ms, error_message
            )
            VALUES (
                log_id, endpoint_record:endpoint_id::VARCHAR, execution_start, client_ip, user_agent,
                http_method, endpoint_path, request_parameters,
                500, response_time, SQLERRM
            );
            
            RETURN OBJECT_CONSTRUCT('error', 'Query execution failed', 'status', 500, 'details', SQLERRM);
    END;
    
    execution_end := CURRENT_TIMESTAMP();
    response_time := DATEDIFF('millisecond', execution_start, execution_end);
    
    -- Cache the result
    INSERT INTO api_results_cache (
        cache_key, endpoint_id, query_hash, result_data,
        expires_at, metadata
    )
    VALUES (
        cache_key, endpoint_record:endpoint_id::VARCHAR, 
        SHA2(final_query, 256), query_result,
        DATEADD('hour', 1, CURRENT_TIMESTAMP()),
        OBJECT_CONSTRUCT('execution_time_ms', response_time, 'cached_at', CURRENT_TIMESTAMP())
    );
    
    -- Log successful request
    INSERT INTO api_access_logs (
        log_id, endpoint_id, request_timestamp, client_ip, user_agent,
        request_method, request_path, request_parameters,
        response_status, response_time_ms, cache_hit
    )
    VALUES (
        log_id, endpoint_record:endpoint_id::VARCHAR, execution_start, client_ip, user_agent,
        http_method, endpoint_path, request_parameters,
        200, response_time, FALSE
    );
    
    RETURN query_result;
END;
$$;

-- Procedure for API authentication
CREATE OR REPLACE PROCEDURE authenticate_api_request(
    auth_token VARCHAR(255)
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    token_info VARIANT;
BEGIN
    SELECT OBJECT_CONSTRUCT(
        'token_id', token_id,
        'user_id', user_id,
        'permissions', permissions,
        'expires_at', expires_at
    ) INTO token_info
    FROM api_tokens
    WHERE token_hash = SHA2(auth_token, 256)
    AND is_active = TRUE
    AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP());
    
    IF (token_info IS NOT NULL) THEN
        -- Update last used timestamp
        UPDATE api_tokens
        SET last_used = CURRENT_TIMESTAMP()
        WHERE token_hash = SHA2(auth_token, 256);
        
        RETURN OBJECT_CONSTRUCT('authenticated', TRUE, 'user_info', token_info);
    ELSE
        RETURN OBJECT_CONSTRUCT('authenticated', FALSE, 'error', 'Invalid or expired token');
    END IF;
END;
$$;

-- Procedure for rate limiting
CREATE OR REPLACE PROCEDURE check_rate_limit(
    client_identifier VARCHAR(255),
    endpoint_id VARCHAR(36)
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    current_window_start TIMESTAMP_NTZ;
    current_window_end TIMESTAMP_NTZ;
    request_count INTEGER;
    rate_limit INTEGER;
    is_allowed BOOLEAN;
BEGIN
    -- Get rate limit for endpoint
    SELECT rate_limit_per_minute INTO rate_limit
    FROM api_endpoints
    WHERE endpoint_id = :endpoint_id;
    
    current_window_start := DATE_TRUNC('minute', CURRENT_TIMESTAMP());
    current_window_end := DATEADD('minute', 1, current_window_start);
    
    -- Check current request count
    SELECT COALESCE(request_count, 0) INTO request_count
    FROM api_rate_limits
    WHERE client_identifier = :client_identifier
    AND endpoint_id = :endpoint_id
    AND window_start = current_window_start;
    
    IF (request_count < rate_limit) THEN
        -- Allow request and update counter
        MERGE INTO api_rate_limits t
        USING (
            SELECT :client_identifier as client_identifier,
                   :endpoint_id as endpoint_id,
                   current_window_start as window_start,
                   current_window_end as window_end
        ) s
        ON t.client_identifier = s.client_identifier
        AND t.endpoint_id = s.endpoint_id
        AND t.window_start = s.window_start
        WHEN MATCHED THEN
            UPDATE SET request_count = request_count + 1
        WHEN NOT MATCHED THEN
            INSERT (limit_id, client_identifier, endpoint_id, request_count, window_start, window_end)
            VALUES (UUID_STRING(), s.client_identifier, s.endpoint_id, 1, s.window_start, s.window_end);
        
        is_allowed := TRUE;
    ELSE
        is_allowed := FALSE;
    END IF;
    
    RETURN OBJECT_CONSTRUCT(
        'allowed', is_allowed,
        'current_count', request_count,
        'limit', rate_limit,
        'window_start', current_window_start,
        'window_end', current_window_end
    );
END;
$$;

-- ====================================================================
-- 3. API GATEWAY FUNCTIONS USING CORTEX
-- ====================================================================

-- Main API Gateway function
CREATE OR REPLACE FUNCTION api_gateway(
    request_path VARCHAR(500),
    request_method VARCHAR(10),
    request_parameters VARIANT,
    auth_header VARCHAR(255),
    client_ip VARCHAR(45),
    user_agent TEXT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    WITH endpoint_check AS (
        SELECT endpoint_id, authentication_required, rate_limit_per_minute
        FROM api_endpoints
        WHERE endpoint_path = request_path
        AND http_method = request_method
        AND is_active = TRUE
    ),
    auth_check AS (
        SELECT 
            CASE 
                WHEN e.authentication_required = TRUE THEN
                    CASE 
                        WHEN auth_header IS NULL THEN 
                            OBJECT_CONSTRUCT('authenticated', FALSE, 'error', 'Authentication required')
                        ELSE
                            (SELECT authenticate_api_request(auth_header))
                    END
                ELSE
                    OBJECT_CONSTRUCT('authenticated', TRUE)
            END as auth_result
        FROM endpoint_check e
    ),
    rate_limit_check AS (
        SELECT 
            CASE 
                WHEN a.auth_result:authenticated::BOOLEAN = TRUE THEN
                    (SELECT check_rate_limit(client_ip, e.endpoint_id))
                ELSE
                    OBJECT_CONSTRUCT('allowed', FALSE, 'error', 'Authentication failed')
            END as rate_result
        FROM endpoint_check e, auth_check a
    )
    SELECT 
        CASE 
            WHEN NOT EXISTS (SELECT 1 FROM endpoint_check) THEN
                OBJECT_CONSTRUCT('error', 'Endpoint not found', 'status', 404)
            WHEN a.auth_result:authenticated::BOOLEAN = FALSE THEN
                OBJECT_CONSTRUCT('error', a.auth_result:error::TEXT, 'status', 401)
            WHEN r.rate_result:allowed::BOOLEAN = FALSE THEN
                OBJECT_CONSTRUCT('error', 'Rate limit exceeded', 'status', 429)
            ELSE
                (SELECT execute_api_endpoint(request_path, request_method, request_parameters, client_ip, user_agent))
        END as response
    FROM auth_check a, rate_limit_check r
$$;

-- ====================================================================
-- 4. CORTEX AI INTEGRATION FUNCTIONS
-- ====================================================================

-- Function to analyze API usage patterns using Cortex
CREATE OR REPLACE FUNCTION analyze_api_usage()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    WITH usage_stats AS (
        SELECT 
            endpoint_id,
            COUNT(*) as total_requests,
            AVG(response_time_ms) as avg_response_time,
            COUNT(CASE WHEN cache_hit = TRUE THEN 1 END) as cache_hits,
            COUNT(CASE WHEN response_status >= 400 THEN 1 END) as error_count,
            DATE_TRUNC('hour', request_timestamp) as hour_bucket
        FROM api_access_logs
        WHERE request_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        GROUP BY endpoint_id, hour_bucket
    ),
    ai_analysis AS (
        SELECT 
            SNOWFLAKE.CORTEX.COMPLETE(
                'mixtral-8x7b',
                'Analyze the following API usage data and provide insights on performance, usage patterns, and recommendations: ' ||
                TO_JSON(OBJECT_CONSTRUCT(
                    'total_requests', SUM(total_requests),
                    'avg_response_time', AVG(avg_response_time),
                    'cache_hit_rate', SUM(cache_hits) / SUM(total_requests),
                    'error_rate', SUM(error_count) / SUM(total_requests),
                    'hourly_patterns', ARRAY_AGG(OBJECT_CONSTRUCT(
                        'hour', hour_bucket,
                        'requests', total_requests,
                        'response_time', avg_response_time
                    ))
                ))
            ) as ai_insights
        FROM usage_stats
    )
    SELECT 
        OBJECT_CONSTRUCT(
            'analysis_timestamp', CURRENT_TIMESTAMP(),
            'insights', ai_insights,
            'raw_data', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'endpoint_id', endpoint_id,
                'total_requests', total_requests,
                'avg_response_time', avg_response_time,
                'cache_hit_rate', cache_hits / total_requests,
                'error_rate', error_count / total_requests
            )) FROM usage_stats)
        ) as analysis_result
    FROM ai_analysis
$$;

-- Function to generate API documentation using Cortex
CREATE OR REPLACE FUNCTION generate_api_documentation(endpoint_id VARCHAR(36))
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    WITH endpoint_info AS (
        SELECT 
            endpoint_name,
            endpoint_path,
            http_method,
            query_template,
            description,
            parameters
        FROM api_endpoints
        WHERE endpoint_id = :endpoint_id
    ),
    documentation AS (
        SELECT 
            SNOWFLAKE.CORTEX.COMPLETE(
                'mixtral-8x7b',
                'Generate comprehensive API documentation for the following endpoint: ' ||
                TO_JSON(OBJECT_CONSTRUCT(
                    'name', endpoint_name,
                    'path', endpoint_path,
                    'method', http_method,
                    'query', query_template,
                    'description', description,
                    'parameters', parameters
                )) ||
                ' Include: description, parameters, request/response examples, error codes, and usage notes.'
            ) as generated_docs
        FROM endpoint_info
    )
    SELECT 
        OBJECT_CONSTRUCT(
            'endpoint_id', :endpoint_id,
            'generated_at', CURRENT_TIMESTAMP(),
            'documentation', generated_docs
        ) as documentation_result
    FROM documentation
$$;

-- ====================================================================
-- 5. EXAMPLE API ENDPOINT REGISTRATIONS
-- ====================================================================

-- Register sample endpoints
CALL register_api_endpoint(
    'Get Users',
    '/api/v1/users',
    'GET',
    'SELECT user_id, username, email, created_at FROM users ORDER BY created_at DESC LIMIT {{limit}} OFFSET {{offset}}',
    'Retrieve paginated list of users',
    OBJECT_CONSTRUCT('limit', OBJECT_CONSTRUCT('type', 'integer', 'default', 100), 'offset', OBJECT_CONSTRUCT('type', 'integer', 'default', 0)),
    TRUE,
    100
);

CALL register_api_endpoint(
    'Get User by ID',
    '/api/v1/users/{id}',
    'GET',
    'SELECT user_id, username, email, profile_data, created_at FROM users WHERE user_id = {{id}}',
    'Retrieve specific user by ID',
    OBJECT_CONSTRUCT('id', OBJECT_CONSTRUCT('type', 'string', 'required', TRUE)),
    TRUE,
    200
);

CALL register_api_endpoint(
    'Analytics Dashboard',
    '/api/v1/analytics/dashboard',
    'GET',
    'SELECT date_trunc(''day'', event_timestamp) as date, event_type, count(*) as count FROM analytics_events WHERE event_timestamp >= dateadd(''day'', -30, current_timestamp()) GROUP BY date, event_type ORDER BY date DESC',
    'Get analytics dashboard data for the last 30 days',
    OBJECT_CONSTRUCT(),
    TRUE,
    50
);

-- ====================================================================
-- 6. MONITORING AND MAINTENANCE VIEWS
-- ====================================================================

-- View for API endpoint performance monitoring
CREATE OR REPLACE VIEW api_performance_dashboard AS
SELECT 
    e.endpoint_name,
    e.endpoint_path,
    e.http_method,
    COUNT(l.log_id) as total_requests,
    AVG(l.response_time_ms) as avg_response_time,
    MIN(l.response_time_ms) as min_response_time,
    MAX(l.response_time_ms) as max_response_time,
    COUNT(CASE WHEN l.response_status >= 400 THEN 1 END) as error_count,
    COUNT(CASE WHEN l.cache_hit = TRUE THEN 1 END) as cache_hits,
    COUNT(CASE WHEN l.cache_hit = TRUE THEN 1 END) * 100.0 / COUNT(l.log_id) as cache_hit_percentage,
    DATE_TRUNC('hour', l.request_timestamp) as hour_bucket
FROM api_endpoints e
LEFT JOIN api_access_logs l ON e.endpoint_id = l.endpoint_id
WHERE l.request_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY e.endpoint_name, e.endpoint_path, e.http_method, hour_bucket
ORDER BY hour_bucket DESC, total_requests DESC;

-- View for real-time API health monitoring
CREATE OR REPLACE VIEW api_health_status AS
SELECT 
    'API_HEALTH' as metric_type,
    COUNT(CASE WHEN l.response_status = 200 THEN 1 END) as healthy_requests,
    COUNT(CASE WHEN l.response_status >= 400 THEN 1 END) as error_requests,
    COUNT(l.log_id) as total_requests,
    AVG(l.response_time_ms) as avg_response_time,
    COUNT(DISTINCT l.client_ip) as unique_clients,
    CURRENT_TIMESTAMP() as calculated_at
FROM api_access_logs l
WHERE l.request_timestamp >= DATEADD('minute', -5, CURRENT_TIMESTAMP());

-- ====================================================================
-- 7. CLEANUP AND MAINTENANCE PROCEDURES
-- ====================================================================

-- Procedure to clean up old logs and cache entries
CREATE OR REPLACE PROCEDURE cleanup_api_data()
RETURNS VARCHAR(255)
LANGUAGE SQL
AS
$$
DECLARE
    logs_deleted INTEGER;
    cache_deleted INTEGER;
BEGIN
    -- Delete old access logs (older than 90 days)
    DELETE FROM api_access_logs 
    WHERE request_timestamp < DATEADD('day', -90, CURRENT_TIMESTAMP());
    logs_deleted := SQLROWCOUNT;
    
    -- Delete expired cache entries
    DELETE FROM api_results_cache 
    WHERE expires_at < CURRENT_TIMESTAMP();
    cache_deleted := SQLROWCOUNT;
    
    -- Delete old rate limit records
    DELETE FROM api_rate_limits 
    WHERE window_end < DATEADD('day', -1, CURRENT_TIMESTAMP());
    
    RETURN 'Cleanup completed. Deleted ' || logs_deleted || ' log entries and ' || cache_deleted || ' cache entries.';
END;
$$;

-- Schedule the cleanup procedure to run daily
-- CREATE OR REPLACE TASK cleanup_api_data_task
-- WAREHOUSE = 'COMPUTE_WH'
-- SCHEDULE = 'USING CRON 0 2 * * * UTC'
-- AS
-- CALL cleanup_api_data();

-- ====================================================================
-- 8. USAGE EXAMPLES
-- ====================================================================

-- Example 1: Execute API endpoint
SELECT api_gateway(
    '/api/v1/users',
    'GET',
    OBJECT_CONSTRUCT('limit', 10, 'offset', 0),
    'Bearer your_jwt_token_here',
    '192.168.1.100',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
) as api_response;

-- Example 2: Get API usage analytics
SELECT analyze_api_usage() as usage_analysis;

-- Example 3: Generate documentation for an endpoint
SELECT generate_api_documentation('your_endpoint_id_here') as documentation;

-- Example 4: Monitor API performance
SELECT * FROM api_performance_dashboard 
WHERE hour_bucket >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY hour_bucket DESC;

-- Example 5: Check API health
SELECT * FROM api_health_status;
