# Implement robust error handling for production environments:

import time
from functools import wraps

def retry_on_failure(max_retries: int = 3, backoff_factor: float = 2.0):
    """Decorator for retrying failed operations"""
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = backoff_factor ** attempt
                        time.sleep(wait_time)
                        continue
                    break
            
            raise last_exception
        return wrapper
    return decorator

class ResilientPaginator(SnowflakeCortexPaginator):
    @retry_on_failure(max_retries=3)
    def execute_query_with_retry(self, query: str):
        """Execute query with automatic retry logic"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            # Log error and re-raise for retry logic
            self.logger.warning(f"Query execution failed: {e}")
            raise
        finally:
            cursor.close()
