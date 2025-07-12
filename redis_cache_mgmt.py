import redis
import pickle
from typing import Optional

class CachedPaginator(SnowflakeCortexPaginator):
    def __init__(self, connection_params: Dict[str, str], redis_client: redis.Redis):
        super().__init__(connection_params)
        self.redis_client = redis_client
        self.cache_ttl = 3600  # 1 hour
    
    def get_cached_page(self, query_hash: str, page_num: int) -> Optional[Dict[str, Any]]:
        """Retrieve cached page if available"""
        cache_key = f"cortex_page:{query_hash}:{page_num}"
        cached_data = self.redis_client.get(cache_key)
        
        if cached_data:
            return pickle.loads(cached_data)
        return None
    
    def cache_page(self, query_hash: str, page_num: int, page_data: Dict[str, Any]):
        """Cache page data"""
        cache_key = f"cortex_page:{query_hash}:{page_num}"
        self.redis_client.setex(
            cache_key, 
            self.cache_ttl, 
            pickle.dumps(page_data)
        )


  
