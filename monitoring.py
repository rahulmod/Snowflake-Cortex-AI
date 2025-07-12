# Implement comprehensive monitoring for production deployments:

import time
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class PaginationMetrics:
    total_pages: int = 0
    total_records: int = 0
    total_execution_time: float = 0.0
    avg_page_time: float = 0.0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
class MonitoredPaginator(SnowflakeCortexPaginator):
    def __init__(self, connection_params: Dict[str, str]):
        super().__init__(connection_params)
        self.metrics = PaginationMetrics()
    
    def paginate_with_monitoring(self, query: str, page_size: int = 1000):
        """Paginate with comprehensive monitoring"""
        start_time = time.time()
        
        try:
            for page in self.paginate_cortex_results(query, page_size):
                self.metrics.total_pages += 1
                self.metrics.total_records += page['total_records']
                
                yield page
                
        except Exception as e:
            self.metrics.errors.append(str(e))
            raise
        finally:
            self.metrics.total_execution_time = time.time() - start_time
            if self.metrics.total_pages > 0:
                self.metrics.avg_page_time = (
                    self.metrics.total_execution_time / self.metrics.total_pages
                )
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        return {
            'total_pages_processed': self.metrics.total_pages,
            'total_records_processed': self.metrics.total_records,
            'total_execution_time_seconds': self.metrics.total_execution_time,
            'average_page_execution_time': self.metrics.avg_page_time,
            'records_per_second': (
                self.metrics.total_records / self.metrics.total_execution_time
                if self.metrics.total_execution_time > 0 else 0
            ),
            'error_count': len(self.metrics.errors),
            'errors': self.metrics.errors
        }
