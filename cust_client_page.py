import snowflake.connector
from typing import Iterator, Dict, Any, Optional
import logging
from datetime import datetime, timedelta

class SnowflakeCortexPaginator:
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(**self.connection_params)
            self.logger.info("Successfully connected to Snowflake")
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def paginate_cortex_results(
        self, 
        query: str, 
        page_size: int = 1000,
        max_pages: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Paginate through large result sets using Cortex AI functions
        
        Args:
            query: Base SQL query (should include pagination placeholders)
            page_size: Number of records per page
            max_pages: Maximum number of pages to process (None for all)
            
        Yields:
            Dictionary containing page data and metadata
        """
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        offset = 0
        page_num = 0
        
        try:
            while True:
                # Check max_pages limit
                if max_pages and page_num >= max_pages:
                    break
                
                # Execute paginated query
                paginated_query = query.format(
                    offset=offset,
                    limit=page_size
                )
                
                start_time = datetime.now()
                cursor.execute(paginated_query)
                results = cursor.fetchall()
                execution_time = datetime.now() - start_time
                
                # If no results, we've reached the end
                if not results:
                    break
                
                # Convert results to dictionaries
                columns = [desc[0] for desc in cursor.description]
                page_data = [dict(zip(columns, row)) for row in results]
                
                yield {
                    'page_number': page_num + 1,
                    'total_records': len(page_data),
                    'data': page_data,
                    'execution_time': execution_time.total_seconds(),
                    'offset': offset
                }
                
                # Prepare for next page
                offset += page_size
                page_num += 1
                
                self.logger.info(f"Processed page {page_num}, {len(page_data)} records")
                
        except Exception as e:
            self.logger.error(f"Error during pagination: {e}")
            raise
        finally:
            cursor.close()
  
  def process_with_cortex_ai(
          self,
          base_query: str,
          ai_functions: Dict[str, str],
          page_size: int = 1000
      ) -> Iterator[Dict[str, Any]]:
          """
          Process data with multiple Cortex AI functions using pagination
          
          Args:
              base_query: Base query for data selection
              ai_functions: Dictionary of AI function names and their SQL
              page_size: Records per page
          """
          
          # Build enhanced query with AI functions
          ai_select_clauses = []
          for func_name, func_sql in ai_functions.items():
              ai_select_clauses.append(f"{func_sql} AS {func_name}")
          
          enhanced_query = f"""
          WITH base_data AS ({base_query})
          SELECT *,
              {', '.join(ai_select_clauses)}
          FROM base_data
          ORDER BY customer_id
          LIMIT {page_size} OFFSET {{offset}}
          """
          
          return self.paginate_cortex_results(enhanced_query, page_size)
