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

    def get_optimal_page_size(query_complexity: str, data_size_mb: float) -> int:
        """Determine optimal page size based on query and data characteristics"""
        
        base_size = 1000
        
        if query_complexity == 'high':  # Multiple AI functions
            base_size = 500
        elif query_complexity == 'low':  # Simple queries
            base_size = 2000
        
        # Adjust for data size
        if data_size_mb > 100:
            base_size = int(base_size * 0.7)
        
    return base_size


# For independent pages, implement parallel processing:

import concurrent.futures
from typing import List

def process_pages_parallel(
    paginator: SnowflakeCortexPaginator,
    query: str,
    page_ranges: List[tuple],
    max_workers: int = 4
) -> List[Dict[str, Any]]:
    """Process multiple page ranges in parallel"""
    
    def process_single_range(page_range):
        start_offset, end_offset = page_range
        results = []
        
        for page in paginator.paginate_cortex_results(
            query, 
            page_size=1000,
            offset_start=start_offset,
            offset_end=end_offset
        ):
            results.extend(page['data'])
        
        return results
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_range = {
            executor.submit(process_single_range, page_range): page_range
            for page_range in page_ranges
        }
        
        all_results = []
        for future in concurrent.futures.as_completed(future_to_range):
            page_range = future_to_range[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                print(f"Error processing range {page_range}: {e}")
        
        return all_results


# Configuration
connection_params = {
    'user': 'your_username',
    'password': 'your_password',
    'account': 'your_account',
    'warehouse': 'your_warehouse',
    'database': 'your_database',
    'schema': 'your_schema'
}

# Initialize paginator
paginator = SnowflakeCortexPaginator(connection_params)
# Define base query
base_query = """
SELECT 
    customer_id,
    transaction_data,
    customer_profile,
    feedback_text
FROM customer_analytics
WHERE created_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP())
"""
# Define AI functions to apply
ai_functions = {
    'sentiment_analysis': """
        SNOWFLAKE.CORTEX.SENTIMENT(feedback_text)
    """,
    'content_summary': """
        SNOWFLAKE.CORTEX.SUMMARIZE(
            GET(transaction_data, 'purchase_history')::STRING
        )
    """,
    'classification': """
        SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
            feedback_text,
            ['positive', 'negative', 'neutral']
        )
    """
}
# Process data with pagination
total_processed = 0
for page in paginator.process_with_cortex_ai(
    base_query, 
    ai_functions, 
    page_size=500
):
    print(f"Processing page {page['page_number']}")
    print(f"Records in page: {page['total_records']}")
    print(f"Execution time: {page['execution_time']:.2f} seconds")
    
    # Process each record
    for record in page['data']:
        # Your processing logic here
        process_ai_insights(record)
        total_processed += 1
    
    print(f"Total processed so far: {total_processed}")
