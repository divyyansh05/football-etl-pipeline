"""
Batch loading utilities for optimized database operations.
"""

import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from database.connection import get_db

logger = logging.getLogger(__name__)


class BatchLoader:
    """
    Optimized batch loading for large datasets.
    """
    
    def __init__(self, db=None, chunk_size: int = 500):
        """
        Initialize batch loader.
        
        Args:
            db: Database connection
            chunk_size: Number of records per batch
        """
        self.db = db or get_db()
        self.chunk_size = chunk_size
    
    def batch_upsert(
        self,
        table: str,
        records: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None
    ) -> int:
        """
        Batch upsert records with ON CONFLICT handling.
        
        Args:
            table: Table name
            records: List of record dictionaries
            conflict_columns: Columns to check for conflicts
            update_columns: Columns to update on conflict (None = all except conflict columns)
            
        Returns:
            Number of records processed
        """
        if not records:
            return 0
        
        total_processed = 0
        
        # Process in chunks
        for i in range(0, len(records), self.chunk_size):
            chunk = records[i:i + self.chunk_size]
            
            try:
                # Build column list from first record
                columns = list(chunk[0].keys())
                
                # Determine update columns
                if update_columns is None:
                    update_columns = [col for col in columns if col not in conflict_columns]
                
                # Build INSERT statement
                placeholders = ', '.join([f':{col}' for col in columns])
                columns_str = ', '.join(columns)
                
                # Build ON CONFLICT clause
                conflict_str = ', '.join(conflict_columns)
                update_str = ', '.join([f'{col} = EXCLUDED.{col}' for col in update_columns])
                
                query = f"""
                    INSERT INTO {table} ({columns_str})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_str})
                    DO UPDATE SET {update_str}
                """
                
                # Execute batch
                with self.db.engine.begin() as conn:
                    conn.execute(text(query), chunk)
                
                total_processed += len(chunk)
                logger.debug(f"Processed {len(chunk)} records for {table}")
                
            except Exception as e:
                logger.error(f"Error in batch upsert for {table}: {e}")
                raise
        
        logger.info(f"Batch upsert complete: {total_processed} records to {table}")
        return total_processed
    
    def bulk_insert(
        self,
        table: str,
        records: List[Dict[str, Any]]
    ) -> int:
        """
        Fast bulk insert for new data (no conflict handling).
        
        Args:
            table: Table name
            records: List of record dictionaries
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
        
        total_inserted = 0
        
        # Process in chunks
        for i in range(0, len(records), self.chunk_size):
            chunk = records[i:i + self.chunk_size]
            
            try:
                # Build column list from first record
                columns = list(chunk[0].keys())
                columns_str = ', '.join(columns)
                placeholders = ', '.join([f':{col}' for col in columns])
                
                query = f"""
                    INSERT INTO {table} ({columns_str})
                    VALUES ({placeholders})
                """
                
                # Execute batch
                with self.db.engine.begin() as conn:
                    conn.execute(text(query), chunk)
                
                total_inserted += len(chunk)
                logger.debug(f"Inserted {len(chunk)} records into {table}")
                
            except Exception as e:
                logger.error(f"Error in bulk insert for {table}: {e}")
                raise
        
        logger.info(f"Bulk insert complete: {total_inserted} records to {table}")
        return total_inserted
    
    def execute_batch(
        self,
        query: str,
        params_list: List[Dict[str, Any]]
    ) -> int:
        """
        Execute a query with multiple parameter sets.
        
        Args:
            query: SQL query with named parameters
            params_list: List of parameter dictionaries
            
        Returns:
            Number of executions
        """
        if not params_list:
            return 0
        
        total_executed = 0
        
        # Process in chunks
        for i in range(0, len(params_list), self.chunk_size):
            chunk = params_list[i:i + self.chunk_size]
            
            try:
                with self.db.engine.begin() as conn:
                    for params in chunk:
                        conn.execute(text(query), params)
                
                total_executed += len(chunk)
                logger.debug(f"Executed {len(chunk)} queries")
                
            except Exception as e:
                logger.error(f"Error in batch execution: {e}")
                raise
        
        logger.info(f"Batch execution complete: {total_executed} queries")
        return total_executed
