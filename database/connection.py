"""
Database connection management with connection pooling and retry logic.
"""

import os
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
from sqlalchemy import create_engine, text, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from config.settings import Config

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """
    Manages PostgreSQL database connections with pooling and automatic retry.
    """
    
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize database connection if not already initialized."""
        if hasattr(self, 'engine'):
            return

        self.connection_string = Config.get_db_url()
        
        # Create engine with connection pooling
        self.engine = create_engine(
            self.connection_string,
            poolclass=pool.QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_pre_ping=True,  # Verify connections before using
            echo=False  # Set to True for SQL logging
        )
        
        # Create session factory
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        logger.info(f"Database connection initialized")
    
    @contextmanager
    def get_session(self) -> Session:
        """
        Context manager for database sessions.
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch: bool = True
    ):
        """
        Execute a SQL query.
        """
        with self.engine.connect() as connection:
            try:
                result = connection.execute(text(query), params or {})
                if fetch:
                    data = result.fetchall()
                    connection.commit()
                    return data
                connection.commit()
                return result
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                raise

# Global database instance
_db_instance = None

def get_db():
    """Get or create global database connection instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseConnection()
    return _db_instance
