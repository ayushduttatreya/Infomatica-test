"""
Database Session Factory and Connection Pool Management
Provides database connectivity with connection pooling, retry logic, and session management.
"""
import time
from contextlib import contextmanager
from typing import Optional, Generator, Dict, Any
from sqlalchemy import create_engine, event, pool, exc
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus

from src.framework.logger import LoggerFactory


logger = LoggerFactory.get_logger(__name__)


class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass


class DatabaseSessionFactory:
    """
    Factory for creating and managing database sessions with connection pooling.
    """
    
    _engines: Dict[str, Engine] = {}
    _session_factories: Dict[str, sessionmaker] = {}
    
    @classmethod
    def create_engine(cls,
                     connection_string: str,
                     pool_size: int = 10,
                     max_overflow: int = 20,
                     pool_timeout: int = 30,
                     pool_recycle: int = 3600,
                     pool_pre_ping: bool = True,
                     echo: bool = False,
                     engine_name: str = 'default') -> Engine:
        """
        Create SQLAlchemy engine with connection pooling.
        
        Args:
            connection_string: Database connection string
            pool_size: Number of connections to maintain in pool
            max_overflow: Maximum overflow connections beyond pool_size
            pool_timeout: Timeout for getting connection from pool
            pool_recycle: Recycle connections after this many seconds
            pool_pre_ping: Test connections before using
            echo: Echo SQL statements to log
            engine_name: Unique name for this engine
            
        Returns:
            SQLAlchemy Engine instance
        """
        if engine_name in cls._engines:
            logger.info(f"Reusing existing engine: {engine_name}")
            return cls._engines[engine_name]
        
        try:
            engine = create_engine(
                connection_string,
                poolclass=pool.QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                pool_pre_ping=pool_pre_ping,
                echo=echo,
                connect_args={
                    'connect_timeout': 10
                }
            )
            
            # Add event listeners for connection lifecycle
            cls._setup_event_listeners(engine)
            
            cls._engines[engine_name] = engine
            logger.info(f"Created database engine: {engine_name}")
            
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise DatabaseError(f"Engine creation failed: {e}")
    
    @classmethod
    def _setup_event_listeners(cls, engine: Engine) -> None:
        """
        Setup SQLAlchemy event listeners for monitoring.
        
        Args:
            engine: SQLAlchemy engine
        """
        @event.listens_for(engine, "connect")
        def receive_connect(dbapi_conn, connection_record):
            logger.debug("Database connection established")
        
        @event.listens_for(engine, "checkout")
        def receive_checkout(dbapi_conn, connection_record, connection_proxy):
            logger.debug("Connection checked out from pool")
        
        @event.listens_for(engine, "checkin")
        def receive_checkin(dbapi_conn, connection_record):
            logger.debug("Connection returned to pool")
    
    @classmethod
    def create_session_factory(cls,
                              engine: Engine,
                              autocommit: bool = False,
                              autoflush: bool = False,
                              expire_on_commit: bool = True,
                              factory_name: str = 'default') -> sessionmaker:
        """
        Create session factory for the given engine.
        
        Args:
            engine: SQLAlchemy engine
            autocommit: Enable autocommit mode
            autoflush: Enable autoflush mode
            expire_on_commit: Expire objects on commit
            factory_name: Unique name for this factory
            
        Returns:
            SQLAlchemy sessionmaker
        """
        if factory_name in cls._session_factories:
            return cls._session_factories[factory_name]
        
        session_factory = sessionmaker(
            bind=engine,
            autocommit=autocommit,
            autoflush=autoflush,
            expire_on_commit=expire_on_commit
        )
        
        cls._session_factories[factory_name] = session_factory
        logger.info(f"Created session factory: {factory_name}")
        
        return session_factory
    
    @classmethod
    def get_scoped_session(cls, factory_name: str = 'default') -> scoped_session:
        """
        Get thread-local scoped session.
        
        Args:
            factory_name: Name of session factory
            
        Returns:
            Scoped session
        """
        if factory_name not in cls._session_factories:
            raise DatabaseError(f"Session factory not found: {factory_name}")
        
        return scoped_session(cls._session_factories[factory_name])
    
    @classmethod
    @contextmanager
    def get_session(cls, 
                   factory_name: str = 'default',
                   auto_commit: bool = True) -> Generator[Session, None, None]:
        """
        Context manager for database sessions with automatic cleanup.
        
        Args:
            factory_name: Name of session factory to use
            auto_commit: Automatically commit on success
            
        Yields:
            Database session
            
        Example:
            with DatabaseSessionFactory.get_session() as session:
                result = session.query(Customer).all()
        """
        if factory_name not in cls._session_factories:
            raise DatabaseError(f"Session factory not found: {factory_name}")
        
        session = cls._session_factories[factory_name]()
        
        try:
            yield session
            if auto_commit:
                session.commit()
                logger.debug("Session committed successfully")
        except exc.SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error, rolling back: {e}")
            raise DatabaseError(f"Database operation failed: {e}")
        except Exception as e:
            session.rollback()
            logger.error(f"Unexpected error, rolling back: {e}")
            raise
        finally:
            session.close()
            logger.debug("Session closed")
    
    @classmethod
    def execute_with_retry(cls,
                          session: Session,
                          operation: callable,
                          max_retries: int = 3,
                          retry_delay: float = 1.0) -> Any:
        """
        Execute database operation with retry logic.
        
        Args:
            session: Database session
            operation: Callable operation to execute
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            
        Returns:
            Operation result
            
        Raises:
            DatabaseError: If all retries fail
        """
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                result = operation(session)
                session.commit()
                return result
                
            except exc.OperationalError as e:
                last_exception = e
                session.rollback()
                
                if attempt < max_retries - 1:
                    logger.warning(f"Database operation failed (attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    logger.error(f"Database operation failed after {max_retries} attempts")
                    
            except Exception as e:
                session.rollback()
                logger.error(f"Non-retryable database error: {e}")
                raise DatabaseError(f"Database operation failed: {e}")
        
        raise DatabaseError(f"Operation failed after {max_retries} retries: {last_exception}")
    
    @classmethod
    def get_connection_pool_status(cls, engine_name: str = 'default') -> Dict[str, Any]:
        """
        Get connection pool status information.
        
        Args:
            engine_name: Name of engine to check
            
        Returns:
            Dictionary with pool status
        """
        if engine_name not in cls._engines:
            raise DatabaseError(f"Engine not found: {engine_name}")
        
        engine = cls._engines[engine_name]
        pool_obj = engine.pool
        
        return {
            'size': pool_obj.size(),
            'checked_in': pool_obj.checkedin(),
            'checked_out': pool_obj.checkedout(),
            'overflow': pool_obj.overflow(),
            'total_connections': pool_obj.size() + pool_obj.overflow()
        }
    
    @classmethod
    def dispose_engine(cls, engine_name: str = 'default') -> None:
        """
        Dispose of engine and close all connections.
        
        Args:
            engine_name: Name of engine to dispose
        """
        if engine_name in cls._engines:
            cls._engines[engine_name].dispose()
            del cls._engines[engine_name]
            logger.info(f"Disposed engine: {engine_name}")
        
        if engine_name in cls._session_factories:
            del cls._session_factories[engine_name]
    
    @classmethod
    def dispose_all(cls) -> None:
        """Dispose all engines and close all connections."""
        for engine_name in list(cls._engines.keys()):
            cls.dispose_engine(engine_name)
        logger.info("Disposed all database engines")


class ConnectionStringBuilder:
    """
    Utility class for building database connection strings.
    """
    
    @staticmethod
    def build_postgresql(host: str,
                        port: int,
                        database: str,
                        username: str,
                        password: str,
                        **kwargs) -> str:
        """Build PostgreSQL connection string."""
        encoded_password = quote_plus(password)
        conn_str = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"
        
        if kwargs:
            params = '&'.join([f"{k}={v}" for k, v in kwargs.items()])
            conn_str += f"?{params}"
        
        return conn_str
    
    @staticmethod
    def build_mysql(host: str,
                   port: int,
                   database: str,
                   username: str,
                   password: str,
                   **kwargs) -> str:
        """Build MySQL connection string."""
        encoded_password = quote_plus(password)
        conn_str = f"mysql+pymysql://{username}:{encoded_password}@{host}:{port}/{database}"
        
        if kwargs:
            params = '&'.join([f"{k}={v}" for k, v in kwargs.items()])
            conn_str += f"?{params}"
        
        return conn_str
    
    @staticmethod
    def build_oracle(host: str,
                    port: int,
                    service_name: str,
                    username: str,
                    password: str,
                    **kwargs) -> str:
        """Build Oracle connection string."""
        encoded_password = quote_plus(password)
        conn_str = f"oracle+cx_oracle://{username}:{encoded_password}@{host}:{port}/?service_name={service_name}"
        
        if kwargs:
            params = '&'.join([f"{k}={v}" for k, v in kwargs.items()])
            conn_str += f"&{params}"
        
        return conn_str
    
    @staticmethod
    def build_mssql(host: str,
                   port: int,
                   database: str,
                   username: str,
                   password: str,
                   driver: str = 'ODBC Driver 17 for SQL Server',
                   **kwargs) -> str:
        """Build Microsoft SQL Server connection string."""
        encoded_password = quote_plus(password)
        encoded_driver = quote_plus(driver)
        conn_str = f"mssql+pyodbc://{username}:{encoded_password}@{host}:{port}/{database}?driver={encoded_driver}"
        
        if kwargs:
            params = '&'.join([f"{k}={v}" for k, v in kwargs.items()])
            conn_str += f"&{params}"
        
        return conn_str