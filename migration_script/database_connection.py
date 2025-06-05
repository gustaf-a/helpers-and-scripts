import psycopg2
import psycopg2.extras
import json
import time
import logging
from typing import List, Dict, Any
from database_config import DatabaseConfig

# Register JSON adapters for psycopg2
psycopg2.extras.register_json_oid = lambda conn, oid, array_oid=None: None
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extras.register_default_jsonb(loads=lambda x: x)

class DatabaseConnection:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def get_connection(self, config: DatabaseConfig, autocommit: bool = False):
        """Create database connection with retry logic"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    host=config.host,
                    port=config.port,
                    user=config.user,
                    password=config.password,
                    database=config.database,
                    connect_timeout=30,
                    application_name="PostgreSQL_Migrator"
                )
                if autocommit:
                    conn.autocommit = True
                
                # Register adapters for complex types
                psycopg2.extras.register_json_oid(conn, None, None)
                psycopg2.extras.register_default_json(conn, loads=json.loads)
                psycopg2.extras.register_default_jsonb(conn, loads=json.loads)
                
                return conn
            except psycopg2.Error as e:
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise
    
    def get_table_list(self, conn, schema: str) -> List[str]:
        """Get list of tables from database"""
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema,))
            return [row[0] for row in cursor.fetchall()]
    
    def get_identity_columns(self, conn, schema: str, table_name: str) -> List[str]:
        """Get list of identity columns for a table"""
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s 
                AND table_name = %s 
                AND is_identity = 'YES'
                AND identity_generation = 'ALWAYS'
            """, (schema, table_name))
            
            return [row[0] for row in cursor.fetchall()]
    
    def get_table_info(self, conn, schema: str, table_name: str) -> tuple:
        """Get table information including row count, primary key, columns, and identity columns"""
        with conn.cursor() as cursor:
            # Get row count
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            row_count = cursor.fetchone()[0]
            
            # Get primary key
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                ORDER BY a.attnum
            """, (f'"{schema}"."{table_name}"',))
            
            pk_result = cursor.fetchall()
            primary_key = pk_result[0][0] if pk_result and pk_result[0] else None
            
            # Get column names
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            columns = [row[0] for row in cursor.fetchall()]
            
            # Get identity columns
            identity_columns = self.get_identity_columns(conn, schema, table_name)
            
            return row_count, primary_key, columns, identity_columns
    
    def get_column_types(self, conn, schema: str, table_name: str) -> Dict[str, str]:
        """Get column types for a table"""
        with conn.cursor() as cursor:
            try:
                query = """
                    SELECT
                        c.column_name,
                        CASE
                            WHEN c.data_type = 'ARRAY' THEN
                                CASE
                                    WHEN e.data_type = 'USER-DEFINED' THEN e.udt_name || '[]'
                                    WHEN e.data_type IS NOT NULL THEN e.data_type || '[]'
                                    ELSE NULL
                                END
                            WHEN c.data_type = 'USER-DEFINED' THEN c.udt_name
                            ELSE c.data_type
                        END AS effective_data_type
                    FROM information_schema.columns c
                    LEFT JOIN information_schema.element_types e
                        ON c.data_type = 'ARRAY'
                        AND c.dtd_identifier = e.collection_type_identifier
                        AND c.table_catalog = e.object_catalog
                        AND c.table_schema = e.object_schema
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position;
                """
                cursor.execute(query, (schema, table_name))
                
                result = {}
                for row in cursor.fetchall():
                    col_name, col_type = row
                    if col_type is None:
                        self.logger.warning(f"Column {col_name} in table {table_name} has null type, defaulting to 'text'")
                        col_type = 'text'
                    result[col_name] = col_type
                    self.logger.debug(f"Column {col_name} in table {table_name} has type: {col_type}")
                
                return result
            except Exception as e:
                self.logger.error(f"Failed to get column types for table {table_name}: {e}")
                return {}
