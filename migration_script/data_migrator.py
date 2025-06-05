import psycopg2
import psycopg2.extras
import json
import time
import logging
from typing import Any, List, Dict
from database_config import DatabaseConfig
from database_connection import DatabaseConnection
from progress_tracker import ProgressTracker, MigrationProgress

class DataMigrator:
    def __init__(self, source_config: DatabaseConfig, target_config: DatabaseConfig, 
                 chunk_size: int, db_connection: DatabaseConnection, 
                 progress_tracker: ProgressTracker, logger: logging.Logger):
        self.source_config = source_config
        self.target_config = target_config
        self.chunk_size = chunk_size
        self.db_connection = db_connection
        self.progress_tracker = progress_tracker
        self.logger = logger
    
    def convert_data_for_insert(self, value: Any, column_type: str) -> Any:
        """Convert data types for PostgreSQL insertion"""
        if value is None:
            return None
        
        if column_type is None:
            self.logger.warning(f"Column type is None, treating as text")
            column_type = 'text'
        
        # Handle JSON/JSONB types
        if column_type.lower() in ('json', 'jsonb'):
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            elif isinstance(value, str):
                try:
                    json.loads(value)
                    return value
                except json.JSONDecodeError:
                    return json.dumps(value)
            return json.dumps(value)
        
        # Handle array types
        if column_type.endswith('[]') or 'ARRAY' in column_type.upper():
            if isinstance(value, (list, tuple)):
                return list(value)
            elif isinstance(value, str):
                if value.startswith('{') and value.endswith('}'):
                    inner = value[1:-1].strip()
                    if not inner:
                        return []
                    
                    array_elements = []
                    for item in inner.split(','):
                        item = item.strip()
                        if not item:
                            continue
                        
                        base_type = column_type.replace('[]', '').strip().lower()
                        
                        try:
                            if base_type in ('bigint', 'int8', 'integer', 'int', 'int4', 'smallint', 'int2'):
                                array_elements.append(int(item))
                            elif base_type in ('real', 'float4', 'double precision', 'float8', 'numeric', 'decimal'):
                                array_elements.append(float(item))
                            elif base_type in ('boolean', 'bool'):
                                array_elements.append(item.lower() in ('true', 't', '1', 'yes', 'on'))
                            else:
                                clean_item = item.strip('"\'')
                                array_elements.append(clean_item)
                        except (ValueError, TypeError) as e:
                            self.logger.warning(f"Could not convert array element '{item}' for type {base_type}: {e}")
                            array_elements.append(item.strip('"\''))
                    
                    self.logger.debug(f"Converted PostgreSQL array '{value}' to Python list: {array_elements}")
                    return array_elements
                
                elif value.startswith('[') and value.endswith(']'):
                    try:
                        parsed_array = json.loads(value)
                        if isinstance(parsed_array, list):
                            base_type = column_type.replace('[]', '').strip().lower()
                            converted_array = []
                            for item in parsed_array:
                                try:
                                    if base_type in ('bigint', 'int8', 'integer', 'int', 'int4', 'smallint', 'int2'):
                                        converted_array.append(int(item))
                                    elif base_type in ('real', 'float4', 'double precision', 'float8', 'numeric', 'decimal'):
                                        converted_array.append(float(item))
                                    else:
                                        converted_array.append(item)
                                except (ValueError, TypeError):
                                    converted_array.append(item)
                            return converted_array
                        return parsed_array
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Could not parse JSON array '{value}': {e}")
                        return [value]
                else:
                    return [value]
            
            return [value]
        
        # Handle vector types (pgvector)
        if 'vector' in column_type.lower():
            if isinstance(value, (list, tuple)):
                return f"[{','.join(map(str, value))}]"
            return value
        
        # Handle other complex types
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        
        return value
    
    def migrate_table_data(self, source_conn, target_conn, table_name: str, 
                          columns: List[str], primary_key: str, total_rows: int, identity_columns: List[str] = None):
        """Migrate table data in chunks"""
        if identity_columns is None:
            identity_columns = []
            
        progress = self.progress_tracker.get_progress(table_name)
        if not progress:
            progress = MigrationProgress(
                table_name=table_name, total_rows=total_rows, migrated_rows=0
            )
        
        if progress.completed:
            self.logger.info(f"Table {table_name} already completed")
            return
        
        # Get column types for proper data conversion
        column_types = self.db_connection.get_column_types(source_conn, self.source_config.schema, table_name)
        
        if not column_types:
            self.logger.warning(f"Could not retrieve column types for table {table_name}, using 'text' as default")
            column_types = {col: 'text' for col in columns}
        
        with source_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as source_cursor:
            column_list = ', '.join(f'"{col}"' for col in columns)
            
            while progress.migrated_rows < total_rows:
                try:
                    if progress.last_primary_key and primary_key:
                        query = f'''
                            SELECT {column_list}
                            FROM "{self.source_config.schema}"."{table_name}"
                            WHERE "{primary_key}" > %s
                            ORDER BY "{primary_key}"
                            LIMIT %s
                        '''
                        params = [progress.last_primary_key, self.chunk_size]
                    else:
                        query = f'''
                            SELECT {column_list}
                            FROM "{self.source_config.schema}"."{table_name}"
                            {f'ORDER BY "{primary_key}"' if primary_key else ""}
                            LIMIT %s OFFSET %s
                        '''
                        params = [self.chunk_size, progress.migrated_rows]
                    
                    self.logger.debug(f"Executing query for {table_name}: {query} with params: {params}")
                    source_cursor.execute(query, params)
                    rows = source_cursor.fetchall()
                    
                    if not rows:
                        self.logger.info(f"No more rows found for table {table_name}, marking as completed")
                        break
                    
                    # Insert chunk into target with data conversion
                    with target_conn.cursor() as target_cursor:
                        placeholders = ', '.join(['%s'] * len(columns))
                        
                        # Check if we need to override identity columns
                        override_clause = ""
                        if identity_columns:
                            has_identity_in_columns = any(col in identity_columns for col in columns)
                            if has_identity_in_columns:
                                override_clause = " OVERRIDING SYSTEM VALUE"
                        
                        insert_query = f'''
                            INSERT INTO "{self.target_config.schema}"."{table_name}" 
                            ({column_list}){override_clause} VALUES ({placeholders})
                            ON CONFLICT DO NOTHING
                        '''
                        
                        # Convert data for each row
                        converted_rows = []
                        for row in rows:
                            converted_row = []
                            for col in columns:
                                col_type = column_types.get(col, 'text')
                                try:
                                    converted_value = self.convert_data_for_insert(row[col], col_type)
                                    converted_row.append(converted_value)
                                except Exception as e:
                                    self.logger.error(f"Error converting value for column {col} (type: {col_type}) in table {table_name}: {e}")
                                    converted_row.append(row[col])
                            converted_rows.append(tuple(converted_row))
                        
                        psycopg2.extras.execute_batch(
                            target_cursor, insert_query, 
                            converted_rows,
                            page_size=min(100, len(converted_rows))
                        )
                        target_conn.commit()
                    
                    # Update progress
                    progress.migrated_rows += len(rows)
                    if primary_key and rows:
                        progress.last_primary_key = str(rows[-1][primary_key])
                    
                    self.progress_tracker.update_progress(table_name, progress)
                    
                    self.logger.info(f"Table {table_name}: {progress.migrated_rows}/{total_rows} rows migrated")
                    
                    # Rate limiting
                    time.sleep(0.1)
                    
                    if progress.migrated_rows >= total_rows:
                        self.logger.info(f"Table {table_name}: All {total_rows} rows processed")
                        break
                        
                    if len(rows) < self.chunk_size and progress.migrated_rows >= total_rows:
                        self.logger.info(f"Table {table_name}: Reached end of data (got {len(rows)} rows, expected {self.chunk_size})")
                        break
                    
                except Exception as e:
                    self.logger.error(f"Error migrating chunk for table {table_name}: {e}")
                    target_conn.rollback()
                    time.sleep(5)
                    continue
        
        progress.completed = True
        self.progress_tracker.update_progress(table_name, progress)
        self.logger.info(f"Table {table_name} migration completed")
