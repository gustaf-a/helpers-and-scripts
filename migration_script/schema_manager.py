import psycopg2
import logging
from typing import List, Dict, Any
from database_config import DatabaseConfig

class SchemaManager:
    def __init__(self, source_config: DatabaseConfig, target_config: DatabaseConfig, logger: logging.Logger):
        self.source_config = source_config
        self.target_config = target_config
        self.logger = logger
    
    def get_sequences_for_schema(self, conn, schema: str) -> List[Dict[str, Any]]:
        """Get all sequences in the schema with their details"""
        with conn.cursor() as cursor:
            try:
                # First try the simple approach to get basic sequence info
                cursor.execute("""
                    SELECT 
                        sequence_name,
                        data_type,
                        start_value,
                        minimum_value,
                        maximum_value,
                        increment,
                        cycle_option
                    FROM information_schema.sequences 
                    WHERE sequence_schema = %s
                    ORDER BY sequence_name
                """, (schema,))
                
                sequences = []
                rows = cursor.fetchall()
                self.logger.debug(f"Found {len(rows)} sequences in schema {schema}")
                
                for row in rows:
                    try:
                        if len(row) < 7:
                            self.logger.error(f"Unexpected row length {len(row)} for sequence query result: {row}")
                            continue
                        
                        seq_name = row[0]
                        
                        # Try to find the owner table/column for this sequence
                        owner_table_column = None
                        full_sequence_name = f'"{schema}"."{seq_name}"'
                        
                        try:
                            cursor.execute("""
                                SELECT t.table_name, c.column_name
                                FROM information_schema.columns c
                                JOIN information_schema.tables t ON c.table_name = t.table_name AND c.table_schema = t.table_schema
                                WHERE c.column_default LIKE %s
                                  AND t.table_schema = %s
                                LIMIT 1
                            """, (f'%{seq_name}%', schema))
                            
                            owner_result = cursor.fetchone()
                            if owner_result:
                                owner_table_column = f"{owner_result[0]}.{owner_result[1]}"
                        except Exception as e:
                            self.logger.debug(f"Could not determine owner for sequence {seq_name}: {e}")
                        
                        sequences.append({
                            'name': seq_name,
                            'data_type': row[1] or 'bigint',
                            'start_value': row[2] or 1,
                            'minimum_value': row[3] or 1,
                            'maximum_value': row[4] or 9223372036854775807,
                            'increment': row[5] or 1,
                            'cycle_option': row[6] or 'NO',
                            'full_name': full_sequence_name,
                            'owner_table_column': owner_table_column
                        })
                        self.logger.debug(f"Added sequence: {seq_name}")
                        
                    except Exception as e:
                        self.logger.error(f"Error processing sequence row {row}: {e}")
                        continue
                
                return sequences
                
            except psycopg2.Error as e:
                self.logger.error(f"Database error while fetching sequences for schema {schema}: {e}")
                # Try fallback method
                return self._get_sequences_fallback(conn, schema)
            except Exception as e:
                self.logger.error(f"Unexpected error while fetching sequences for schema {schema}: {e}")
                # Try fallback method
                return self._get_sequences_fallback(conn, schema)

    def _get_sequences_fallback(self, conn, schema: str) -> List[Dict[str, Any]]:
        """Fallback method to get sequences using pg_class"""
        self.logger.info("Using fallback method to fetch sequences")
        sequences = []
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT c.relname as sequence_name
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE c.relkind = 'S' AND n.nspname = %s
                    ORDER BY c.relname
                """, (schema,))
                
                for row in cursor.fetchall():
                    seq_name = row[0]
                    sequences.append({
                        'name': seq_name,
                        'data_type': 'bigint',
                        'start_value': 1,
                        'minimum_value': 1,
                        'maximum_value': 9223372036854775807,
                        'increment': 1,
                        'cycle_option': 'NO',
                        'full_name': f'"{schema}"."{seq_name}"',
                        'owner_table_column': None
                    })
                    self.logger.debug(f"Added sequence from fallback: {seq_name}")
                
        except Exception as e:
            self.logger.error(f"Fallback sequence detection also failed: {e}")
        
        return sequences

    def create_sequences(self, source_conn, target_conn):
        """Create all sequences in target database"""
        self.logger.info("Creating sequences in target database")
        
        try:
            sequences = self.get_sequences_for_schema(source_conn, self.source_config.schema)
            
            if not sequences:
                self.logger.info("No sequences found to create")
                return
            
            created_count = 0
            with target_conn.cursor() as target_cursor:
                for seq in sequences:
                    try:
                        if not seq.get('name'):
                            self.logger.warning(f"Skipping sequence with no name: {seq}")
                            continue
                            
                        # Check if sequence already exists
                        target_cursor.execute("""
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.sequences 
                                WHERE sequence_schema = %s AND sequence_name = %s
                            )
                        """, (self.target_config.schema, seq['name']))
                        
                        if target_cursor.fetchone()[0]:
                            self.logger.info(f"Sequence {seq['name']} already exists")
                            continue
                        
                        # Create sequence with proper defaults
                        seq_def = f'''
                            CREATE SEQUENCE "{self.target_config.schema}"."{seq['name']}"
                            AS {seq.get('data_type', 'bigint')}
                            START WITH {seq.get('start_value', 1)}
                            INCREMENT BY {seq.get('increment', 1)}
                            MINVALUE {seq.get('minimum_value', 1)}
                            MAXVALUE {seq.get('maximum_value', 9223372036854775807)}
                        '''
                        
                        if seq.get('cycle_option') == 'YES':
                            seq_def += ' CYCLE'
                        else:
                            seq_def += ' NO CYCLE'
                        
                        self.logger.debug(f"Creating sequence with definition: {seq_def}")
                        target_cursor.execute(seq_def)
                        created_count += 1
                        
                        self.logger.info(f"Created sequence: {seq['name']}")
                        
                    except Exception as e:
                        self.logger.error(f"Failed to create sequence {seq.get('name', 'unknown')}: {e}")
                        # Don't rollback here, continue with other sequences
                        continue
            
            if created_count > 0:
                target_conn.commit()
                self.logger.info(f"Successfully created {created_count} sequences")
            else:
                self.logger.info("No new sequences were created")
                
        except Exception as e:
            self.logger.error(f"Error in create_sequences: {e}")
            try:
                target_conn.rollback()
            except:
                pass
            self.logger.warning("Continuing migration without sequence creation")
    
    def update_sequence_values(self, source_conn, target_conn):
        """Update sequence current values to match source database"""
        self.logger.info("Updating sequence current values")
        
        sequences = self.get_sequences_for_schema(source_conn, self.source_config.schema)
        
        with source_conn.cursor() as source_cursor, target_conn.cursor() as target_cursor:
            for seq in sequences:
                try:
                    # Get current value from source sequence
                    source_cursor.execute(f'SELECT last_value FROM "{self.source_config.schema}"."{seq["name"]}"')
                    result = source_cursor.fetchone()
                    if result:
                        current_value = result[0]
                        
                        # Set the sequence value in target
                        target_cursor.execute(
                            f'SELECT setval(%s, %s)',
                            (f'"{self.target_config.schema}"."{seq["name"]}"', current_value)
                        )
                        
                        self.logger.info(f"Updated sequence {seq['name']} to value: {current_value}")
                    
                except Exception as e:
                    self.logger.warning(f"Could not update sequence {seq['name']}: {e}")
                    target_conn.rollback()
                    continue
            
            target_conn.commit()
    
    def set_sequence_ownership(self, source_conn, target_conn):
        """Set sequence ownership to match source database"""
        self.logger.info("Setting sequence ownership")
        
        sequences = self.get_sequences_for_schema(source_conn, self.source_config.schema)
        
        with target_conn.cursor() as target_cursor:
            for seq in sequences:
                if seq['owner_table_column']:
                    try:
                        table_name, column_name = seq['owner_table_column'].split('.')
                        
                        # Set sequence ownership
                        alter_cmd = f'''
                            ALTER SEQUENCE "{self.target_config.schema}"."{seq['name']}" 
                            OWNED BY "{self.target_config.schema}"."{table_name}"."{column_name}"
                        '''
                        
                        target_cursor.execute(alter_cmd)
                        self.logger.info(f"Set sequence {seq['name']} ownership to {table_name}.{column_name}")
                        
                    except Exception as e:
                        self.logger.warning(f"Could not set ownership for sequence {seq['name']}: {e}")
                        target_conn.rollback()
                        continue
            
            target_conn.commit()
    
    def ensure_extensions(self, target_conn):
        """Ensure required extensions exist in target database"""
        self.logger.info("Checking and creating required extensions")
        
        extensions_to_check = ['vector', 'uuid-ossp', 'pg_trgm']
        
        with target_conn.cursor() as cursor:
            for ext in extensions_to_check:
                try:
                    cursor.execute(f"CREATE EXTENSION IF NOT EXISTS \"{ext}\"")
                    target_conn.commit()
                    self.logger.info(f"Extension {ext} is available")
                except psycopg2.Error as e:
                    self.logger.warning(f"Could not create extension {ext}: {e}")
                    target_conn.rollback()
    
    def create_table_if_not_exists(self, source_conn, target_conn, table_name: str):
        """Create table in target database if it doesn't exist"""
        self.logger.info(f"Starting table creation process for {table_name}")
        
        # ...existing table creation logic...
        try:
            with source_conn.cursor() as source_cursor:
                self.logger.debug(f"Querying column information for table {table_name}")
                source_cursor.execute("""
                    SELECT c.column_name, c.data_type, c.character_maximum_length, 
                           c.is_nullable, c.column_default, c.numeric_precision, c.numeric_scale,
                           c.udt_name,
                           CASE 
                               WHEN c.data_type = 'ARRAY' THEN 
                                   CASE 
                                       WHEN e.data_type = 'USER-DEFINED' THEN e.udt_name || '[]'
                                       WHEN e.data_type IS NOT NULL THEN e.data_type || '[]'
                                       ELSE NULL
                                   END
                               ELSE NULL
                           END as array_element_type_with_brackets
                    FROM information_schema.columns c
                    LEFT JOIN information_schema.element_types e 
                        ON c.data_type = 'ARRAY'
                        AND c.dtd_identifier = e.collection_type_identifier
                        AND c.table_catalog = e.object_catalog
                        AND c.table_schema = e.object_schema
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position
                """, (self.source_config.schema, table_name))
                
                columns_info = source_cursor.fetchall()
                if not columns_info:
                    self.logger.error(f"No columns found for table {table_name}")
                    raise Exception(f"No columns found for table {table_name}")
                
                self.logger.debug(f"Found {len(columns_info)} columns for table {table_name}")
                
                column_defs = []
                for col_data in columns_info:
                    col_name, data_type_val, max_length, nullable, default, num_precision, num_scale, udt_name_val, array_type_val = col_data
                    self.logger.debug(f"Processing column {col_name}: type={data_type_val}, udt_name={udt_name_val}, array_type_val={array_type_val}, default={default}")
                    
                    col_type_str = ""
                    if data_type_val == 'ARRAY':
                        if array_type_val:
                            col_type_str = array_type_val
                            self.logger.debug(f"Using determined array type {array_type_val} for column {col_name}")
                        else:
                            self.logger.warning(f"Could not determine element type for array column {col_name}. Defaulting to udt_name: {udt_name_val} or TEXT if udt_name is also problematic.")
                            col_type_str = udt_name_val
                            if not col_type_str:
                                 col_type_str = "TEXT[]"
                    elif data_type_val == 'USER-DEFINED':
                        col_type_str = udt_name_val
                        self.logger.debug(f"Using USER-DEFINED type {udt_name_val} for column {col_name}")
                    else:
                        col_type_str = data_type_val
                    
                    col_def = f'"{col_name}" {col_type_str}'
                    
                    if data_type_val != 'ARRAY':
                        if data_type_val in ('character varying', 'varchar', 'char') and max_length:
                            col_def = f'"{col_name}" {col_type_str}({max_length})'
                            self.logger.debug(f"Added length constraint ({max_length}) to column {col_name}")
                        elif data_type_val in ('numeric', 'decimal') and num_precision:
                            if num_scale:
                                col_def = f'"{col_name}" {col_type_str}({num_precision},{num_scale})'
                                self.logger.debug(f"Added precision and scale ({num_precision},{num_scale}) to column {col_name}")
                            else:
                                col_def = f'"{col_name}" {col_type_str}({num_precision})'
                                self.logger.debug(f"Added precision ({num_precision}) to column {col_name}")
                    
                    if nullable == 'NO':
                        col_def += ' NOT NULL'
                        self.logger.debug(f"Added NOT NULL constraint to column {col_name}")
                    
                    # Handle default values, including sequence-based ones
                    if default:
                        if 'nextval(' in default:
                            adjusted_default = default.replace(
                                f"'{self.source_config.schema}.",
                                f"'{self.target_config.schema}."
                            )
                            col_def += f' DEFAULT {adjusted_default}'
                            self.logger.debug(f"Added sequence-based default value ({adjusted_default}) to column {col_name}")
                        else:
                            col_def += f' DEFAULT {default}'
                            self.logger.debug(f"Added default value ({default}) to column {col_name}")
                    
                    column_defs.append(col_def)
                
                self.logger.debug(f"Created column definitions: {column_defs}")
                
                # Get primary key constraints
                self.logger.debug(f"Querying primary key constraints for table {table_name}")
                source_cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary
                    ORDER BY a.attnum
                """, (f'"{self.source_config.schema}"."{table_name}"',))
                
                pk_columns = [row[0] for row in source_cursor.fetchall()]
                self.logger.debug(f"Primary key columns for {table_name}: {pk_columns}")
                
                table_def = f'CREATE TABLE "{self.target_config.schema}"."{table_name}" ({", ".join(column_defs)})'
                
                if pk_columns:
                    pk_column_list = ', '.join(f'"{col}"' for col in pk_columns)
                    pk_constraint = f', PRIMARY KEY ({pk_column_list})'
                    table_def = table_def[:-1] + pk_constraint + ')'
                    self.logger.debug(f"Added primary key constraint for columns: {pk_columns}")
                
                self.logger.info(f"Successfully created table definition for {table_name}")
                self.logger.debug(f"Table definition for {table_name}: {table_def[:200]}...")
                
        except psycopg2.Error as e:
            source_conn.rollback()
            self.logger.error(f"Failed to get table structure for {table_name}: {e}")
            raise Exception(f"Failed to get table structure for {table_name}: {e}")
        
        # Create table in target database
        self.logger.info(f"Creating table {table_name} in target database")
        with target_conn.cursor() as target_cursor:
            target_cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (self.target_config.schema, table_name))
            
            if target_cursor.fetchone()[0]:
                self.logger.info(f"Table {table_name} already exists in target database")
                return
            
            self.logger.debug(f"Executing CREATE TABLE statement for {table_name}")
            self.logger.debug(f"Table definition: {table_def}")
            target_cursor.execute(table_def)
            target_conn.commit()
            self.logger.info(f"Successfully created table {table_name} in target database")
