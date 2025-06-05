import logging
import argparse
import os
from datetime import datetime
from typing import List, Optional

from database_config import DatabaseConfig, load_config_from_file
from database_connection import DatabaseConnection
from schema_manager import SchemaManager
from data_migrator import DataMigrator
from progress_tracker import ProgressTracker

class PostgreSQLMigrator:
    def __init__(self, source_config: DatabaseConfig, target_config: DatabaseConfig, 
                 chunk_size: int = 1000, progress_file: str = "migration_progress.json"):
        self.source_config = source_config
        self.target_config = target_config
        self.chunk_size = chunk_size
        self.logger = self._setup_logging()
        
        # Initialize components
        self.db_connection = DatabaseConnection(self.logger)
        self.progress_tracker = ProgressTracker(progress_file, self.logger)
        self.schema_manager = SchemaManager(source_config, target_config, self.logger)
        self.data_migrator = DataMigrator(
            source_config, target_config, chunk_size, 
            self.db_connection, self.progress_tracker, self.logger
        )
        
    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging"""
        # Ensure logging directory exists
        logging_dir = "logging"
        os.makedirs(logging_dir, exist_ok=True)
        
        logger = logging.getLogger('postgresql_migrator')
        logger.setLevel(logging.INFO)
        
        # File handler - save to logging directory
        log_filename = f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        log_filepath = os.path.join(logging_dir, log_filename)
        file_handler = logging.FileHandler(log_filepath)
        file_handler.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def migrate(self, tables: Optional[List[str]] = None):
        """Main migration method"""
        self.logger.info("Starting PostgreSQL migration")
        
        source_conn = None
        target_conn = None
        
        try:
            # Establish connections
            source_conn = self.db_connection.get_connection(self.source_config)
            target_conn = self.db_connection.get_connection(self.target_config)
            
            # Ensure required extensions are available
            self.schema_manager.ensure_extensions(target_conn)
            
            # Create sequences first - this is critical for tables with auto-increment columns
            try:
                self.schema_manager.create_sequences(source_conn, target_conn)
            except Exception as e:
                self.logger.error(f"Failed to create sequences: {e}")
                self.logger.warning("Continuing migration - tables with sequence dependencies may fail")
            
            # Get table list
            if not tables:
                tables = self.db_connection.get_table_list(source_conn, self.source_config.schema)
            
            self.logger.info(f"Found {len(tables)} tables to migrate")
            
            for table_name in tables:
                self.logger.info(f"Starting migration for table: {table_name}")
                
                try:
                    # Get table info
                    row_count, primary_key, columns, identity_columns = self.db_connection.get_table_info(
                        source_conn, self.source_config.schema, table_name
                    )
                    
                    if row_count == 0:
                        self.logger.info(f"Table {table_name} is empty, skipping")
                        continue
                    
                    # Create table in target
                    self.schema_manager.create_table_if_not_exists(source_conn, target_conn, table_name)
                    
                    # Migrate data
                    self.data_migrator.migrate_table_data(
                        source_conn, target_conn, table_name, columns, primary_key, row_count, identity_columns
                    )
                    
                except Exception as e:
                    self.logger.error(f"Failed to migrate table {table_name}: {e}")
                    # Continue with next table instead of failing completely
                    continue
            
            # Set sequence ownership after all tables are created
            try:
                self.schema_manager.set_sequence_ownership(source_conn, target_conn)
            except Exception as e:
                self.logger.warning(f"Failed to set sequence ownership: {e}")
            
            # Update sequence values to current values from source
            try:
                self.schema_manager.update_sequence_values(source_conn, target_conn)
            except Exception as e:
                self.logger.warning(f"Failed to update sequence values: {e}")
            
            self.logger.info("Migration completed successfully")
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            raise
        finally:
            if source_conn:
                source_conn.close()
            if target_conn:
                target_conn.close()

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Database Migration Tool')
    parser.add_argument('--source-config', default='../env_prod', help='Source database config file')
    parser.add_argument('--target-config', default='../env_stg', help='Target database config file')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Chunk size for data transfer')
    parser.add_argument('--tables', nargs='*', help='Specific tables to migrate (optional)')
    
    args = parser.parse_args()
    
    # Load configurations
    source_config = load_config_from_file(args.source_config)
    target_config = load_config_from_file(args.target_config)
    
    # Create migrator
    migrator = PostgreSQLMigrator(
        source_config=source_config,
        target_config=target_config,
        chunk_size=args.chunk_size
    )
    
    # Run migration
    migrator.migrate(tables=args.tables)

if __name__ == "__main__":
    main()
