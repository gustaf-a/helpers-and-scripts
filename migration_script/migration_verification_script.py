import psycopg2
import logging
import argparse
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from database_config import DatabaseConfig, load_config_from_file

@dataclass
class VerificationResult:
    table_name: str
    structure_match: bool
    row_count_match: bool
    source_row_count: int
    target_row_count: int
    errors: List[str]
    warnings: List[str]

class DatabaseVerifier:
    def __init__(self, source_config: DatabaseConfig, target_config: DatabaseConfig):
        self.source_config = source_config
        self.target_config = target_config
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging"""
        # Ensure logging directory exists
        logging_dir = "logging"
        os.makedirs(logging_dir, exist_ok=True)
        
        logger = logging.getLogger('database_verifier')
        logger.setLevel(logging.INFO)
        
        # File handler - save to logging directory
        log_filename = f'verification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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
    
    def _get_connection(self, config: DatabaseConfig):
        """Create database connection"""
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            connect_timeout=30,
            application_name="Database_Verifier"
        )
    
    def _get_table_list(self, conn, schema: str) -> List[str]:
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
    
    def _get_table_structure(self, conn, schema: str, table_name: str) -> Dict[str, Any]:
        """Get detailed table structure"""
        with conn.cursor() as cursor:
            # Get column information
            cursor.execute("""
                SELECT c.column_name, c.data_type, c.character_maximum_length, 
                       c.is_nullable, c.column_default, c.numeric_precision, c.numeric_scale,
                       c.udt_name,
                       CASE 
                           WHEN c.data_type = 'ARRAY' THEN 
                               CASE 
                                   WHEN e.data_type = 'USER-DEFINED' THEN e.udt_name || '[]'
                                   ELSE e.data_type || '[]'
                               END
                           ELSE NULL
                       END as array_type
                FROM information_schema.columns c
                LEFT JOIN information_schema.element_types e 
                    ON c.table_catalog = e.object_catalog
                    AND c.table_schema = e.object_schema
                    AND c.table_name = e.object_name
                    AND c.column_name = e.collation_name
                    AND c.data_type = 'ARRAY'
                WHERE c.table_schema = %s AND c.table_name = %s
                ORDER BY c.ordinal_position
            """, (schema, table_name))
            columns = cursor.fetchall()
            
            # Get primary key
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                ORDER BY a.attnum
            """, (f'"{schema}"."{table_name}"',))
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Get indexes
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s
                ORDER BY indexname
            """, (schema, table_name))
            indexes = cursor.fetchall()
            
            return {
                'columns': columns,
                'primary_keys': primary_keys,
                'indexes': indexes
            }
    
    def _get_row_count(self, conn, schema: str, table_name: str) -> int:
        """Get row count for a table"""
        with conn.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            return cursor.fetchone()[0]
    
    def _normalize_default_value(self, default_value: str, source_schema: str, target_schema: str) -> str:
        """Normalize default values for comparison, handling schema differences"""
        if not default_value:
            return default_value
        
        # Handle sequence-based defaults - normalize schema references
        if 'nextval(' in default_value:
            # Replace source schema with target schema for comparison
            normalized = default_value.replace(f"'{source_schema}.", f"'{target_schema}.")
            return normalized
        
        return default_value

    def _compare_column_structures(self, source_columns: List[Tuple], target_columns: List[Tuple], table_name: str) -> Tuple[bool, List[str]]:
        """Compare column structures in detail and return specific differences"""
        errors = []
        
        # Convert to dictionaries for easier comparison
        source_cols = {col[0]: col for col in source_columns}
        target_cols = {col[0]: col for col in target_columns}
        
        # Check for missing columns in target
        missing_in_target = set(source_cols.keys()) - set(target_cols.keys())
        if missing_in_target:
            errors.append(f"Columns missing in target: {', '.join(sorted(missing_in_target))}")
        
        # Check for extra columns in target
        extra_in_target = set(target_cols.keys()) - set(source_cols.keys())
        if extra_in_target:
            errors.append(f"Extra columns in target: {', '.join(sorted(extra_in_target))}")
        
        # Compare common columns
        common_columns = set(source_cols.keys()) & set(target_cols.keys())
        for col_name in sorted(common_columns):
            source_col = source_cols[col_name]
            target_col = target_cols[col_name]
            
            # Compare each attribute of the column
            col_errors = []
            
            if source_col[1] != target_col[1]:  # data_type
                col_errors.append(f"data_type: '{source_col[1]}' vs '{target_col[1]}'")
            
            if source_col[2] != target_col[2]:  # character_maximum_length
                col_errors.append(f"max_length: {source_col[2]} vs {target_col[2]}")
            
            if source_col[3] != target_col[3]:  # is_nullable
                col_errors.append(f"nullable: {source_col[3]} vs {target_col[3]}")
            
            # Compare defaults with normalization for sequence-based defaults
            source_default = source_col[4]
            target_default = target_col[4]
            
            if source_default or target_default:
                # Normalize both defaults for comparison
                normalized_source = self._normalize_default_value(
                    source_default or '', self.source_config.schema, self.target_config.schema
                )
                normalized_target = self._normalize_default_value(
                    target_default or '', self.source_config.schema, self.target_config.schema
                )
                
                if normalized_source != normalized_target:
                    # Only report as error if normalization didn't resolve the difference
                    # This handles cases where the only difference is schema name in sequence references
                    if not (('nextval(' in (source_default or '')) and ('nextval(' in (target_default or ''))):
                        col_errors.append(f"default: '{source_default}' vs '{target_default}'")
                    else:
                        # For sequence defaults, check if they reference equivalent sequences
                        source_seq_name = None
                        target_seq_name = None
                        
                        if source_default and 'nextval(' in source_default:
                            # Extract sequence name from nextval('schema.sequence_name'::regclass)
                            import re
                            match = re.search(r"nextval\('([^']+)'", source_default)
                            if match:
                                source_seq_name = match.group(1).split('.')[-1]  # Get just the sequence name
                        
                        if target_default and 'nextval(' in target_default:
                            match = re.search(r"nextval\('([^']+)'", target_default)
                            if match:
                                target_seq_name = match.group(1).split('.')[-1]  # Get just the sequence name
                        
                        if source_seq_name != target_seq_name:
                            col_errors.append(f"sequence_default: '{source_default}' vs '{target_default}'")
            
            if source_col[5] != target_col[5]:  # numeric_precision
                col_errors.append(f"precision: {source_col[5]} vs {target_col[5]}")
            
            if source_col[6] != target_col[6]:  # numeric_scale
                col_errors.append(f"scale: {source_col[6]} vs {target_col[6]}")
            
            if source_col[7] != target_col[7]:  # udt_name
                col_errors.append(f"udt_name: '{source_col[7]}' vs '{target_col[7]}'")
            
            if len(source_col) > 8 and len(target_col) > 8:
                if source_col[8] != target_col[8]:  # array_type
                    col_errors.append(f"array_type: '{source_col[8]}' vs '{target_col[8]}'")
            
            if col_errors:
                errors.append(f"Column '{col_name}' differences: {'; '.join(col_errors)}")
        
        return len(errors) == 0, errors

    def _compare_indexes(self, source_indexes: List[Tuple], target_indexes: List[Tuple]) -> Tuple[bool, List[str]]:
        """Compare index structures in detail"""
        errors = []
        
        source_idx_names = {idx[0] for idx in source_indexes}
        target_idx_names = {idx[0] for idx in target_indexes}
        
        missing_indexes = source_idx_names - target_idx_names
        if missing_indexes:
            errors.append(f"Missing indexes in target: {', '.join(sorted(missing_indexes))}")
        
        extra_indexes = target_idx_names - source_idx_names
        if extra_indexes:
            errors.append(f"Extra indexes in target: {', '.join(sorted(extra_indexes))}")
        
        # Compare common indexes
        source_idx_dict = {idx[0]: idx[1] for idx in source_indexes}
        target_idx_dict = {idx[0]: idx[1] for idx in target_indexes}
        
        common_indexes = source_idx_names & target_idx_names
        for idx_name in sorted(common_indexes):
            if source_idx_dict[idx_name] != target_idx_dict[idx_name]:
                errors.append(f"Index '{idx_name}' definition differs")
                self.logger.debug(f"Source index def: {source_idx_dict[idx_name]}")
                self.logger.debug(f"Target index def: {target_idx_dict[idx_name]}")
        
        return len(errors) == 0, errors

    def _verify_table(self, source_conn, target_conn, table_name: str) -> VerificationResult:
        """Verify a single table"""
        self.logger.info(f"Verifying table: {table_name}")
        
        errors = []
        warnings = []
        structure_match = True
        row_count_match = True
        
        try:
            # Check if table exists in both databases
            source_tables = self._get_table_list(source_conn, self.source_config.schema)
            target_tables = self._get_table_list(target_conn, self.target_config.schema)
            
            if table_name not in source_tables:
                errors.append(f"Table {table_name} not found in source database")
                return VerificationResult(
                    table_name=table_name, structure_match=False, row_count_match=False,
                    source_row_count=0, target_row_count=0,
                    errors=errors, warnings=warnings
                )
            
            if table_name not in target_tables:
                warnings.append(f"Table {table_name} not found in target database - not migrated yet")
                source_row_count = self._get_row_count(source_conn, self.source_config.schema, table_name)
                return VerificationResult(
                    table_name=table_name, structure_match=False, row_count_match=False,
                    source_row_count=source_row_count, target_row_count=0,
                    errors=[], warnings=warnings
                )
            
            # Get table structures
            source_structure = self._get_table_structure(source_conn, self.source_config.schema, table_name)
            target_structure = self._get_table_structure(target_conn, self.target_config.schema, table_name)
            
            # Compare column structures with detailed reporting
            columns_match, column_errors = self._compare_column_structures(
                source_structure['columns'], target_structure['columns'], table_name
            )
            if not columns_match:
                structure_match = False
                errors.extend(column_errors)
                
                # Log detailed column information for debugging
                self.logger.debug(f"=== Column structure details for table '{table_name}' ===")
                self.logger.debug(f"Source columns ({len(source_structure['columns'])}):")
                for i, col in enumerate(source_structure['columns']):
                    self.logger.debug(f"  {i+1}. {col}")
                self.logger.debug(f"Target columns ({len(target_structure['columns'])}):")
                for i, col in enumerate(target_structure['columns']):
                    self.logger.debug(f"  {i+1}. {col}")
            
            # Compare primary keys with detailed reporting
            if source_structure['primary_keys'] != target_structure['primary_keys']:
                structure_match = False
                source_pk = ', '.join(source_structure['primary_keys']) if source_structure['primary_keys'] else 'None'
                target_pk = ', '.join(target_structure['primary_keys']) if target_structure['primary_keys'] else 'None'
                errors.append(f"Primary keys differ - Source: [{source_pk}], Target: [{target_pk}]")
            
            # Compare indexes with detailed reporting
            indexes_match, index_errors = self._compare_indexes(
                source_structure['indexes'], target_structure['indexes']
            )
            if not indexes_match:
                # Note: Index differences are warnings, not errors, as they might be intentional
                warnings.extend([f"Index difference: {err}" for err in index_errors])
                self.logger.info(f"Index differences found in table '{table_name}' (not considered errors)")
            
            # Get row counts
            source_row_count = self._get_row_count(source_conn, self.source_config.schema, table_name)
            target_row_count = self._get_row_count(target_conn, self.target_config.schema, table_name)
            
            if source_row_count != target_row_count:
                row_count_match = False
                errors.append(f"Row count mismatch: source={source_row_count}, target={target_row_count}")
            
            return VerificationResult(
                table_name=table_name,
                structure_match=structure_match,
                row_count_match=row_count_match,
                source_row_count=source_row_count,
                target_row_count=target_row_count,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            errors.append(f"Verification failed with error: {str(e)}")
            self.logger.exception(f"Exception during verification of table '{table_name}'")
            return VerificationResult(
                table_name=table_name, structure_match=False, row_count_match=False,
                source_row_count=0, target_row_count=0,
                errors=errors, warnings=warnings
            )
    
    def verify_migration(self, tables: Optional[List[str]] = None) -> Dict[str, VerificationResult]:
        """Main verification method"""
        self.logger.info("Starting migration verification")
        
        source_conn = None
        target_conn = None
        results = {}
        
        try:
            # Establish connections
            source_conn = self._get_connection(self.source_config)
            target_conn = self._get_connection(self.target_config)
            
            # Get table list
            if not tables:
                source_tables = self._get_table_list(source_conn, self.source_config.schema)
                target_tables = self._get_table_list(target_conn, self.target_config.schema)
                tables = list(set(source_tables + target_tables))
            
            self.logger.info(f"Verifying {len(tables)} tables")
            
            # Verify each table
            for table_name in tables:
                result = self._verify_table(source_conn, target_conn, table_name)
                results[table_name] = result
                
                if result.errors:
                    self.logger.error(f"Table {table_name} verification failed: {result.errors}")
                else:
                    self.logger.info(f"Table {table_name} verification passed")
            
            # Generate summary report
            self._generate_report(results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Verification failed: {e}")
            raise
        finally:
            if source_conn:
                source_conn.close()
            if target_conn:
                target_conn.close()
    
    def _generate_report(self, results: Dict[str, VerificationResult]):
        """Generate verification report"""
        # Ensure reports directory exists
        reports_dir = "reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        total_tables = len(results)
        passed_tables = sum(1 for r in results.values() if not r.errors and not r.warnings)
        failed_tables = sum(1 for r in results.values() if r.errors)
        pending_tables = sum(1 for r in results.values() if r.warnings and not r.errors)
        
        total_source_rows = sum(r.source_row_count for r in results.values())
        total_target_rows = sum(r.target_row_count for r in results.values())
        
        report = f"""
{'='*80}
MIGRATION VERIFICATION REPORT
{'='*80}
Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY:
- Total tables verified: {total_tables}
- Passed: {passed_tables}
- Failed: {failed_tables}
- Pending migration: {pending_tables}
- Total source rows: {total_source_rows:,}
- Total target rows: {total_target_rows:,}

DETAILED RESULTS:
"""
        
        for table_name, result in results.items():
            if result.errors:
                status = "[FAIL]"
            elif result.warnings:
                status = "[PENDING]"
            else:
                status = "[PASS]"
                
            report += f"\n{'-'*60}"
            report += f"\nTable: {table_name} {status}"
            report += f"\n  Rows: {result.source_row_count:,} -> {result.target_row_count:,}"
            report += f"\n  Structure Match: {result.structure_match}"
            report += f"\n  Row Count Match: {result.row_count_match}"
            
            if result.errors:
                report += f"\n  ERRORS:"
                for error in result.errors:
                    report += f"\n    • {error}"
            
            if result.warnings:
                report += f"\n  WARNINGS:"
                for warning in result.warnings:
                    report += f"\n    • {warning}"
        
        report += f"\n\n{'='*80}"
        
        # Write report to reports directory with UTF-8 encoding
        report_filename = f"verification_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report_filepath = os.path.join(reports_dir, report_filename)
        with open(report_filepath, 'w', encoding='utf-8') as f:
            f.write(report)
        
        self.logger.info(f"Verification report saved to: {report_filepath}")
        print(report)

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Migration Verification Tool')
    parser.add_argument('--source-config', default='../env_prod', help='Source database config file')
    parser.add_argument('--target-config', default='../env_stg', help='Target database config file')
    parser.add_argument('--tables', nargs='*', help='Specific tables to verify (optional)')
    
    args = parser.parse_args()
    
    # Load configurations
    source_config = load_config_from_file(args.source_config)
    target_config = load_config_from_file(args.target_config)
    
    # Create verifier
    verifier = DatabaseVerifier(
        source_config=source_config,
        target_config=target_config
    )
    
    # Run verification
    results = verifier.verify_migration(tables=args.tables)
    
    # Exit with error code only if any verification actually failed (not just pending)
    failed_count = sum(1 for r in results.values() if r.errors)
    if failed_count > 0:
        exit(1)

if __name__ == "__main__":
    main()
