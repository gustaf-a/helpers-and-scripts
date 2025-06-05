# PostgreSQL Database Migration Tool

A robust Python tool for migrating data between Azure Database for PostgreSQL flexible servers with built-in progress tracking, error handling, and verification capabilities.

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install psycopg2-binary configparser
   ```

2. **Run migration:**
   ```bash
   python migration_script.py
   ```

3. **Verify migration:**
   ```bash
   python migration_verification_script.py
   ```

## Configuration

Create configuration files for your databases:

**Source (env_prod):**
```ini
DatabaseConfig_host=your-prod-server.postgres.database.azure.com
DatabaseConfig_port=5432
DatabaseConfig_user=your_username
DatabaseConfig_password=your_password
DatabaseConfig_database=your_database
DatabaseConfig_schema=public
```

**Target (env_stg):**
```ini
DatabaseConfig_host=your-staging-server.postgres.database.azure.com
DatabaseConfig_port=5432
DatabaseConfig_user=your_username
DatabaseConfig_password=your_password
DatabaseConfig_database=your_database
DatabaseConfig_schema=public
```

## Migration Examples

**Basic migration (all tables):**
```bash
python migration_script.py
```

**Migrate specific tables:**
```bash
python migration_script.py --tables users orders products
```

**Custom configurations:**
```bash
python migration_script.py --source-config /path/to/prod --target-config /path/to/staging
```

**Adjust performance:**
```bash
# For slower networks or large tables
python migration_script.py --chunk-size 500

# For faster networks
python migration_script.py --chunk-size 2000
```

## Verification Examples

**Verify all migrated tables:**
```bash
python migration_verification_script.py
```

**Verify specific tables:**
```bash
python migration_verification_script.py --tables users orders
```

**Custom configurations:**
```bash
python migration_verification_script.py --source-config /path/to/prod --target-config /path/to/staging
```

## Key Features

- ✅ **Chunked Processing** - Handles large tables without memory issues
- ✅ **Progress Tracking** - Resume from where you left off
- ✅ **Error Resilience** - Automatic retries and graceful error handling
- ✅ **Schema Management** - Automatically creates tables and sequences
- ✅ **Verification** - Comprehensive post-migration validation
- ✅ **Detailed Logging** - Track progress and troubleshoot issues

## Options

### Migration Script
| Option | Description | Default |
|--------|-------------|---------|
| `--source-config` | Source database config file | `../env_prod` |
| `--target-config` | Target database config file | `../env_stg` |
| `--chunk-size` | Rows per batch | `1000` |
| `--tables` | Specific tables to migrate | All tables |

### Verification Script
| Option | Description | Default |
|--------|-------------|---------|
| `--source-config` | Source database config file | `../env_prod` |
| `--target-config` | Target database config file | `../env_stg` |
| `--tables` | Specific tables to verify | All tables |

## What Gets Migrated

1. **Table structures** - Columns, data types, constraints
2. **Sequences** - Auto-increment sequences with correct values
3. **Primary keys** - Table primary key constraints
4. **Data** - All table data in configurable chunks
5. **Indexes** - Table indexes (best effort)

## Output Files

- **Logs:** `migration_YYYYMMDD_HHMMSS.log` and `verification_YYYYMMDD_HHMMSS.log`
- **Progress:** `migration_progress.json` (for resumption)
- **Reports:** `verification_report_YYYYMMDD_HHMMSS.txt`

## Troubleshooting

**Connection issues:**
```bash
# Test with smaller chunks
python migration_script.py --chunk-size 100
```

**Large table issues:**
```bash
# Migrate one table at a time
python migration_script.py --tables large_table --chunk-size 200
```

**Resume after interruption:**
```bash
# Automatically resumes from last checkpoint
python migration_script.py
```

**Check verification results:**
```bash
# Look for FAIL status in verification report
cat verification_report_*.txt
```

## Performance Tips

- **Slow networks:** Use `--chunk-size 200-500`
- **Fast networks:** Use `--chunk-size 1000-2000`
- **Large tables:** Migrate separately with smaller chunks
- **Run during off-peak hours** for better performance

## Architecture

The tool consists of modular components:
- `migration_script.py` - Main migration orchestrator
- `migration_verification_script.py` - Post-migration verification
- `database_config.py` - Configuration handling
- `database_connection.py` - Connection management
- `schema_manager.py` - Schema and sequence management
- `data_migrator.py` - Core data transfer logic
- `progress_tracker.py` - Progress tracking and resumption

Each component handles specific responsibilities for better maintainability and testing.
