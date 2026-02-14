# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build the OSGi bundle
mvn clean package

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=CsvFormatDetectorTest

# Run a single test method
mvn test -Dtest=CsvFormatDetectorTest#detectLhvBankFormat
```

## Architecture Overview

This is a **Joget DX 8 Process Tool plugin** packaged as an OSGi bundle. It imports CSV bank/securities statement files, performs de-duplication, and batch-inserts raw transaction rows into staging tables.

### Core Pipeline

The `StatementImporter` plugin executes a 10-step pipeline:
1. Read workflow variables (recordId, accountType, bank, dates)
2. Load statement record from `bank_statement` table via FormDataDao
3. Resolve physical CSV file using FileUtil
4. Transition status: NEW → IMPORTING (via `gam-framework` StatusManager)
5. Detect CSV format from header line (`CsvFormatDetector`)
6. Parse CSV rows (`StatementParser`)
7. De-duplication check (`DeduplicationChecker`)
8. Batch-insert non-duplicates (`RawTransactionPersister`)
9. Update statement metadata (row_count, duplicate_count)
10. Transition status: IMPORTING → IMPORTED

### Package Structure

- **`parser/`** - CSV format detection and parsing
  - `Format` enum defines three formats: `LHV_BANK`, `SWEDBANK`, `SECURITIES`
  - Format detection uses header keywords (e.g., "dokumendi number" → LHV_BANK)
  - LHV Bank CSV has an empty column at index 6 that must be stripped before mapping

- **`mapping/`** - CSV column to database column mappings
  - `MappingConfigurations` contains static configs: `LHV_BANK_CONFIG`, `SWED_BANK_CONFIG`, `SECU_CONFIG`
  - Database columns use `c_` prefix (e.g., `c_payment_date`)
  - Target tables: `app_fd_bank_account_trx` (bank), `app_fd_sec_account_trx` (securities)

- **`dedup/`** - De-duplication logic
  - Bank: uses `provider_reference` field, or full-row hash if empty
  - Securities: uses `reference` field

- **`persister/`** - JDBC batch insert (bypasses FormDataDao for performance)

### Key Dependencies

- **gam-framework** - Status management (EntityType, Status, StatusManager)
- **wflow-core** - Joget platform APIs (FormDataDao, AppUtil, WorkflowManager)
- **Apache Commons CSV** - CSV parsing

### Joget Integration Notes

- Plugin registered via OSGi `Activator` class
- Uses Joget table naming: bare name in `Format.targetTable`, prefixed name (`app_fd_*`) in `MappingConfig`
- Workflow variables set via `WorkflowManager.activityVariable()`
- Status transitions use gam-framework's `StatusManager.transition()`
