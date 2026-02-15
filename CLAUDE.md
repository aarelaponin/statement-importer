# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Reference

| Task | Command |
|------|---------|
| Build | `mvn clean package` |
| Test all | `mvn test` |
| Test class | `mvn test -Dtest=CsvFormatDetectorTest` |
| Test method | `mvn test -Dtest=CsvFormatDetectorTest#detectLhvBankFormat` |

## Project Overview

This is a **Joget DX 8 Process Tool plugin bundle** that handles CSV bank/securities statement processing:

1. **StatementImporter** - Imports CSV files, detects format, performs de-duplication, and batch-inserts raw transaction rows into staging tables
2. **StatementConsolidator** - Consolidates raw rows into summary rows via GROUP BY aggregation

## Build Commands

```bash
# Build the OSGi bundle (outputs to target/statement-importer-8.1-SNAPSHOT.jar)
mvn clean package

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=CsvFormatDetectorTest

# Run a single test method
mvn test -Dtest=CsvFormatDetectorTest#detectLhvBankFormat

# Skip tests during build
mvn clean package -DskipTests

# Run tests with debug output
mvn test -X

# Generate Javadoc
mvn javadoc:javadoc
```

## Architecture Overview

### Import Pipeline (13 Steps) - StatementImporter

The `StatementImporter` plugin executes this pipeline:

1. Get record ID from workflow variable `id` (set by wf-activator)
2. Load statement record from `bank_statement` table via FormDataDao
3. Read form fields directly: account_type, bank, dates, statement_file
4. Validate required inputs
5. Resolve physical CSV file using FileUtil
6. Delete existing raw rows (idempotency)
7. Transition status: NEW → IMPORTING (via `gam-framework` StatusManager)
8. Detect CSV format from header line (`CsvFormatDetector`)
9. Parse CSV rows (`StatementParser`)
10. De-duplication check (`DeduplicationChecker`)
11. Batch-insert non-duplicates (`RawTransactionPersister`)
12. Update statement metadata (row_count, duplicate_count)
13. Transition status: IMPORTING → IMPORTED

### Consolidation Pipeline (9 Steps) - StatementConsolidator

The `StatementConsolidator` plugin runs as Activity 2 (after import):

1. Get record ID from workflow variable `id`
2. Load statement record via FormDataDao
3. Read account_type to determine bank vs securities
4. Transition status: IMPORTED → CONSOLIDATING
5. Delete existing consolidated rows (idempotency)
6. Execute GROUP BY query on raw transactions
7. Batch-insert consolidated rows with statement references (STMT{YYYY}.{SEQ})
8. Update statement metadata (total_count)
9. Transition status: CONSOLIDATING → CONSOLIDATED

**Consolidation Logic:**

| Account Type | Raw Table | Target Table | GROUP BY Columns |
|--------------|-----------|--------------|------------------|
| Bank | app_fd_bank_account_trx | app_fd_bank_total_trx | 11 columns: account_number, document_nr, payment_date, other_side_account, other_side_name, other_side_bank, d_c, payment_description, currency, customer_id, other_side_bic |
| Securities | app_fd_sec_account_trx | app_fd_secu_total_trx | 6 columns: value_date, transaction_date, type, ticker, description, currency |

**Aggregations:**
- Bank: SUM(payment_amount), SUM(transaction_fee), GROUP_CONCAT(provider_reference)
- Securities: SUM(quantity), AVG(price), SUM(amount), SUM(fee), SUM(total_amount), GROUP_CONCAT(reference)

### Package Structure

```
com.fiscaladmin.gam.statementimporter/
├── lib/
│   ├── StatementImporter.java       # Import plugin (orchestrator)
│   └── StatementConsolidator.java   # Consolidation plugin (orchestrator)
├── parser/
│   ├── Format.java                  # Enum: LHV_BANK, SWEDBANK, SECURITIES
│   ├── CsvFormatDetector.java       # Header-based format detection
│   ├── StatementParser.java         # Apache Commons CSV wrapper
│   └── UnrecognisedFormatException.java
├── mapping/
│   ├── FieldMapping.java            # CSV index -> DB column
│   ├── MappingConfig.java           # Table + mappings container
│   └── MappingConfigurations.java   # Static configs per format
├── dedup/
│   ├── DeduplicationChecker.java    # Duplicate detection
│   └── DeduplicationResult.java     # Result container
├── persister/
│   └── RawTransactionPersister.java # JDBC batch insert for raw rows
└── consolidation/
    ├── BankConsolidationQuery.java      # Bank GROUP BY + INSERT SQL
    ├── SecuConsolidationQuery.java      # Securities GROUP BY + INSERT SQL
    └── ConsolidatedRowPersister.java    # JDBC batch insert for consolidated rows
```

### CSV Format Detection

| Format | Separator | Detection Keyword | Target Table |
|--------|-----------|-------------------|--------------|
| LHV_BANK | `,` | "dokumendi number" | bank_account_trx |
| SWEDBANK | `;` | "dok nr" | bank_account_trx |
| SECURITIES | `,` | "väärtuspäev" AND "tehingupäev" | sec_account_trx |

### De-duplication Strategy

| Account Type | Primary Key | Fallback Composite Key |
|--------------|-------------|------------------------|
| Bank | provider_reference (idx 17) | account + doc_nr + date + amount + currency |
| Securities | reference (idx 11) | value_date + trx_date + type + ticker + amount + currency |

### Database Tables

**Statement:**
- `app_fd_bank_statement` - Statement metadata and status

**Raw Transactions (from import):**
- `app_fd_bank_account_trx` - Bank transaction rows (18 mapped fields)
- `app_fd_sec_account_trx` - Securities transaction rows (13 mapped fields)

**Consolidated Transactions (from consolidation):**
- `app_fd_bank_total_trx` - Consolidated bank transactions
- `app_fd_secu_total_trx` - Consolidated securities transactions

## Key Dependencies

| Dependency | Purpose |
|------------|---------|
| gam-framework | Status management (EntityType, Status, StatusManager) |
| wflow-core | Joget platform APIs (FormDataDao, AppUtil, WorkflowManager) |
| commons-csv 1.10.0 | CSV parsing |
| junit 4.13.2 | Unit testing |
| mockito 4.11.0 | Mocking |
| h2 2.2.224 | In-memory DB for tests |

**Build Order:** gam-framework must be installed first:
```bash
cd ../gam-framework && mvn clean install
cd ../statement-importer && mvn clean package
```

## Joget Integration Notes

### Plugin Registration

- Plugins registered via OSGi `Activator` class
- Both implement `DefaultApplicationPlugin` (Process Tool)
- Registered services:
  - `StatementImporter.class.getName()`
  - `StatementConsolidator.class.getName()`

### Table Naming Convention

- Joget tables use `app_fd_` prefix in MySQL
- `Format.targetTable` stores bare name (e.g., `bank_account_trx`)
- `MappingConfig.targetTable` stores full name (e.g., `app_fd_bank_account_trx`)

### Workflow Integration

**wf-activator Integration:**
- Plugin is triggered via `wf-activator` as form Post-Processing Tool
- Process name must be: `gam_statement_submission` (convention: `{serviceId}_submission`)
- Service ID in wf-activator: `gam_statement`

**Workflow Variable Requirements:**
- Only ONE workflow variable required: `id` (String)
- The `id` variable must be pre-defined in the process (Joget requirement)
- Plugin reads all other data directly from the form record

**How it works:**
1. wf-activator passes form `id` as workflow variable
2. Plugin gets `id` via `WorkflowManager.getProcessVariable()`
3. Plugin loads form record using that ID
4. Plugin reads `account_type`, `bank`, `from_date`, `to_date`, `statement_file` from form

### Status Management

- Uses gam-framework's `StatusManager.transition()`
- Entity type: `EntityType.STATEMENT`
- Status flow: `NEW` → `IMPORTING` → `IMPORTED` → `CONSOLIDATING` → `CONSOLIDATED`
- Error status: `ERROR` (can occur at any stage)

## Testing Guidelines

### Test Structure

| Package | Test Class | Coverage |
|---------|------------|----------|
| parser | CsvFormatDetectorTest | Format detection |
| parser | StatementParserTest | CSV parsing |
| mapping | MappingConfigurationsTest | Config retrieval |
| dedup | DeduplicationCheckerTest | Duplicate detection |
| persister | RawTransactionPersisterTest | Batch insert (H2) |
| persister | RealCsvEndToEndTest | End-to-end with real CSVs (13 tests) |
| lib | StatementImporterTest | Plugin execution |
| consolidation | BankConsolidationTest | Bank GROUP BY + aggregation |
| consolidation | SecuConsolidationTest | Securities GROUP BY + aggregation |
| consolidation | ConsolidatedRowPersisterTest | Consolidated row batch insert |
| consolidation | ConsolidationEndToEndTest | Full consolidation pipeline |

### RealCsvEndToEndTest Coverage

The end-to-end test suite validates both core pipeline and edge cases:

**Core Pipeline Tests (5):**
- `bankCsvFullPipeline` — Full bank CSV: detect → parse → persist → verify 161 rows
- `secuCsvFullPipeline` — Full securities CSV: detect → parse → persist → verify 45 rows
- `bankInsertColumnsMatchFormSchema` — INSERT columns match Joget form schema
- `secuInsertColumnsMatchFormSchema` — INSERT columns match Joget form schema
- `bankAndSecuDoNotInterfere` — Bank and securities imports are isolated

**Edge Case Tests (8):**
- `secuMandatoryFieldsNotNull` — No NULL mandatory securities fields
- `secuStockSplitTransactionsHandled` — Stock split+/split- pairs with shared reference
- `bankNegativeAmountsPreserved` — Negative amounts correlate with Debit (D)
- `bankMultiCurrencySegregation` — EUR/USD segregation (totals 161)
- `bankEmptyFieldsNotNull` — Empty fields are empty strings, not NULL
- `secuZeroFeeTransactions` — Zero fee transactions (LHV1T buys)
- `bankCurrencyExchangeTransactions` — FX pairs: EUR debit + USD credit
- `secuHighPrecisionPrices` — 8-decimal price precision preserved

### Writing Tests

```java
// Pattern for testing with H2 in-memory database
@Test
public void testBatchInsert() throws SQLException {
    try (Connection con = DriverManager.getConnection("jdbc:h2:mem:test")) {
        // Create table
        con.createStatement().execute("CREATE TABLE app_fd_bank_account_trx (...)");

        // Test
        int count = RawTransactionPersister.persist(rows, "stmt1", config, con);

        // Verify
        assertEquals(expectedCount, count);
    }
}
```

### Mocking Joget APIs

```java
// Pattern for mocking FormDataDao
FormDataDao mockDao = mock(FormDataDao.class);
FormRow mockRow = new FormRow();
mockRow.setProperty("statement_file", "test.csv");
when(mockDao.load(anyString(), anyString(), anyString())).thenReturn(mockRow);
```

## Debugging Tips

### Enable Debug Logging

Add to Joget's `log4j.properties`:
```
log4j.logger.com.fiscaladmin.gam.statementimporter=DEBUG
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Statement record not found: null" | `id` variable not defined in process | Add `id` workflow variable to process definition |
| "Statement record not found: 12345" | Wrong ID (process number, not record ID) | Ensure `id` variable defined and process redeployed |
| "Unknown column 'c_error_message'" | Missing field in form | Add `error_message` field to statement form |
| "context attribute X does not exist" | Workflow variable not pre-defined | Define required variables in process before deploying |
| UnrecognisedFormatException | Header doesn't match patterns | Check CSV header row unchanged |
| Format mismatch | Wrong account_type | Verify account_type matches CSV format |
| File not found | FileUtil path issue | Check file upload in Joget |
| SQL error | Missing table/column | Verify form field IDs match MappingConfig |
| Consolidated table empty | Wrong target table name | Verify table names: bank_total_trx, secu_total_trx |
| MySQL reserved keyword error | Column alias uses reserved word | Escape with backticks (e.g., \`references\`) |
| Fewer consolidated rows than expected | GROUP BY too aggressive | Check GROUP BY columns match original design |

### Log Prefixes

- `StatementImporter` - Import orchestration
- `StatementConsolidator` - Consolidation orchestration
- `CsvFormatDetector` - Format detection
- `DeduplicationChecker` - Dedup results
- `RawTransactionPersister` - Raw row batch insert
- `ConsolidatedRowPersister` - Consolidated row batch insert

## Code Conventions

### Field Naming

- CSV indices are **0-based**
- DB columns use `c_` prefix (Joget convention)
- LHV Bank: indices are **after stripping** empty field 6

### Error Handling

- Validation errors: `IllegalArgumentException`
- State errors: `IllegalStateException`
- Format errors: `UnrecognisedFormatException`
- DB errors: `RuntimeException` wrapping `SQLException`

### Null Safety

- CSV fields: never null (empty string if missing)
- DB columns: nullable, use IFNULL in queries

## Adding a New CSV Format

1. Add enum value to `Format`:
   ```java
   NEW_FORMAT("bank", ',', "bank_account_trx");
   ```

2. Add detection pattern in `CsvFormatDetector.detect()`:
   ```java
   if (normalised.contains("new format keyword")) {
       return Format.NEW_FORMAT;
   }
   ```

3. Add mapping config to `MappingConfigurations`:
   ```java
   public static final MappingConfig NEW_FORMAT_CONFIG = new MappingConfig(
       "app_fd_bank_account_trx",
       Arrays.asList(new FieldMapping(0, "c_column_name"), ...)
   );
   ```

4. Update `getConfig()` switch statement

5. Add unit tests

## Performance Notes

- Batch insert: all rows in single batch (OK for <500 rows)
- Dedup queries: indexed on statement_id and provider_reference
- Memory: loads all CSV rows into memory
- Connections: single connection per import, uses pool

## Related Documentation

- [README.md](README.md) - Project overview and setup
- [docs/DEPLOYMENT_GUIDE.md](docs/DEPLOYMENT_GUIDE.md) - **Complete deployment instructions with wf-activator**
- [docs/TECHNICAL.md](docs/TECHNICAL.md) - Detailed API documentation
- [docs/USER_GUIDE.md](docs/USER_GUIDE.md) - End-user instructions

## Deployment Quick Reference

| Item | Value |
|------|-------|
| Plugin JAR | `target/statement-importer-8.1-SNAPSHOT.jar` |
| Required plugin | wf-activator |
| Process name | `gam_statement_submission` |
| Service ID | `gam_statement` |
| Required workflow variable | `id` (String) - must be pre-defined in process |
| Required form field | `error_message` (Text Area) |
| Plugins in bundle | StatementImporter, StatementConsolidator |
| Activity 1 | StatementImporter (NEW → IMPORTED) |
| Activity 2 | StatementConsolidator (IMPORTED → CONSOLIDATED) |
