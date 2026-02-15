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

This is a **Joget DX 8 Process Tool plugin** that imports CSV bank/securities statement files. It detects format, performs de-duplication, and batch-inserts raw transaction rows into staging tables.

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

### Core Pipeline (13 Steps)

The `StatementImporter` plugin executes this pipeline:

1. Read workflow variables (recordId, accountType, bank, dates)
2. Validate required inputs
3. Load statement record from `bank_statement` table via FormDataDao
4. Resolve physical CSV file using FileUtil
5. Delete existing raw rows (idempotency)
6. Transition status: NEW -> IMPORTING (via `gam-framework` StatusManager)
7. Detect CSV format from header line (`CsvFormatDetector`)
8. Parse CSV rows (`StatementParser`)
9. De-duplication check (`DeduplicationChecker`)
10. Batch-insert non-duplicates (`RawTransactionPersister`)
11. Update statement metadata (row_count, duplicate_count)
12. Transition status: IMPORTING -> IMPORTED
13. Set workflow variable statementStatus = "imported"

### Package Structure

```
com.fiscaladmin.gam.statementimporter/
├── lib/
│   └── StatementImporter.java    # Main plugin (orchestrator)
├── parser/
│   ├── Format.java               # Enum: LHV_BANK, SWEDBANK, SECURITIES
│   ├── CsvFormatDetector.java    # Header-based format detection
│   ├── StatementParser.java      # Apache Commons CSV wrapper
│   └── UnrecognisedFormatException.java
├── mapping/
│   ├── FieldMapping.java         # CSV index -> DB column
│   ├── MappingConfig.java        # Table + mappings container
│   └── MappingConfigurations.java # Static configs per format
├── dedup/
│   ├── DeduplicationChecker.java # Duplicate detection
│   └── DeduplicationResult.java  # Result container
└── persister/
    └── RawTransactionPersister.java # JDBC batch insert
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

- `app_fd_bank_statement` - Statement metadata and status
- `app_fd_bank_account_trx` - Bank transaction rows (18 mapped fields)
- `app_fd_sec_account_trx` - Securities transaction rows (13 mapped fields)

## Key Dependencies

| Dependency | Purpose |
|------------|---------|
| gam-framework | Status management (EntityType, Status, StatusManager) |
| wflow-core | Joget platform APIs (FormDataDao, AppUtil, WorkflowManager) |
| commons-csv 1.10.0 | CSV parsing |
| junit 4.13.2 | Unit testing |
| mockito 4.11.0 | Mocking |
| h2 2.2.224 | In-memory DB for tests |

## Joget Integration Notes

### Plugin Registration

- Plugin registered via OSGi `Activator` class
- Implements `DefaultApplicationPlugin` (Process Tool)
- Registered service: `StatementImporter.class.getName()`

### Table Naming Convention

- Joget tables use `app_fd_` prefix in MySQL
- `Format.targetTable` stores bare name (e.g., `bank_account_trx`)
- `MappingConfig.targetTable` stores full name (e.g., `app_fd_bank_account_trx`)

### Workflow Integration

- Workflow variables read from `properties` map
- Output variable set via `WorkflowManager.activityVariable()`
- Assignment obtained from `properties.get("workflowAssignment")`

### Status Management

- Uses gam-framework's `StatusManager.transition()`
- Entity type: `EntityType.STATEMENT`
- Status values: `NEW`, `IMPORTING`, `IMPORTED`, `ERROR`

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
| UnrecognisedFormatException | Header doesn't match patterns | Check CSV header row unchanged |
| Format mismatch | Wrong account_type | Verify workflow variable |
| File not found | FileUtil path issue | Check file upload in Joget |
| SQL error | Missing table/column | Verify table structure |

### Log Prefixes

- `StatementImporter` - Main orchestration
- `CsvFormatDetector` - Format detection
- `DeduplicationChecker` - Dedup results
- `RawTransactionPersister` - Batch insert

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
- [docs/TECHNICAL.md](docs/TECHNICAL.md) - Detailed API documentation
- [docs/USER_GUIDE.md](docs/USER_GUIDE.md) - End-user instructions
