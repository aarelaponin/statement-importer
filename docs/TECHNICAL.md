# Technical API Documentation

This document provides detailed technical documentation for the Statement Importer plugin, including class-level APIs, data flow diagrams, and integration patterns.

## Table of Contents

- [Class Overview](#class-overview)
- [API Reference](#api-reference)
- [Data Flow](#data-flow)
- [Database Schema](#database-schema)
- [Integration Points](#integration-points)
- [Extension Guide](#extension-guide)
- [Performance Considerations](#performance-considerations)

---

## Class Overview

### Architecture Diagram

```
                     ┌─────────────────────────────────────┐
                     │        StatementImporter            │
                     │     (DefaultApplicationPlugin)      │
                     │                                     │
                     │  Orchestrates the 13-step pipeline  │
                     └─────────────────┬───────────────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────┐
          │                            │                            │
          ▼                            ▼                            ▼
┌─────────────────┐         ┌─────────────────────┐      ┌─────────────────────┐
│ parser package  │         │   dedup package     │      │  persister package  │
├─────────────────┤         ├─────────────────────┤      ├─────────────────────┤
│ CsvFormatDetector│        │ DeduplicationChecker│      │RawTransactionPersister│
│ StatementParser │         │ DeduplicationResult │      └─────────────────────┘
│ Format (enum)   │         └─────────────────────┘
│ UnrecognisedFmt │
└─────────────────┘
          │
          ▼
┌─────────────────┐
│ mapping package │
├─────────────────┤
│ FieldMapping    │
│ MappingConfig   │
│ MappingConfigs  │
└─────────────────┘
```

### Class Responsibilities

| Class | Responsibility | Design Pattern |
|-------|----------------|----------------|
| `StatementImporter` | Main orchestrator, Joget plugin entry point | Template Method |
| `CsvFormatDetector` | Header-based format detection | Strategy (implicit) |
| `StatementParser` | CSV parsing with Apache Commons CSV | Adapter |
| `Format` | Format enumeration with metadata | Type-Safe Enum |
| `MappingConfigurations` | Static mapping config factory | Factory |
| `MappingConfig` | Column mapping container | Value Object |
| `FieldMapping` | Single column mapping | Value Object |
| `DeduplicationChecker` | Duplicate detection logic | Strategy |
| `DeduplicationResult` | Dedup operation result | Value Object |
| `RawTransactionPersister` | JDBC batch insert | Repository |

---

## API Reference

### StatementImporter

**Package**: `com.fiscaladmin.gam.statementimporter.lib`

**Extends**: `org.joget.plugin.base.DefaultApplicationPlugin`

The main plugin class that orchestrates the entire import process.

#### Plugin Metadata

```java
getName()        // "Statement Importer"
getDescription() // "Parses uploaded CSV statement files..."
getVersion()     // "8.1-SNAPSHOT"
getLabel()       // "Statement Importer"
getClassName()   // "com.fiscaladmin.gam.statementimporter.lib.StatementImporter"
```

#### execute(Map properties)

Main entry point invoked by the Joget workflow engine.

**Parameters**:
- `properties` - Map containing workflow variables and `workflowAssignment`

**Expected Properties**:
| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `recordId` | String | Yes | Statement record ID |
| `account_type` | String | Yes | "bank" or "secu" |
| `bank` | String | No | Bank identifier |
| `from_date` | String | Yes | yyyy-MM-dd format |
| `to_date` | String | Yes | yyyy-MM-dd format |
| `workflowAssignment` | WorkflowAssignment | Auto | Injected by Joget |

**Returns**: `null` (side effects only)

**Side Effects**:
- Inserts rows into `app_fd_bank_account_trx` or `app_fd_sec_account_trx`
- Updates `app_fd_bank_statement` metadata
- Sets workflow variable `statementStatus`

---

### CsvFormatDetector

**Package**: `com.fiscaladmin.gam.statementimporter.parser`

Utility class for detecting CSV format from the header line.

#### detect(File file)

```java
public static Format detect(File file)
    throws IOException, UnrecognisedFormatException
```

**Parameters**:
- `file` - CSV file to inspect

**Returns**: `Format` enum value (`LHV_BANK`, `SWEDBANK`, or `SECURITIES`)

**Throws**:
- `IOException` - If file cannot be read
- `UnrecognisedFormatException` - If header doesn't match any known format

**Detection Logic**:
1. Read first line of file
2. Strip UTF-8 BOM if present
3. Normalize: remove quotes, lowercase
4. Match patterns in order:
   - Contains "väärtuspäev" AND "tehingupäev" -> `SECURITIES`
   - Contains "dokumendi number" -> `LHV_BANK`
   - Contains "dok nr" -> `SWEDBANK`

---

### StatementParser

**Package**: `com.fiscaladmin.gam.statementimporter.parser`

Parses CSV files using Apache Commons CSV.

#### parse(File file, Format format)

```java
public static List<String[]> parse(File file, Format format)
    throws IOException
```

**Parameters**:
- `file` - CSV file to parse
- `format` - Detected format (determines separator and post-processing)

**Returns**: `List<String[]>` - Each array is one data row (header excluded)

**Special Handling**:
- UTF-8 BOM is stripped automatically
- For `LHV_BANK`: Empty column at index 6 is removed from each row
- Returns trimmed, non-null field values (empty string if null)

---

### Format

**Package**: `com.fiscaladmin.gam.statementimporter.parser`

Enum defining supported CSV formats.

```java
public enum Format {
    LHV_BANK("bank", ',', "bank_account_trx"),
    SWEDBANK("bank", ';', "bank_account_trx"),
    SECURITIES("secu", ',', "sec_account_trx");
}
```

#### Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `getAccountType()` | `String` | "bank" or "secu" |
| `getSeparator()` | `char` | CSV delimiter character |
| `getTargetTable()` | `String` | Bare table name (without `app_fd_` prefix) |

---

### MappingConfigurations

**Package**: `com.fiscaladmin.gam.statementimporter.mapping`

Factory for format-specific mapping configurations.

#### Static Constants

| Constant | Table | Fields |
|----------|-------|--------|
| `SECU_CONFIG` | `app_fd_sec_account_trx` | 13 fields |
| `LHV_BANK_CONFIG` | `app_fd_bank_account_trx` | 18 fields (after stripping) |
| `SWED_BANK_CONFIG` | `app_fd_bank_account_trx` | 14 fields |

#### getConfig(Format format)

```java
public static MappingConfig getConfig(Format format)
```

**Parameters**:
- `format` - The detected CSV format

**Returns**: `MappingConfig` for the format

**Throws**: `IllegalArgumentException` if no config exists

---

### MappingConfig

**Package**: `com.fiscaladmin.gam.statementimporter.mapping`

Container for a complete column mapping configuration.

#### Constructor

```java
public MappingConfig(String targetTable, List<FieldMapping> fieldMappings)
```

**Parameters**:
- `targetTable` - Full MySQL table name WITH `app_fd_` prefix
- `fieldMappings` - Ordered list of field mappings

#### Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `getTargetTable()` | `String` | e.g., `app_fd_bank_account_trx` |
| `getFieldMappings()` | `List<FieldMapping>` | Ordered CSV -> DB mappings |

---

### FieldMapping

**Package**: `com.fiscaladmin.gam.statementimporter.mapping`

Immutable mapping from CSV column index to database column.

#### Constructor

```java
public FieldMapping(int csvIndex, String dbColumnName)
```

**Parameters**:
- `csvIndex` - Zero-based index in the parsed CSV row
- `dbColumnName` - SQL column name WITH `c_` prefix

#### Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `getCsvIndex()` | `int` | 0-based CSV column index |
| `getDbColumnName()` | `String` | e.g., `c_payment_date` |

---

### DeduplicationChecker

**Package**: `com.fiscaladmin.gam.statementimporter.dedup`

Detects duplicate transactions across overlapping statement uploads.

#### check(List<String[]> rows, String statementId, String fromDate, String toDate, String accountType)

```java
public static DeduplicationResult check(
    List<String[]> rows,
    String statementId,
    String fromDate,
    String toDate,
    String accountType)
```

**Parameters**:
- `rows` - Parsed CSV rows to check
- `statementId` - Current statement ID (excluded from overlap search)
- `fromDate` - Statement period start (yyyy-MM-dd)
- `toDate` - Statement period end (yyyy-MM-dd)
- `accountType` - "bank" or "secu"

**Returns**: `DeduplicationResult` with non-duplicate rows and counts

#### De-duplication Key Strategy

| Account Type | Primary Key Index | Primary Key Field | Composite Key Indices | Composite Key Fields |
|--------------|-------------------|-------------------|----------------------|---------------------|
| Bank | 17 | `provider_reference` | 0, 1, 2, 7, 12 | account_number, document_nr, payment_date, payment_amount, currency |
| Securities | 11 | `reference` | 0, 1, 2, 3, 8, 7 | value_date, transaction_date, type, ticker, amount, currency |

---

### DeduplicationResult

**Package**: `com.fiscaladmin.gam.statementimporter.dedup`

Immutable result container for de-duplication operations.

#### Constructor

```java
public DeduplicationResult(
    List<String[]> nonDuplicateRows,
    int duplicateCount,
    int totalCount)
```

#### Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `getNonDuplicateRows()` | `List<String[]>` | Rows to insert |
| `getDuplicateCount()` | `int` | Number of duplicates found |
| `getTotalCount()` | `int` | Total rows checked |

---

### RawTransactionPersister

**Package**: `com.fiscaladmin.gam.statementimporter.persister`

Performs JDBC batch inserts into raw transaction tables.

#### persist(List<String[]> rows, String statementId, MappingConfig config)

```java
public static int persist(
    List<String[]> rows,
    String statementId,
    MappingConfig config)
```

**Parameters**:
- `rows` - Parsed CSV rows to insert
- `statementId` - Parent statement ID (set on all rows)
- `config` - Mapping configuration for the format

**Returns**: Number of rows inserted

**Throws**: `RuntimeException` wrapping `SQLException` on failure

**Generated Fields**:
| Column | Value |
|--------|-------|
| `id` | Random UUID |
| `c_transaction_id` | Zero-padded sequence (001, 002, ...) |
| `c_statement_id` | Input `statementId` |
| `dateCreated` | Current timestamp |
| `createdBy` | "system" |

---

## Data Flow

### Sequence Diagram

```
┌──────────┐  ┌──────────────┐  ┌───────────────┐  ┌───────────────┐
│  Joget   │  │StatementImporter│ │CsvFormatDetect│  │StatementParser│
└────┬─────┘  └──────┬────────┘  └──────┬────────┘  └──────┬────────┘
     │               │                   │                  │
     │ execute()     │                   │                  │
     │──────────────>│                   │                  │
     │               │                   │                  │
     │               │ detect(file)      │                  │
     │               │──────────────────>│                  │
     │               │                   │                  │
     │               │    Format         │                  │
     │               │<──────────────────│                  │
     │               │                   │                  │
     │               │ parse(file, fmt)                     │
     │               │─────────────────────────────────────>│
     │               │                                      │
     │               │         List<String[]>               │
     │               │<─────────────────────────────────────│
     │               │                   │                  │
```

```
┌──────────────┐  ┌───────────────────┐  ┌────────────────────┐  ┌──────────────┐
│StatementImporter│ │DeduplicationChecker│  │RawTransactionPersister│ │   MySQL    │
└──────┬────────┘  └──────────┬────────┘  └──────────┬─────────┘  └──────┬───────┘
       │                      │                       │                   │
       │ check(rows,...)      │                       │                   │
       │─────────────────────>│                       │                   │
       │                      │                       │                   │
       │                      │ SELECT overlapping    │                   │
       │                      │──────────────────────────────────────────>│
       │                      │                       │                   │
       │                      │    existing keys      │                   │
       │                      │<──────────────────────────────────────────│
       │                      │                       │                   │
       │ DeduplicationResult  │                       │                   │
       │<─────────────────────│                       │                   │
       │                      │                       │                   │
       │ persist(newRows,...) │                       │                   │
       │─────────────────────────────────────────────>│                   │
       │                      │                       │                   │
       │                      │                       │ INSERT BATCH      │
       │                      │                       │──────────────────>│
       │                      │                       │                   │
       │         rowCount     │                       │                   │
       │<─────────────────────────────────────────────│                   │
       │                      │                       │                   │
```

---

## Database Schema

### Statement Table

```sql
CREATE TABLE app_fd_bank_statement (
    id VARCHAR(255) PRIMARY KEY,
    c_account_type VARCHAR(50),           -- "bank" or "secu"
    c_bank VARCHAR(100),
    c_from_date DATE,
    c_to_date DATE,
    c_statement_file VARCHAR(500),
    c_status VARCHAR(50),                 -- new, importing, imported, error
    c_row_count INT,
    c_duplicate_count INT,
    c_processing_timestamp DATETIME,
    c_error_message TEXT,
    dateCreated DATETIME,
    dateModified DATETIME,
    createdBy VARCHAR(255),
    modifiedBy VARCHAR(255)
);
```

### Bank Transaction Table

```sql
CREATE TABLE app_fd_bank_account_trx (
    id VARCHAR(255) PRIMARY KEY,
    c_statement_id VARCHAR(255),          -- FK to bank_statement
    c_transaction_id VARCHAR(10),         -- "001", "002", etc.
    c_account_number VARCHAR(50),
    c_document_nr VARCHAR(50),
    c_payment_date DATE,
    c_other_side_account VARCHAR(50),
    c_other_side_name VARCHAR(255),
    c_other_side_bank VARCHAR(100),
    c_d_c VARCHAR(10),                    -- Debit/Credit
    c_payment_amount DECIMAL(15,2),
    c_reference_number VARCHAR(100),
    c_archival_number VARCHAR(100),
    c_payment_description TEXT,
    c_transaction_fee DECIMAL(10,2),
    c_currency VARCHAR(10),
    c_customer_id VARCHAR(50),
    c_other_side_bic VARCHAR(20),
    c_initiator VARCHAR(255),
    c_transaction_reference VARCHAR(100),
    c_provider_reference VARCHAR(100),
    dateCreated DATETIME,
    createdBy VARCHAR(255)
);

CREATE INDEX idx_bank_trx_statement ON app_fd_bank_account_trx(c_statement_id);
CREATE INDEX idx_bank_trx_provider_ref ON app_fd_bank_account_trx(c_provider_reference);
```

### Securities Transaction Table

```sql
CREATE TABLE app_fd_sec_account_trx (
    id VARCHAR(255) PRIMARY KEY,
    c_statement_id VARCHAR(255),
    c_transaction_id VARCHAR(10),
    c_value_date DATE,
    c_transaction_date DATE,
    c_type VARCHAR(50),
    c_ticker VARCHAR(20),
    c_description TEXT,
    c_quantity DECIMAL(15,4),
    c_price DECIMAL(15,4),
    c_currency VARCHAR(10),
    c_amount DECIMAL(15,2),
    c_fee DECIMAL(10,2),
    c_total_amount DECIMAL(15,2),
    c_reference VARCHAR(100),
    c_comment TEXT,
    dateCreated DATETIME,
    createdBy VARCHAR(255)
);

CREATE INDEX idx_sec_trx_statement ON app_fd_sec_account_trx(c_statement_id);
CREATE INDEX idx_sec_trx_reference ON app_fd_sec_account_trx(c_reference);
```

---

## Integration Points

### Joget Platform APIs

| API | Class | Usage |
|-----|-------|-------|
| Plugin Registration | `DefaultApplicationPlugin` | Base class for process tools |
| Form Data Access | `FormDataDao` | Load/save statement records |
| File Access | `FileUtil` | Resolve uploaded file paths |
| Workflow Variables | `WorkflowManager` | Set output variables |
| Data Source | `AppUtil.getBean("setupDataSource")` | JDBC connection |
| Logging | `LogUtil` | Info/warn/error logging |

### GAM Framework Integration

| Component | Usage |
|-----------|-------|
| `StatusManager` | State transitions (NEW -> IMPORTING -> IMPORTED) |
| `EntityType.STATEMENT` | Entity type constant |
| `Status` enum | Status constants |

### OSGi Integration

The plugin is packaged as an OSGi bundle and registered via `Activator`:

```java
public class Activator implements BundleActivator {
    public void start(BundleContext context) {
        context.registerService(
            StatementImporter.class.getName(),
            new StatementImporter(),
            null
        );
    }
}
```

---

## Extension Guide

### Adding a New CSV Format

1. **Add enum value to `Format`**:
```java
NEW_BANK("bank", ';', "bank_account_trx");
```

2. **Add detection pattern to `CsvFormatDetector.detect()`**:
```java
if (normalised.contains("new bank header keyword")) {
    return Format.NEW_BANK;
}
```

3. **Add mapping config to `MappingConfigurations`**:
```java
public static final MappingConfig NEW_BANK_CONFIG = new MappingConfig(
    "app_fd_bank_account_trx",
    Arrays.asList(
        new FieldMapping(0, "c_account_number"),
        // ... more mappings
    )
);
```

4. **Update `getConfig()` switch statement**:
```java
case NEW_BANK: return NEW_BANK_CONFIG;
```

5. **Add unit tests for the new format**

### Customizing De-duplication

To modify de-duplication logic for a format:

1. Edit `DeduplicationChecker` constants:
```java
static final int NEW_PRIMARY_KEY_INDEX = 5;
static final int[] NEW_COMPOSITE_INDICES = {0, 1, 2};
```

2. Update the `check()` method's conditional logic

---

## Performance Considerations

### Batch Insert Sizing

- Current implementation: Single batch for all rows
- Typical statement size: ~150 rows (bank), ~45 rows (securities)
- For very large files (>1000 rows), consider chunking

### De-duplication Query Optimization

The overlap query uses indexed columns:
- `c_account_type` - Indexed
- `c_from_date`, `c_to_date` - Should be indexed
- `c_status` - Status filtering

### Memory Usage

- CSV parsing loads all rows into memory
- For large files (>10MB), consider streaming approach

### Connection Management

- Uses try-with-resources for connection cleanup
- DataSource from Joget connection pool
- Single connection per import operation

---

## Error Handling

### Error Categories

| Category | Handling |
|----------|----------|
| Validation errors | Throw `IllegalArgumentException`, set status ERROR |
| File not found | Throw `IllegalStateException`, set status ERROR |
| Format detection | Throw `UnrecognisedFormatException`, set status ERROR |
| Database errors | Wrap in `RuntimeException`, set status ERROR |

### Recovery Strategy

1. Delete existing raw rows before import (idempotency)
2. Catch all exceptions in `execute()`
3. Attempt status transition to ERROR
4. Fall back to direct SQL if StatusManager fails
5. Set workflow variable for error routing

---

*Last updated: 2024*
