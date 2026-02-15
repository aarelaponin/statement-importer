# Statement Importer Plugin

A **Joget DX 8 Process Tool plugin bundle** that handles CSV bank and securities statement processing:

1. **StatementImporter** - Imports CSV files, performs intelligent de-duplication, and batch-inserts raw transaction rows into staging tables
2. **StatementConsolidator** - Consolidates raw transaction rows into summary rows via GROUP BY aggregation

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Supported CSV Formats](#supported-csv-formats)
- [Usage](#usage)
- [Architecture](#architecture)
- [Development](#development)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Overview

The Statement Importer plugin automates the import of bank and securities account statements from CSV files. It is designed to be used as a Tool in BPMN workflows within the Joget DX platform, specifically in the `gam_statement_submission` process.

### Key Capabilities

- **Automatic Format Detection**: Identifies LHV Bank, Swedbank, and Securities statement formats from CSV headers
- **Intelligent De-duplication**: Prevents duplicate transaction entries across overlapping statement uploads
- **High-Performance Batch Insert**: Uses JDBC batch operations for efficient bulk data loading
- **Status Management**: Integrates with gam-framework for state machine transitions
- **Error Recovery**: Supports idempotent re-processing after failures

## Features

| Feature | Description |
|---------|-------------|
| Multi-format Support | Handles LHV Bank, Swedbank, and Securities CSV formats |
| BOM Handling | Strips UTF-8 BOM automatically |
| Smart Dedup | Uses provider reference or composite keys for duplicate detection |
| Batch Processing | JDBC batch insert for optimal performance |
| Status Tracking | NEW → IMPORTING → IMPORTED → CONSOLIDATING → CONSOLIDATED |
| Consolidation | GROUP BY aggregation with SUM, AVG, GROUP_CONCAT |
| Error Handling | Comprehensive error capture with workflow routing |

## Installation

### Prerequisites

- Joget DX 8.x
- Java 11+
- Maven 3.6+
- gam-framework plugin installed

### Build the Plugin

```bash
# Clone the repository
git clone <repository-url>
cd statement-importer

# Build the OSGi bundle
mvn clean package

# The bundle JAR will be in target/statement-importer-8.1-SNAPSHOT.jar
```

### Deploy to Joget

1. Navigate to **Joget Admin** > **Manage Plugins**
2. Click **Upload Plugin**
3. Select `target/statement-importer-8.1-SNAPSHOT.jar`
4. Click **Upload**

The plugin will be registered and available as "Statement Importer" in the Process Tool list.

## Configuration

### Workflow Variables

The plugin expects these workflow variables to be set before execution:

| Variable | Type | Required | Description |
|----------|------|----------|-------------|
| `recordId` | String | Yes | ID of the bank_statement record |
| `account_type` | String | Yes | Either "bank" or "secu" |
| `bank` | String | No | Bank identifier (informational) |
| `from_date` | String | Yes | Statement period start (yyyy-MM-dd) |
| `to_date` | String | Yes | Statement period end (yyyy-MM-dd) |

### Output Variables

After execution, the plugin sets:

| Variable | Description |
|----------|-------------|
| `statementStatus` | "imported" on success, "error" on failure |

### Database Tables

The plugins write to these Joget form tables:

**Statement:**
- `app_fd_bank_statement` - Statement metadata and status

**Raw Transactions (StatementImporter):**
- `app_fd_bank_account_trx` - Bank transaction raw rows
- `app_fd_sec_account_trx` - Securities transaction raw rows

**Consolidated Transactions (StatementConsolidator):**
- `app_fd_bank_total_trx` - Consolidated bank transactions
- `app_fd_secu_total_trx` - Consolidated securities transactions

## Supported CSV Formats

### LHV Bank (`LHV_BANK`)

- **Separator**: Comma (`,`)
- **Detection**: Header contains "dokumendi number"
- **Columns**: 19 (empty column at index 6 is stripped)
- **Target**: `bank_account_trx`

### Swedbank (`SWEDBANK`)

- **Separator**: Semicolon (`;`)
- **Detection**: Header contains "dok nr"
- **Columns**: 14
- **Target**: `bank_account_trx`

### Securities (`SECURITIES`)

- **Separator**: Comma (`,`)
- **Detection**: Header contains "väärtuspäev" AND "tehingupäev"
- **Columns**: 13
- **Target**: `sec_account_trx`

## Usage

### In BPMN Workflow

1. Add a **Tool** activity to your BPMN process
2. Select **Statement Importer** as the plugin
3. Map the required workflow variables in the tool configuration
4. Connect the tool output to an Exclusive Gateway for routing based on `statementStatus`

### Processing Flow

```
[Upload CSV] -> [Submit Form] -> [Statement Importer] -> [Statement Consolidator]
                                         |                        |
                    +--------------------+           +------------+------------+
                    |                                |                         |
              status=imported                  status=consolidated      status=error
                    |                                |                         |
               [Continue]                       [Continue]              [Error Handler]
```

### Example BPMN Configuration

```xml
<serviceTask id="importStatement" name="Import Statement">
  <extensionElements>
    <joget:tool class="com.fiscaladmin.gam.statementimporter.lib.StatementImporter">
      <joget:property key="recordId" value="#{recordId}"/>
      <joget:property key="account_type" value="#{accountType}"/>
      <joget:property key="from_date" value="#{fromDate}"/>
      <joget:property key="to_date" value="#{toDate}"/>
    </joget:tool>
  </extensionElements>
</serviceTask>
```

## Architecture

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
│   ├── FieldMapping.java            # CSV index -> DB column mapping
│   ├── MappingConfig.java           # Table + field mappings container
│   └── MappingConfigurations.java   # Static configs per format
├── dedup/
│   ├── DeduplicationChecker.java    # Duplicate detection logic
│   └── DeduplicationResult.java     # Result container
├── persister/
│   └── RawTransactionPersister.java # JDBC batch insert for raw rows
└── consolidation/
    ├── BankConsolidationQuery.java      # Bank GROUP BY + INSERT SQL
    ├── SecuConsolidationQuery.java      # Securities GROUP BY + INSERT SQL
    └── ConsolidatedRowPersister.java    # JDBC batch insert for consolidated rows
```

### Processing Pipeline

```
1. Read workflow variables
2. Load statement from bank_statement table
3. Resolve physical CSV file
4. Delete existing raw rows (idempotency)
5. Transition: NEW -> IMPORTING
6. Detect CSV format
7. Parse CSV rows
8. De-duplication check
9. Batch insert non-duplicates
10. Update statement metadata
11. Transition: IMPORTING -> IMPORTED
12. Set workflow variable
```

### De-duplication Strategy

| Account Type | Primary Key | Fallback Composite Key |
|--------------|-------------|------------------------|
| Bank | `provider_reference` | account_number + document_nr + payment_date + payment_amount + currency |
| Securities | `reference` | value_date + transaction_date + type + ticker + amount + currency |

## Development

### Project Structure

```
statement-importer/
├── pom.xml                 # Maven build configuration
├── CLAUDE.md               # AI assistant context
├── README.md               # This file
├── docs/
│   ├── TECHNICAL.md        # Technical API documentation
│   └── USER_GUIDE.md       # End-user guide
└── src/
    ├── main/java/          # Source code
    └── test/java/          # Unit tests
```

### Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| wflow-core | 8.1-SNAPSHOT | Joget platform APIs |
| gam-framework | 8.1-SNAPSHOT | Status management |
| commons-csv | 1.10.0 | CSV parsing |
| junit | 4.13.2 | Unit testing |
| mockito | 4.11.0 | Mocking |
| h2 | 2.2.224 | In-memory DB for tests |

### Building

```bash
# Full build with tests
mvn clean package

# Skip tests
mvn clean package -DskipTests

# Run only unit tests
mvn test
```

## Testing

### Run All Tests

```bash
mvn test
```

### Run Specific Test Class

```bash
mvn test -Dtest=CsvFormatDetectorTest
```

### Run Specific Test Method

```bash
mvn test -Dtest=CsvFormatDetectorTest#detectLhvBankFormat
```

### Test Coverage

| Package | Test Class |
|---------|------------|
| parser | CsvFormatDetectorTest, StatementParserTest |
| mapping | MappingConfigurationsTest |
| dedup | DeduplicationCheckerTest |
| persister | RawTransactionPersisterTest, RealCsvEndToEndTest |
| lib | StatementImporterTest |
| consolidation | BankConsolidationTest, SecuConsolidationTest, ConsolidatedRowPersisterTest, ConsolidationEndToEndTest |

## Troubleshooting

### Common Issues

#### "Unrecognised CSV format"

**Cause**: The CSV header doesn't match any known format pattern.

**Solution**: Verify the CSV file is from a supported bank and has the correct header row.

#### "Format mismatch"

**Cause**: The detected format's account type differs from the workflow variable.

**Solution**: Ensure the `account_type` workflow variable matches the uploaded file type.

#### "CSV file not found on disk"

**Cause**: The file reference in the database doesn't point to a valid file.

**Solution**: Check that the file was uploaded correctly and the path is accessible.

#### "Statement record not found"

**Cause**: The `recordId` doesn't exist in the `bank_statement` table.

**Solution**: Verify the workflow variable is passing the correct record ID.

### Logging

The plugins log to the Joget server log at INFO level. Key log prefixes:

- `StatementImporter` - Import orchestration events
- `StatementConsolidator` - Consolidation orchestration events
- `CsvFormatDetector` - Format detection
- `DeduplicationChecker` - Duplicate detection results
- `RawTransactionPersister` - Raw row batch insert operations
- `ConsolidatedRowPersister` - Consolidated row batch insert operations

### Debug Mode

For detailed debugging, adjust the Joget logging configuration to enable DEBUG level for:

```
com.fiscaladmin.gam.statementimporter
```

## License

Proprietary - FiscalAdmin GAM Platform

---

For technical API documentation, see [docs/TECHNICAL.md](docs/TECHNICAL.md).
For end-user instructions, see [docs/USER_GUIDE.md](docs/USER_GUIDE.md).
