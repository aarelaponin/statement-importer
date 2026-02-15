# Statement Consolidator — Design Document

**GAM Automated Accounting System — Genesis Asset Management**

**Version:** 1.0 | **Date:** 15 February 2026
**Platform:** Joget DX Enterprise Edition 8.1 + Java 11 OSGi Plugins
**Replaces:** Plan tasks T2.8–T2.10 (BeanShell consolidation scripts)

---

## 1. Purpose

This document specifies the `StatementConsolidator` — a Joget Process Tool plugin that performs GROUP BY consolidation of raw imported transaction rows into summary rows for downstream enrichment processing.

It reads raw rows from staging tables (`app_fd_bank_account_trx` / `app_fd_sec_account_trx`), aggregates them by business key, and writes consolidated rows into processing tables (`app_fd_bank_total_trx` / `app_fd_secu_total_trx`).

**Architectural decision:** The original preprocessing plan (T2.8–T2.9) specified BeanShell Process Tool scripts for consolidation. This document replaces that approach with a Java Process Tool within the existing `statement-importer` OSGi bundle. Rationale: consistency with the plugin-based architecture, testability with H2, and elimination of BeanShell from the preprocessing pipeline.

---

## 2. Context — Where This Fits

### 2.1 Pipeline Position

```
Stage 1: UPLOAD          Stage 2: IMPORT               Stage 3: CONSOLIDATION
─────────────────        ─────────────────────          ─────────────────────────
User uploads CSV         StatementImporter plugin       StatementConsolidator plugin ← THIS
saves F01.00             CSV → F01.01 / F01.02          F01.01/02 → F01.03/04
wf-activator starts      status: new→importing→imported  status: imported→consolidating
BPMN workflow                                                    →consolidated
```

### 2.2 BPMN Workflow Activity

In the `gam_statement_submission` BPMN process:

- **Activity 1** (Import): configured with `StatementImporter` Process Tool
- **Gateway 1**: checks `statementStatus == "imported"`
- **Activity 2** (Consolidation): configured with `StatementConsolidator` Process Tool ← THIS
- **Gateway 2**: checks `statementStatus == "consolidated"`

### 2.3 Predecessor

The `StatementImporter` plugin (steps 2.1–2.7) has already:
- Parsed the CSV file and detected the format
- Inserted raw rows into `app_fd_bank_account_trx` (161 rows for sample bank) or `app_fd_sec_account_trx` (45 rows for sample secu)
- Set statement status to `imported`
- Set workflow variable `statementStatus = "imported"`
- Updated statement metadata: `row_count`, `duplicate_count`, `processing_timestamp`

### 2.4 Successor

After consolidation, consolidated rows sit at `status = new` in `app_fd_bank_total_trx` / `app_fd_secu_total_trx`. The enrichment pipeline (out of scope) picks them up from there.

---

## 3. Source Tables — Input

### 3.1 Bank Raw Rows: `app_fd_bank_account_trx` (F01.01)

| Column | Type | Description | Role in Consolidation |
|--------|------|-------------|----------------------|
| `id` | VARCHAR(255) | PK (UUID) | Not used |
| `c_statement_id` | VARCHAR(255) | FK to bank_statement | WHERE filter |
| `c_transaction_id` | VARCHAR(10) | Sequential row number | Not used |
| `c_account_number` | VARCHAR(255) | Bank account number | GROUP BY |
| `c_document_nr` | VARCHAR(255) | Document number | GROUP BY |
| `c_payment_date` | VARCHAR(255) | Payment date (yyyy-MM-dd) | GROUP BY |
| `c_other_side_account` | VARCHAR(255) | Counterparty account | GROUP BY |
| `c_other_side_name` | VARCHAR(255) | Counterparty name | GROUP BY |
| `c_other_side_bank` | VARCHAR(255) | Counterparty bank | GROUP BY |
| `c_d_c` | VARCHAR(10) | Debit/Credit indicator | GROUP BY |
| `c_payment_amount` | VARCHAR(255) | Amount (text, parseable as BigDecimal) | **SUM** aggregate |
| `c_reference_number` | VARCHAR(255) | Reference number | GROUP BY |
| `c_archival_number` | VARCHAR(255) | Archival number | Carry-through (first) |
| `c_payment_description` | TEXT | Payment description | Carry-through (first) |
| `c_transaction_fee` | VARCHAR(255) | Fee (text, parseable as BigDecimal) | **SUM** aggregate |
| `c_currency` | VARCHAR(255) | Currency code | GROUP BY |
| `c_customer_id` | VARCHAR(255) | Customer ID | GROUP BY |
| `c_other_side_bic` | VARCHAR(255) | Counterparty BIC | GROUP BY |
| `c_initiator` | VARCHAR(255) | Transaction initiator | Carry-through (first) |
| `c_transaction_reference` | VARCHAR(255) | Bank reference | Carry-through (first) |
| `c_provider_reference` | VARCHAR(255) | Provider reference | Carry-through (first) |

### 3.2 Securities Raw Rows: `app_fd_sec_account_trx` (F01.02)

| Column | Type | Description | Role in Consolidation |
|--------|------|-------------|----------------------|
| `id` | VARCHAR(255) | PK (UUID) | Not used |
| `c_statement_id` | VARCHAR(255) | FK to bank_statement | WHERE filter |
| `c_transaction_id` | VARCHAR(10) | Sequential row number | Not used |
| `c_value_date` | VARCHAR(255) | Value/settlement date | GROUP BY |
| `c_transaction_date` | VARCHAR(255) | Trade date | GROUP BY |
| `c_type` | VARCHAR(255) | Transaction type (ost, müük, div, etc.) | GROUP BY |
| `c_ticker` | VARCHAR(255) | Security ticker | GROUP BY |
| `c_description` | TEXT | Security description | GROUP BY |
| `c_quantity` | VARCHAR(255) | Number of shares/units | **SUM** aggregate |
| `c_price` | VARCHAR(255) | Price per unit | **AVG** aggregate |
| `c_currency` | VARCHAR(255) | Currency code | GROUP BY |
| `c_amount` | VARCHAR(255) | Net amount | **SUM** aggregate |
| `c_fee` | VARCHAR(255) | Fee amount | **SUM** aggregate |
| `c_total_amount` | VARCHAR(255) | Total (amount + fee) | **SUM** aggregate |
| `c_reference` | VARCHAR(255) | Transaction reference | Carry-through (concatenated) |
| `c_comment` | TEXT | Comment | Carry-through (first) |

---

## 4. Target Tables — Output

### 4.1 Bank Consolidated: `app_fd_bank_total_trx` (F01.03)

Columns derived from the form API (`f0103-openapi.yaml`):

| Column | Source | Notes |
|--------|--------|-------|
| `id` | Generated UUID | Joget PK, 36-char UUID |
| `c_statement_id` | Input parameter | FK to bank_statement |
| `c_account_number` | GROUP BY passthrough | |
| `c_document_nr` | GROUP BY passthrough | |
| `c_payment_date` | GROUP BY passthrough | |
| `c_other_side_account` | GROUP BY passthrough | |
| `c_other_side_name` | GROUP BY passthrough | |
| `c_other_side_bank` | GROUP BY passthrough | |
| `c_d_c` | GROUP BY passthrough | |
| `c_reference_number` | GROUP BY passthrough | |
| `c_currency` | GROUP BY passthrough | |
| `c_customer_id` | GROUP BY passthrough | |
| `c_other_side_bic` | GROUP BY passthrough | |
| `c_payment_amount` | `SUM(CAST(... AS DECIMAL(15,2)))` | Aggregated, stored as text |
| `c_transaction_fee` | `SUM(CAST(... AS DECIMAL(15,2)))` | Aggregated, stored as text |
| `c_payment_description` | `MIN(...)` — first row in group | Carry-through |
| `c_archival_number` | `MIN(...)` — first row in group | Carry-through |
| `c_initiator` | `MIN(...)` — first row in group | Carry-through |
| `c_transaction_reference` | `MIN(...)` — first row in group | Carry-through |
| `c_provider_reference` | `MIN(...)` — first row in group | Carry-through |
| `c_statement_reference` | Generated: `STMT{YYYY}.{SEQ}` | Statement reference |
| `c_status` | `"new"` | Initial status |
| `c_account_type` | `"bank"` | Identifies the source |
| `c_transaction_id` | Generated: sequential (zero-padded) | |
| `dateCreated` | Current timestamp | Joget audit field |
| `createdBy` | `"system"` | Joget audit field |

**Columns NOT populated during consolidation** (remain NULL, populated later by enrichment or pairing):
`c_enrichment_id`, `c_trx_account_id`, `c_acc_post_id`, `c_main_bank_total_trx`, `c_allocated_amount`

### 4.2 Securities Consolidated: `app_fd_secu_total_trx` (F01.04)

Columns derived from the form API (`f0104-openapi.yaml`):

| Column | Source | Notes |
|--------|--------|-------|
| `id` | Generated UUID | Joget PK |
| `c_statement_id` | Input parameter | FK to bank_statement |
| `c_value_date` | GROUP BY passthrough | |
| `c_transaction_date` | GROUP BY passthrough | |
| `c_type` | GROUP BY passthrough | |
| `c_ticker` | GROUP BY passthrough | |
| `c_description` | GROUP BY passthrough | |
| `c_currency` | GROUP BY passthrough | |
| `c_quantity` | `SUM(CAST(... AS DECIMAL(15,4)))` | 4 decimal places for fractional shares |
| `c_price` | `AVG(CAST(... AS DECIMAL(15,6)))` | Simple average (not weighted) — documented limitation |
| `c_amount` | `SUM(CAST(... AS DECIMAL(15,2)))` | |
| `c_fee` | `SUM(CAST(... AS DECIMAL(15,2)))` | |
| `c_total_amount` | `SUM(CAST(... AS DECIMAL(15,2)))` | |
| `c_reference` | `GROUP_CONCAT(... SEPARATOR '; ')` | All refs concatenated |
| `c_comment` | `MIN(...)` — first row in group | Carry-through |
| `c_statement_reference` | Generated: `STMT{YYYY}.{SEQ}` | Statement reference |
| `c_status` | `"new"` | Initial status |
| `c_transaction_id` | Generated: sequential (zero-padded) | |
| `dateCreated` | Current timestamp | Joget audit field |
| `createdBy` | `"system"` | Joget audit field |

**Columns NOT populated during consolidation** (remain NULL):
`c_enrichment_id`, `c_trx_account_id`, `c_acc_post_id`, `c_internal_type`, `c_transaction_type`, `c_bank_payment_trx_id`, `c_bank_fee_trx_id`, `c_allocated_amount`

### 4.3 Known Limitation: Securities Price Averaging

`AVG(c_price)` is a simple arithmetic average, not a volume-weighted average price (VWAP). For the LHV1T case (two buy fragments: 257 @ 3.50 + 500 @ 3.50), the prices happen to be equal so this doesn't matter. For future cases where fragment prices differ, VWAP should be computed as `SUM(amount) / SUM(quantity)`. This is documented for Phase 2 improvement.

---

## 5. Consolidation SQL

### 5.1 Bank Consolidation Query

```sql
SELECT
    c_account_number,
    c_document_nr,
    c_payment_date,
    c_other_side_account,
    c_other_side_name,
    c_other_side_bank,
    c_d_c,
    c_reference_number,
    c_currency,
    c_customer_id,
    c_other_side_bic,
    SUM(CAST(c_payment_amount AS DECIMAL(15,2))) AS agg_payment_amount,
    SUM(CAST(c_transaction_fee AS DECIMAL(15,2))) AS agg_transaction_fee,
    MIN(c_payment_description) AS first_description,
    MIN(c_archival_number) AS first_archival,
    MIN(c_initiator) AS first_initiator,
    MIN(c_transaction_reference) AS first_trx_ref,
    MIN(c_provider_reference) AS first_provider_ref,
    COUNT(*) AS group_row_count
FROM app_fd_bank_account_trx
WHERE c_statement_id = ?
GROUP BY
    c_account_number,
    c_document_nr,
    c_payment_date,
    c_other_side_account,
    c_other_side_name,
    c_other_side_bank,
    c_d_c,
    c_reference_number,
    c_currency,
    c_customer_id,
    c_other_side_bic
ORDER BY c_payment_date, c_document_nr
```

**Result column index mapping** (used in Java when reading `ResultSet`):

| Index | Alias | Target Column |
|-------|-------|---------------|
| 1 | `c_account_number` | `c_account_number` |
| 2 | `c_document_nr` | `c_document_nr` |
| 3 | `c_payment_date` | `c_payment_date` |
| 4 | `c_other_side_account` | `c_other_side_account` |
| 5 | `c_other_side_name` | `c_other_side_name` |
| 6 | `c_other_side_bank` | `c_other_side_bank` |
| 7 | `c_d_c` | `c_d_c` |
| 8 | `c_reference_number` | `c_reference_number` |
| 9 | `c_currency` | `c_currency` |
| 10 | `c_customer_id` | `c_customer_id` |
| 11 | `c_other_side_bic` | `c_other_side_bic` |
| 12 | `agg_payment_amount` | `c_payment_amount` |
| 13 | `agg_transaction_fee` | `c_transaction_fee` |
| 14 | `first_description` | `c_payment_description` |
| 15 | `first_archival` | `c_archival_number` |
| 16 | `first_initiator` | `c_initiator` |
| 17 | `first_trx_ref` | `c_transaction_reference` |
| 18 | `first_provider_ref` | `c_provider_reference` |
| 19 | `group_row_count` | (metadata only — not stored) |

### 5.2 Securities Consolidation Query

```sql
SELECT
    c_value_date,
    c_transaction_date,
    c_type,
    c_ticker,
    c_description,
    c_currency,
    SUM(CAST(c_quantity AS DECIMAL(15,4))) AS agg_quantity,
    AVG(CAST(c_price AS DECIMAL(15,6))) AS agg_price,
    SUM(CAST(c_amount AS DECIMAL(15,2))) AS agg_amount,
    SUM(CAST(c_fee AS DECIMAL(15,2))) AS agg_fee,
    SUM(CAST(c_total_amount AS DECIMAL(15,2))) AS agg_total_amount,
    GROUP_CONCAT(c_reference ORDER BY c_reference SEPARATOR '; ') AS concat_reference,
    MIN(c_comment) AS first_comment,
    COUNT(*) AS group_row_count
FROM app_fd_sec_account_trx
WHERE c_statement_id = ?
GROUP BY
    c_value_date,
    c_transaction_date,
    c_type,
    c_ticker,
    c_description,
    c_currency
ORDER BY c_transaction_date, c_ticker, c_type
```

**Result column index mapping:**

| Index | Alias | Target Column |
|-------|-------|---------------|
| 1 | `c_value_date` | `c_value_date` |
| 2 | `c_transaction_date` | `c_transaction_date` |
| 3 | `c_type` | `c_type` |
| 4 | `c_ticker` | `c_ticker` |
| 5 | `c_description` | `c_description` |
| 6 | `c_currency` | `c_currency` |
| 7 | `agg_quantity` | `c_quantity` |
| 8 | `agg_price` | `c_price` |
| 9 | `agg_amount` | `c_amount` |
| 10 | `agg_fee` | `c_fee` |
| 11 | `agg_total_amount` | `c_total_amount` |
| 12 | `concat_reference` | `c_reference` |
| 13 | `first_comment` | `c_comment` |
| 14 | `group_row_count` | (metadata only) |

### 5.3 H2 Compatibility Note

`GROUP_CONCAT` is MySQL-specific. In H2 (used for testing), the equivalent is `LISTAGG`:

```sql
-- H2 equivalent:
LISTAGG(c_reference, '; ') WITHIN GROUP (ORDER BY c_reference) AS concat_reference
```

The consolidator code should use a SQL dialect abstraction or the tests should use a helper that adapts the query for H2. Recommended approach: extract the aggregation query strings into constants, and provide an H2-compatible variant for the test class. Alternatively, set H2 to MySQL compatibility mode (`MODE=MySQL`) which supports `GROUP_CONCAT`.

**Recommended H2 connection string for tests:**
```
jdbc:h2:mem:consolidation_test;DB_CLOSE_DELAY=-1;MODE=MySQL
```

### 5.4 Idempotency — Delete Before Insert

Before running the GROUP BY and inserting new rows, delete any existing consolidated rows:

```sql
DELETE FROM app_fd_bank_total_trx WHERE c_statement_id = ?;
-- or
DELETE FROM app_fd_secu_total_trx WHERE c_statement_id = ?;
```

This makes the consolidation safely re-runnable. After error recovery (operator resets statement to `new` and re-runs), the pipeline cleans up its own prior output.

---

## 6. Insert SQL

### 6.1 Bank Insert

```sql
INSERT INTO app_fd_bank_total_trx (
    id,
    c_statement_id,
    c_account_number,
    c_document_nr,
    c_payment_date,
    c_other_side_account,
    c_other_side_name,
    c_other_side_bank,
    c_d_c,
    c_reference_number,
    c_currency,
    c_customer_id,
    c_other_side_bic,
    c_payment_amount,
    c_transaction_fee,
    c_payment_description,
    c_archival_number,
    c_initiator,
    c_transaction_reference,
    c_provider_reference,
    c_statement_reference,
    c_status,
    c_account_type,
    c_transaction_id,
    dateCreated,
    createdBy
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

**26 parameters.**

### 6.2 Securities Insert

```sql
INSERT INTO app_fd_secu_total_trx (
    id,
    c_statement_id,
    c_value_date,
    c_transaction_date,
    c_type,
    c_ticker,
    c_description,
    c_currency,
    c_quantity,
    c_price,
    c_amount,
    c_fee,
    c_total_amount,
    c_reference,
    c_comment,
    c_statement_reference,
    c_status,
    c_transaction_id,
    dateCreated,
    createdBy
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

**20 parameters.**

---

## 7. Statement Reference Generation

Format: `STMT{YYYY}.{SEQ}`

- `{YYYY}` — extracted from the statement's `from_date` field (year portion)
- `{SEQ}` — zero-padded three digits, sequential within this consolidation run, starting at 001

**Example:** `STMT2024.001`, `STMT2024.002`, ... `STMT2024.042`

**Implementation:**

```java
private String generateStatementReference(String fromDate, int seq) {
    String year = fromDate.substring(0, 4); // "2024-06-01" → "2024"
    return String.format("STMT%s.%03d", year, seq);
}
```

---

## 8. Class Design

### 8.1 Bundle Location

The `StatementConsolidator` lives in the **same OSGi bundle** as `StatementImporter`:

```
statement-importer/
  src/main/java/com/fiscaladmin/gam/
    Activator.java                              ← updated to register both plugins
    statementimporter/
      lib/
        StatementImporter.java                  ← existing (Activity 1)
        StatementConsolidator.java              ← NEW (Activity 2)
      consolidation/                            ← NEW package
        BankConsolidationQuery.java             ← SQL + column mapping for bank
        SecuConsolidationQuery.java             ← SQL + column mapping for secu
        ConsolidatedRowPersister.java           ← JDBC batch insert for consolidated rows
      dedup/                                    ← existing
      mapping/                                  ← existing
      parser/                                   ← existing
      persister/                                ← existing
  src/main/resources/properties/app/
    StatementImporter.json                      ← existing
    StatementConsolidator.json                  ← NEW
  src/test/java/com/fiscaladmin/gam/statementimporter/
    consolidation/                              ← NEW
      BankConsolidationTest.java
      SecuConsolidationTest.java
      ConsolidatedRowPersisterTest.java
      ConsolidationEndToEndTest.java
```

### 8.2 StatementConsolidator — Process Tool Plugin

```java
package com.fiscaladmin.gam.statementimporter.lib;

/**
 * Statement Consolidator — Joget Process Tool plugin.
 *
 * Groups raw imported transaction rows by business key and
 * inserts consolidated summary rows for downstream enrichment.
 *
 * Configured as Activity 2 in the gam_statement_submission BPMN process.
 *
 * Orchestration flow:
 *   1. Read recordId from workflow
 *   2. Load statement record (account_type, from_date)
 *   3. Status: IMPORTED → CONSOLIDATING (via StatusManager)
 *   4. Idempotency: delete existing consolidated rows for this statement
 *   5. Execute GROUP BY query (bank or secu based on account_type)
 *   6. Generate statement_reference values
 *   7. Batch-insert consolidated rows with status = "new"
 *   8. Update statement metadata: total_count
 *   9. Status: CONSOLIDATING → CONSOLIDATED
 *  10. Set workflow variable statementStatus = "consolidated"
 */
public class StatementConsolidator extends DefaultApplicationPlugin {
    // ...
}
```

**Key methods:**

| Method | Responsibility |
|--------|---------------|
| `execute(Map properties)` | Main orchestration (same pattern as StatementImporter) |
| `consolidateBank(Connection con, String statementId, String fromDate)` | Runs bank GROUP BY, returns list of consolidated rows |
| `consolidateSecu(Connection con, String statementId, String fromDate)` | Runs secu GROUP BY, returns list of consolidated rows |
| `deleteExistingConsolidatedRows(Connection con, String statementId, String accountType)` | Idempotency delete |
| `updateStatementMetadata(FormDataDao dao, String recordId, int totalCount)` | Sets total_count |

### 8.3 Consolidation Query Classes

Rather than embedding long SQL strings in the consolidator, extract them into dedicated classes:

**`BankConsolidationQuery`** — encapsulates:
- The GROUP BY SQL string (section 5.1)
- The INSERT SQL string (section 6.1)
- Column index constants for reading ResultSet
- Method `readRow(ResultSet rs, String statementId, String fromDate, int seq)` → returns a `Map<String, String>` of column→value pairs

**`SecuConsolidationQuery`** — encapsulates:
- The GROUP BY SQL string (section 5.2)
- The INSERT SQL string (section 6.2)
- Column index constants
- Method `readRow(ResultSet rs, String statementId, String fromDate, int seq)` → Map

### 8.4 ConsolidatedRowPersister

Follows the same pattern as `RawTransactionPersister`:

```java
/**
 * Batch-inserts consolidated rows into the target total transaction table.
 * Uses JDBC batch for performance (same approach as RawTransactionPersister).
 */
public class ConsolidatedRowPersister {
    
    /**
     * @param rows         list of column→value maps (from consolidation query)
     * @param insertSql    the INSERT SQL with placeholders
     * @param columnOrder  ordered list of column names matching INSERT placeholder positions
     * @param con          JDBC connection
     * @return number of rows inserted
     */
    public static int persist(List<Map<String, String>> rows, 
                               String insertSql, 
                               List<String> columnOrder, 
                               Connection con) throws SQLException {
        // JDBC batch insert
    }
}
```

### 8.5 Activator Update

```java
public class Activator implements BundleActivator {
    
    protected Collection<ServiceRegistration> registrationList;

    public void start(BundleContext context) {
        registrationList = new ArrayList<>();
        registrationList.add(context.registerService(
            StatementImporter.class.getName(), new StatementImporter(), null));
        registrationList.add(context.registerService(
            StatementConsolidator.class.getName(), new StatementConsolidator(), null));
    }
    // stop() unchanged
}
```

---

## 9. Execute Flow — Detailed Pseudocode

```
execute(Map properties):
    recordId = extractRecordId(properties)     // same pattern as StatementImporter
    
    dao = StatusManager.getFormDataDao()
    statementRow = dao.load("bank_statement", "bank_statement", recordId)
    
    accountType = statementRow.getProperty("account_type")   // "bank" or "secu"
    fromDate    = statementRow.getProperty("from_date")      // "2024-06-01"
    
    // Status transition: IMPORTED → CONSOLIDATING
    statusManager.transition(dao, STATEMENT, recordId, CONSOLIDATING, 
                             "statement-consolidator", "Starting consolidation")
    
    ds = AppUtil.getBean("setupDataSource")
    con = ds.getConnection()
    
    try:
        // Idempotency: delete existing consolidated rows
        targetTable = (accountType == "bank") 
                        ? "app_fd_bank_total_trx" 
                        : "app_fd_secu_total_trx"
        DELETE FROM {targetTable} WHERE c_statement_id = ?
        
        // Execute GROUP BY
        if accountType == "bank":
            resultRows = executeBankConsolidation(con, recordId, fromDate)
            insertSql  = BankConsolidationQuery.INSERT_SQL
            columnOrder = BankConsolidationQuery.INSERT_COLUMN_ORDER
        else:
            resultRows = executeSecuConsolidation(con, recordId, fromDate)
            insertSql  = SecuConsolidationQuery.INSERT_SQL
            columnOrder = SecuConsolidationQuery.INSERT_COLUMN_ORDER
        
        // Batch-insert
        totalCount = ConsolidatedRowPersister.persist(resultRows, insertSql, columnOrder, con)
        
        // Update statement metadata
        updateStatementMetadata(dao, recordId, totalCount)
        
        // Status transition: CONSOLIDATING → CONSOLIDATED
        statusManager.transition(dao, STATEMENT, recordId, CONSOLIDATED,
                                 "statement-consolidator", 
                                 "Consolidation completed: " + totalCount + " rows")
        
        // Workflow variable for gateway
        setWorkflowVariable(properties, "statementStatus", "consolidated")
        
    catch Exception:
        // Error handling — same pattern as StatementImporter
        statusManager.transition(dao, STATEMENT, recordId, ERROR, ...)
        // fallback: setStatusDirectly(recordId, "error", e.getMessage())
        setWorkflowVariable(properties, "statementStatus", "error")
    finally:
        con.close()
```

---

## 10. Record ID Extraction

Use the exact same multi-approach pattern from `StatementImporter.execute()`:

1. Try workflow variable `id` (from assignment.getProcessId())
2. Try workflow variable `recordId`
3. Try plugin properties `recordId`
4. Try plugin properties `id`

This code should be extracted into a shared utility method. For now, duplicate the pattern from `StatementImporter` — refactoring into a shared helper can be done in a later session.

---

## 11. Error Handling

Follow the same pattern as `StatementImporter`:

1. **StatusManager transition to ERROR** — try first
2. **Direct SQL fallback** — if StatusManager fails (e.g., unexpected current state), use direct `UPDATE app_fd_bank_statement SET c_status = ?, c_error_message = ? WHERE id = ?`
3. **Populate error_message** — write the error description to the statement's `error_message` field
4. **Set workflow variable** — `statementStatus = "error"` for gateway routing
5. **Truncate error messages** — max 1000 chars (same helper as StatementImporter)

### Error Scenarios Specific to Consolidation

| Scenario | Detection | Error Message |
|----------|-----------|---------------|
| Zero raw rows for statement | GROUP BY returns empty ResultSet | "No raw rows found for statement {id} in {table}. Import may not have run." |
| SQL syntax error | SQLException | "SQL error during consolidation: {message}" |
| CAST failure (non-numeric amount) | SQLException from CAST | "Invalid numeric value in raw data: {detail}" |
| Connection failure | SQLException | "Database connection failed: {message}" |
| Statement not found | FormRow is null | "Statement record not found: {id}" |
| Status not IMPORTED | InvalidTransitionException | "Cannot consolidate: statement status is '{current}', expected 'imported'" |

---

## 12. Plugin Properties

File: `src/main/resources/properties/app/StatementConsolidator.json`

```json
[
    {
        "title": "Statement Consolidator",
        "properties": [
            {
                "name": "info",
                "label": "Configuration",
                "type": "label",
                "value": "Consolidates raw imported transaction rows into summary rows. Reads data from the statement form record. No configuration needed."
            }
        ]
    }
]
```

---

## 13. Amount Formatting

All raw transaction tables store amounts as text (VARCHAR). The consolidation aggregates use `CAST(... AS DECIMAL)` for arithmetic, but the results must be stored back as text in the target tables.

**Formatting rules:**

| Field | Precision | Format Example |
|-------|-----------|----------------|
| `c_payment_amount` | 2 decimal places | "1234.56", "-0.31" |
| `c_transaction_fee` | 2 decimal places | "8.10", "0.00" |
| `c_quantity` | 4 decimal places (strip trailing zeros) | "757", "1.5" |
| `c_price` | 6 decimal places (preserve precision) | "1051.847220", "3.500000" |
| `c_amount` | 2 decimal places | "-2649.50" |
| `c_fee` | 2 decimal places | "-8.10" |
| `c_total_amount` | 2 decimal places | "-2657.60" |

**Implementation:** Use `BigDecimal` from the `ResultSet` and format with appropriate scale:

```java
BigDecimal amount = rs.getBigDecimal("agg_payment_amount");
String formatted = amount.setScale(2, RoundingMode.HALF_UP).toPlainString();
```

For `c_quantity`, strip trailing zeros:
```java
BigDecimal qty = rs.getBigDecimal("agg_quantity");
String formatted = qty.stripTrailingZeros().toPlainString();
```

For `c_price`, preserve 6 decimal places:
```java
BigDecimal price = rs.getBigDecimal("agg_price");
String formatted = price.setScale(6, RoundingMode.HALF_UP).toPlainString();
```

---

## 14. Testing Strategy

### 14.1 Test Infrastructure

All tests use H2 in-memory database in MySQL compatibility mode:

```java
Connection con = DriverManager.getConnection(
    "jdbc:h2:mem:consolidation_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
```

### 14.2 H2 Table Schemas

**Source tables** (same as in `RealCsvEndToEndTest`):

```sql
-- app_fd_bank_account_trx — same CREATE TABLE as in RealCsvEndToEndTest.setUp()
-- app_fd_sec_account_trx  — same CREATE TABLE as in RealCsvEndToEndTest.setUp()
```

**Target tables** (new — must mirror Joget form schemas):

```sql
CREATE TABLE IF NOT EXISTS app_fd_bank_total_trx (
    id VARCHAR(255) PRIMARY KEY,
    c_statement_id VARCHAR(255),
    c_transaction_id VARCHAR(10),
    c_account_number VARCHAR(255),
    c_document_nr VARCHAR(255),
    c_payment_date VARCHAR(255),
    c_other_side_account VARCHAR(255),
    c_other_side_name VARCHAR(255),
    c_other_side_bank VARCHAR(255),
    c_d_c VARCHAR(10),
    c_reference_number VARCHAR(255),
    c_currency VARCHAR(255),
    c_customer_id VARCHAR(255),
    c_other_side_bic VARCHAR(255),
    c_payment_amount VARCHAR(255),
    c_transaction_fee VARCHAR(255),
    c_payment_description TEXT,
    c_archival_number VARCHAR(255),
    c_initiator VARCHAR(255),
    c_transaction_reference VARCHAR(255),
    c_provider_reference VARCHAR(255),
    c_statement_reference VARCHAR(255),
    c_status VARCHAR(50),
    c_account_type VARCHAR(50),
    c_enrichment_id VARCHAR(255),
    c_trx_account_id VARCHAR(255),
    c_acc_post_id VARCHAR(255),
    c_main_bank_total_trx VARCHAR(255),
    c_allocated_amount VARCHAR(255),
    dateCreated VARCHAR(50),
    createdBy VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS app_fd_secu_total_trx (
    id VARCHAR(255) PRIMARY KEY,
    c_statement_id VARCHAR(255),
    c_transaction_id VARCHAR(10),
    c_value_date VARCHAR(255),
    c_transaction_date VARCHAR(255),
    c_type VARCHAR(255),
    c_ticker VARCHAR(255),
    c_description TEXT,
    c_currency VARCHAR(255),
    c_quantity VARCHAR(255),
    c_price VARCHAR(255),
    c_amount VARCHAR(255),
    c_fee VARCHAR(255),
    c_total_amount VARCHAR(255),
    c_reference TEXT,
    c_comment TEXT,
    c_statement_reference VARCHAR(255),
    c_status VARCHAR(50),
    c_internal_type VARCHAR(255),
    c_transaction_type VARCHAR(255),
    c_enrichment_id VARCHAR(255),
    c_trx_account_id VARCHAR(255),
    c_acc_post_id VARCHAR(255),
    c_bank_payment_trx_id VARCHAR(255),
    c_bank_fee_trx_id VARCHAR(255),
    c_allocated_amount VARCHAR(255),
    dateCreated VARCHAR(50),
    createdBy VARCHAR(255)
);
```

### 14.3 Test Classes and Cases

#### `BankConsolidationTest`

| # | Test | What It Verifies |
|---|------|------------------|
| 1 | `bankGroupByReducesRowCount` | Consolidated count < 161 raw rows |
| 2 | `allConsolidatedRowsHaveStatusNew` | Every row has `c_status = "new"` |
| 3 | `statementReferencePattern` | All refs match `STMT2024.NNN` pattern |
| 4 | `statementReferenceSequentialNoDuplicates` | Refs are sequential with no gaps |
| 5 | `amountAggregation` | For a known `document_nr` with multiple raw rows, SUM matches |
| 6 | `feeAggregation` | SUM of fees matches raw row sum for a known group |
| 7 | `carryThroughFieldsPopulated` | `payment_description`, `archival_number` etc. are non-null |
| 8 | `groupByColumnsPreserved` | GROUP BY fields on consolidated row match raw row values |
| 9 | `allRowsHaveStatementId` | Every consolidated row has correct `c_statement_id` |
| 10 | `allRowsHaveAccountTypeBank` | Every consolidated row has `c_account_type = "bank"` |
| 11 | `jogetAuditFieldsSet` | `dateCreated` and `createdBy` are populated |
| 12 | `idIsValidUuid` | `id` column is 36-char UUID format |

#### `SecuConsolidationTest`

| # | Test | What It Verifies |
|---|------|------------------|
| 1 | `secuGroupByReducesRowCount` | Consolidated count < 45 raw rows |
| 2 | `lhv1tFragmentsMerged` | Two LHV1T buy rows → one consolidated row with qty=757 |
| 3 | `lhv1tAmountSummed` | Amount = -(899.50 + 1750.00) = -2649.50 |
| 4 | `lhv1tPriceAveraged` | Price = AVG(3.50, 3.50) = 3.500000 |
| 5 | `lhv1tReferencesConcatenated` | Reference = "903867200; 903867343" |
| 6 | `singleRowGroupUnchanged` | A single-row group (e.g., HLMBK bond buy) has same values |
| 7 | `splitTransactionsNotMerged` | NVDA split+ and split- remain as separate groups (different `c_type`) |
| 8 | `dividendRowsConsolidated` | Dividend rows with same key are grouped |
| 9 | `statementReferencePattern` | All refs match `STMT2024.NNN` |
| 10 | `allStatusNew` | Every row has `c_status = "new"` |
| 11 | `feeSummed` | Fee aggregation correct |
| 12 | `totalAmountSummed` | Total amount aggregation correct |

#### `ConsolidatedRowPersisterTest`

| # | Test | What It Verifies |
|---|------|------------------|
| 1 | `bankInsertColumnsMatchSchema` | INSERT with all columns succeeds against H2 schema |
| 2 | `secuInsertColumnsMatchSchema` | INSERT with all columns succeeds against H2 schema |
| 3 | `batchInsertMultipleRows` | Inserting N rows results in N rows in DB |
| 4 | `emptyListInsertsZero` | Empty input returns 0 |

#### `ConsolidationEndToEndTest`

| # | Test | What It Verifies |
|---|------|------------------|
| 1 | `bankFullPipeline` | Parse CSV → persist raw → consolidate → verify consolidated rows |
| 2 | `secuFullPipeline` | Same for securities |
| 3 | `idempotencyRerun` | Run consolidation twice → same result, no duplicates |
| 4 | `bankAndSecuIsolation` | Bank consolidation doesn't affect secu tables and vice versa |
| 5 | `zeroRawRowsProducesZeroConsolidated` | No raw rows → zero consolidated rows (not an error within the SQL layer; the plugin wrapper decides whether this is an error) |

### 14.4 Test Data Source

Tests use the real CSV files from `test-data/bank_statement.csv` and `test-data/secu_statement.csv` (same files as `RealCsvEndToEndTest`). The test flow is:

1. Parse CSV via `CsvFormatDetector` + `StatementParser` (existing code)
2. Insert raw rows via `RawTransactionPersister.persist()` (existing code)
3. Run consolidation SQL against H2
4. Verify consolidated rows

This tests the full import → consolidation pipeline using real data.

### 14.5 Specific Verification Values

**Bank — known multi-row group check:**
Pick a `document_nr` that appears multiple times in the 161 raw rows. After consolidation, verify that the consolidated row's `c_payment_amount` equals the sum of individual amounts.

**Securities — LHV1T buy fragments:**

| Field | Row 1 (raw) | Row 2 (raw) | Consolidated |
|-------|-------------|-------------|-------------|
| `c_ticker` | LHV1T | LHV1T | LHV1T |
| `c_type` | ost | ost | ost |
| `c_quantity` | 257 | 500 | 757 |
| `c_price` | 3.50000000 | 3.50000000 | 3.500000 (AVG) |
| `c_amount` | -899.50 | -1750.00 | -2649.50 |
| `c_fee` | 0.00 | 0.00 | 0.00 |
| `c_total_amount` | -899.50 | -1750.00 | -2649.50 |
| `c_reference` | 903867200 | 903867343 | 903867200; 903867343 |

---

## 15. Files to Create/Modify

### 15.1 New Files

| File | Purpose |
|------|---------|
| `src/main/java/.../lib/StatementConsolidator.java` | Process Tool plugin — main orchestrator |
| `src/main/java/.../consolidation/BankConsolidationQuery.java` | Bank GROUP BY SQL, column mappings, row reader |
| `src/main/java/.../consolidation/SecuConsolidationQuery.java` | Securities GROUP BY SQL, column mappings, row reader |
| `src/main/java/.../consolidation/ConsolidatedRowPersister.java` | JDBC batch insert for consolidated rows |
| `src/main/resources/properties/app/StatementConsolidator.json` | Plugin property definition |
| `src/test/java/.../consolidation/BankConsolidationTest.java` | Bank consolidation unit tests |
| `src/test/java/.../consolidation/SecuConsolidationTest.java` | Securities consolidation unit tests |
| `src/test/java/.../consolidation/ConsolidatedRowPersisterTest.java` | Persister unit tests |
| `src/test/java/.../consolidation/ConsolidationEndToEndTest.java` | Full pipeline tests |

### 15.2 Modified Files

| File | Change |
|------|--------|
| `src/main/java/.../Activator.java` | Register `StatementConsolidator` as second OSGi service |

### 15.3 No Changes Required

| File | Why Unchanged |
|------|---------------|
| `pom.xml` | No new dependencies needed — JDBC, BigDecimal, UUID are all in Java stdlib; gam-framework and H2 are already declared |
| `StatementImporter.java` | No changes to existing import logic |
| All mapping/parser/dedup/persister classes | Existing code unaffected |

---

## 16. Naming Conventions Checklist

| Convention | This Plugin | Source |
|-----------|-------------|--------|
| Process Tool class name | `StatementConsolidator` | Matches `StatementImporter` |
| Plugin name/label | `"Statement Consolidator"` | Shown in Joget Plugin Manager |
| Package for new code | `com.fiscaladmin.gam.statementimporter.consolidation` | Under existing plugin package |
| Bare table names in FormDataDao | `bank_statement`, `bank_total_trx`, `secu_total_trx` | Per architecture §2.3 |
| Prefixed table names in JDBC | `app_fd_bank_total_trx`, `app_fd_secu_total_trx` | Per architecture §2.3 |
| Column prefix in SQL | `c_` | Per architecture §2.3 |
| Status values | `Status.CONSOLIDATING`, `Status.CONSOLIDATED` | From `gam-framework` Status enum |
| Entity type | `EntityType.STATEMENT` | From `gam-framework` EntityType enum |
| Workflow variable | `statementStatus` | Per architecture §9.3 |
| Statement reference format | `STMT{YYYY}.{SEQ}` | Per architecture §10.3 |
| Audit field: `createdBy` | `"system"` | Matches `RawTransactionPersister` |

---

## 17. Implementation Order

Recommended sequence for a single development session:

| Step | Task | Depends On | Effort |
|------|------|-----------|--------|
| 1 | Create H2 target table schemas in test setUp | — | 15 min |
| 2 | Write `BankConsolidationQuery` (SQL + column mapping) | — | 45 min |
| 3 | Write `BankConsolidationTest` | Steps 1–2 | 45 min |
| 4 | Write `SecuConsolidationQuery` | — | 30 min |
| 5 | Write `SecuConsolidationTest` | Steps 1, 4 | 45 min |
| 6 | Write `ConsolidatedRowPersister` | — | 30 min |
| 7 | Write `ConsolidatedRowPersisterTest` | Steps 1, 6 | 30 min |
| 8 | Write `StatementConsolidator` (orchestrator) | Steps 2, 4, 6 | 60 min |
| 9 | Write `ConsolidationEndToEndTest` | All above | 45 min |
| 10 | Update `Activator.java` | Step 8 | 5 min |
| 11 | Create `StatementConsolidator.json` | — | 5 min |
| 12 | Maven build + test run | All above | 15 min |
| **Total** | | | **~6 hours** |

---

## 18. Verification After Implementation

### 18.1 Maven Build

```bash
cd /Users/aarelaponin/IdeaProjects/gam-plugins/statement-importer
mvn clean package
```

All existing tests (import pipeline) plus all new tests (consolidation) must pass.

### 18.2 SQL Verification Queries (for use in Joget after deployment)

```sql
-- Check consolidated bank row counts
SELECT COUNT(*) FROM app_fd_bank_total_trx WHERE c_statement_id = '{id}';
-- Expected: > 0, < 161

-- Check all have status = new
SELECT c_status, COUNT(*) FROM app_fd_bank_total_trx
WHERE c_statement_id = '{id}' GROUP BY c_status;
-- Expected: all 'new'

-- Check statement reference pattern
SELECT c_statement_reference FROM app_fd_bank_total_trx
WHERE c_statement_id = '{id}' ORDER BY c_statement_reference LIMIT 5;
-- Expected: STMT2024.001, STMT2024.002, ...

-- Check statement metadata updated
SELECT c_status, c_total_count FROM app_fd_bank_statement WHERE id = '{id}';
-- Expected: status=consolidated, total_count matches count above

-- Amount aggregation cross-check
SELECT c_document_nr, c_payment_amount
FROM app_fd_bank_total_trx WHERE c_statement_id = '{id}'
AND c_document_nr IS NOT NULL AND c_document_nr != ''
ORDER BY c_payment_amount DESC LIMIT 5;
-- Cross-check: sum of raw rows for same doc_nr should match

-- Securities: LHV1T fragment merge
SELECT c_ticker, c_quantity, c_price, c_amount
FROM app_fd_secu_total_trx
WHERE c_statement_id = '{id}' AND c_ticker = 'LHV1T' AND c_type = 'ost';
-- Expected: quantity=757, amount=-2649.50

-- Idempotency: re-run should produce same results
-- Run consolidation twice, verify row count unchanged
```

---

*End of design document.*
