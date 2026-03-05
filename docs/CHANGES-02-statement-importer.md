# statement-importer — Change Spec

**Version:** 8.1-SNAPSHOT
**Date:** 4 March 2026
**Dependency:** None (gam-framework unchanged)

---

## 1. Purpose

Preserve the bank transaction reference numbers (`Kande viide` / `c_transaction_reference`) through consolidation as a concatenated text field for traceability and audit. The consolidation algorithm itself (GROUP BY dimensions) must NOT be changed — it implements the business logic of grouping raw bank rows into meaningful customer-visible transactions.

---

## 2. Current State Analysis

### 2.1 Bank Consolidation — Deliberate Business Logic

The 11 GROUP BY dimensions define what constitutes "the same transaction from the customer's perspective":

```
c_account_number, c_document_nr, c_payment_date,
c_other_side_account, c_other_side_name, c_other_side_bank,
c_d_c, c_payment_description, c_currency, c_customer_id, c_other_side_bic
```

Multiple raw bank rows consolidate when they share all 11 identifying fields. For example, 7 raw "Securities sell (ADBE)" rows on 2024-06-03 become 1 consolidated row with summed amounts. This is correct business behavior — from the customer's perspective, this is one sell event.

**The GROUP BY must not be modified.**

### 2.2 What Is Lost During Consolidation

| Raw Field | Consolidated | Reason |
|-----------|-------------|--------|
| c_transaction_reference | ❌ LOST | Not in query at all |
| c_provider_reference | ✅ GROUP_CONCAT | Unique per raw row, concatenated |
| c_reference_number | ❌ LOST | Not in query |
| c_archival_number | ❌ LOST | Not in query |
| c_initiator | ❌ LOST | Not in query |

### 2.3 Actual Data: References Within Consolidation Groups

From the test data:

**Multiple raw rows that consolidate — each has a DIFFERENT c_transaction_reference:**
- 7 "Securities sell (ADBE)" rows: refs 900727947, 900727952, 900727956, 900727959, 900727960, 900727961, 900727964
- 6 LHV1T buy rows: refs 903867200, 903867343, 903869459, 903872803, 903874667, 903897704
- 5 HYLN sell rows: refs 932677233, 932677234, 932677237, 932677238, 932677240

**Rows with SAME reference but different descriptions (don't consolidate):**
- ref 903745878: "Securities buy (HLMBK095034FA)" -1051.85 AND "Securities commission fee (HLMBK095034FA)" -8.10

Adding `c_transaction_reference` to GROUP BY would shatter each consolidation group into individual rows — completely breaking the consolidation purpose.

### 2.4 Secu Side — Same Pattern

Secu consolidation (6 GROUP BY: value_date, transaction_date, type, ticker, description, currency) also merges multiple raw rows. The secu `c_reference` (VIIDE) is already preserved via GROUP_CONCAT. Both sides consolidate at the same granularity: same ticker + same date + same type → one row.

### 2.5 Implication for Pairing

Since both bank and secu consolidation merge the same underlying executions into single rows, **pairing operates at the consolidated level using ticker + date + type** — not individual reference numbers. The reference numbers serve as audit trail only.

---

## 3. Change Required

### 3.1 BankConsolidationQuery.java — Add GROUP_CONCAT for c_transaction_reference

**File:** `src/main/java/com/fiscaladmin/gam/statementimporter/consolidation/BankConsolidationQuery.java`

The approach mirrors the existing `c_provider_reference` pattern: GROUP_CONCAT the values into a comma-separated string.

#### Change 1: GROUP_BY_SQL — Add GROUP_CONCAT in SELECT

Add one more aggregation line. No change to GROUP BY clause.

```java
public static final String GROUP_BY_SQL =
    "SELECT " +
    "  c_account_number, " +
    "  c_document_nr, " +
    "  c_payment_date, " +
    "  c_other_side_account, " +
    "  c_other_side_name, " +
    "  c_other_side_bank, " +
    "  c_d_c, " +
    "  c_payment_description, " +
    "  c_currency, " +
    "  c_customer_id, " +
    "  c_other_side_bic, " +
    "  ROUND(SUM(CAST(c_payment_amount AS DECIMAL(15,2))), 2) AS total_amount, " +
    "  ROUND(SUM(CAST(c_transaction_fee AS DECIMAL(15,2))), 2) AS total_fee, " +
    "  GROUP_CONCAT(c_provider_reference ORDER BY c_transaction_id SEPARATOR ',') AS provider_references, " +
    "  GROUP_CONCAT(c_transaction_reference ORDER BY c_transaction_id SEPARATOR ',') AS transaction_references " +
    "FROM " + SOURCE_TABLE + " " +
    "WHERE c_statement_id = ? " +
    "GROUP BY c_account_number, c_document_nr, c_payment_date, c_other_side_account, " +
    "         c_other_side_name, c_other_side_bank, c_d_c, c_payment_description, " +
    "         c_currency, c_customer_id, c_other_side_bic " +
    "ORDER BY c_payment_date, c_d_c, c_other_side_name";
```

**Note:** GROUP BY clause is unchanged (still 11 dimensions). Only the SELECT gets one more aggregated column.

#### Change 2: INSERT_COLUMNS — Add c_transaction_reference

Add after `c_provider_reference`:

```java
public static final List<String> INSERT_COLUMNS = Arrays.asList(
    "id",
    "c_statement_id",
    "c_statement_reference",
    "c_account_number",
    "c_document_nr",
    "c_payment_date",
    "c_other_side_account",
    "c_other_side_name",
    "c_other_side_bank",
    "c_d_c",
    "c_payment_description",
    "c_currency",
    "c_customer_id",
    "c_other_side_bic",
    "c_payment_amount",
    "c_transaction_fee",
    "c_provider_reference",
    "c_transaction_reference",   // <-- NEW: GROUP_CONCAT'd references
    "c_status",
    "dateCreated",
    "createdBy"
);
```

#### Change 3: INSERT_SQL — Add placeholder

```java
public static final String INSERT_SQL =
    "INSERT INTO " + TARGET_TABLE + " (" +
    "id, " +
    "c_statement_id, " +
    "c_statement_reference, " +
    "c_account_number, " +
    "c_document_nr, " +
    "c_payment_date, " +
    "c_other_side_account, " +
    "c_other_side_name, " +
    "c_other_side_bank, " +
    "c_d_c, " +
    "c_payment_description, " +
    "c_currency, " +
    "c_customer_id, " +
    "c_other_side_bic, " +
    "c_payment_amount, " +
    "c_transaction_fee, " +
    "c_provider_reference, " +
    "c_transaction_reference, " +  // <-- NEW
    "c_status, " +
    "dateCreated, " +
    "createdBy" +
    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
```

Note: 21 placeholders (was 20).

#### Change 4: readRow() — Read the new aggregated column

```java
public static Map<String, String> readRow(ResultSet rs) throws SQLException {
    Map<String, String> row = new HashMap<>();
    // GROUP BY columns (11 identifying fields — UNCHANGED)
    row.put("c_account_number", rs.getString("c_account_number"));
    row.put("c_document_nr", rs.getString("c_document_nr"));
    row.put("c_payment_date", rs.getString("c_payment_date"));
    row.put("c_other_side_account", rs.getString("c_other_side_account"));
    row.put("c_other_side_name", rs.getString("c_other_side_name"));
    row.put("c_other_side_bank", rs.getString("c_other_side_bank"));
    row.put("c_d_c", rs.getString("c_d_c"));
    row.put("c_payment_description", rs.getString("c_payment_description"));
    row.put("c_currency", rs.getString("c_currency"));
    row.put("c_customer_id", rs.getString("c_customer_id"));
    row.put("c_other_side_bic", rs.getString("c_other_side_bic"));
    // Aggregated columns
    row.put("c_payment_amount", rs.getString("total_amount"));
    row.put("c_transaction_fee", rs.getString("total_fee"));
    row.put("c_provider_reference", rs.getString("provider_references"));
    row.put("c_transaction_reference", rs.getString("transaction_references"));  // <-- NEW
    return row;
}
```

#### Change 5: Javadoc — Add to Aggregations list

Update the class Javadoc to add `c_transaction_reference: GROUP_CONCAT` to the Aggregations list. The "11 identifying fields" count stays the same.

### 3.2 ConsolidatedRowPersister.java — Verify

Check that the persister reads from the Map by column name (not by position). If it does, no changes needed — the new key `c_transaction_reference` in the Map will be picked up automatically via INSERT_COLUMNS.

---

## 4. What NOT to Change

- **GROUP BY clause** — Must stay at 11 dimensions. This is the business consolidation logic.
- **SecuConsolidationQuery.java** — Already preserves `c_reference` via GROUP_CONCAT. No change.
- **MappingConfigurations.java** — Raw CSV parsing is correct.
- **StatementImporter.java / StatementConsolidator.java** — No changes.

---

## 5. Testing

### 5.1 Unit Tests

```bash
cd gam-plugins/statement-importer
mvn test
```

### 5.2 Verify Consolidation Unchanged

```sql
-- Row count must not change
SELECT COUNT(*) FROM app_fd_bank_total_trx WHERE c_statement_id = '<id>';
-- Before and after: same count
```

### 5.3 Verify References Preserved

```sql
-- Securities-related consolidated rows now have concatenated references
SELECT c_payment_description, c_payment_amount,
       c_transaction_reference, c_provider_reference
FROM app_fd_bank_total_trx
WHERE c_statement_id = '<id>'
  AND c_payment_description LIKE 'Securities%'
ORDER BY c_payment_date;
```

**Expected examples:**
- "Securities sell (ADBE)": c_transaction_reference = "900727947,900727952,900727956,..." (7 refs)
- "Securities buy (HLMBK095034FA)": c_transaction_reference = "903745878" (single ref)
- "Securities commission fee (HLMBK095034FA)": c_transaction_reference = "903745878" (same ref)

### 5.4 Verify Secu References (no change, confirm existing)

```sql
SELECT c_reference, c_ticker, c_type
FROM app_fd_secu_total_trx
WHERE c_statement_id = '<secu_id>'
LIMIT 5;
```

**Expected:** `c_reference` already contains GROUP_CONCAT'd references.

---

## 6. Downstream Impact

The `c_transaction_reference` field on F01.03 will contain comma-separated reference numbers matching the provider_reference pattern. This serves as:

1. **Audit trail** — traces which raw bank rows were consolidated
2. **Cross-reference** — the same reference numbers appear on the secu side (F01.04.c_reference, also GROUP_CONCAT'd)
3. **NOT the primary pairing key** — pairing at the consolidated level uses ticker + date + type, because both sides consolidate the same underlying executions into single rows

The rows-enrichment plugin can store this as `source_reference` on F01.05 for traceability.
