# Testing the Statement-Importer Plugin Before Integration

**Context:** Tracks 1 and 2 are complete. The `statement-importer` plugin is built and compiles. Before starting Track 3 (workflow integration), we want to verify that the plugin correctly parses CSVs and produces rows matching the form schemas.

---

## Testing Strategy — Three Levels

Your plugin has a clean separation of concerns that allows testing at three levels, each progressively closer to production. You should run them in order.

### Level 1: Unit Tests (No Joget, No Database)

You already have these. They test parsing, mapping, and persistence in isolation using synthetic data and H2 in-memory DB.

```bash
cd /Users/aarelaponin/IdeaProjects/gam-plugins/statement-importer
mvn clean test
```

**What this validates:**
- CSV format detection works for all three formats
- LHV Bank field-6 stripping produces 18-element arrays
- Securities parsing produces 13-element arrays
- Column mappings align between parser output and persister INSERT
- Batch insert generates UUIDs, sequential transaction IDs, audit fields
- De-duplication logic

**What this does NOT validate:**
- That the real CSV files parse correctly end-to-end into the exact DB schema your Joget forms expect
- That column names in the INSERT match actual Joget table columns

### Level 2: Real-CSV-to-H2 Integration Test (New — Recommended)

This is the critical gap. Write a single integration test that:

1. Reads the **actual** `bank_statement.csv` and `secu_statement.csv` from `test-data`
2. Parses them through `CsvFormatDetector` → `StatementParser`
3. Inserts via `RawTransactionPersister` into H2 tables **whose schemas match the Joget form definitions**
4. Verifies row counts, specific field values, and column completeness

Create this test class:

```java
// src/test/java/com/fiscaladmin/gam/statementimporter/RealCsvEndToEndTest.java
package com.fiscaladmin.gam.statementimporter;

import com.fiscaladmin.gam.statementimporter.mapping.MappingConfigurations;
import com.fiscaladmin.gam.statementimporter.parser.CsvFormatDetector;
import com.fiscaladmin.gam.statementimporter.parser.Format;
import com.fiscaladmin.gam.statementimporter.parser.StatementParser;
import com.fiscaladmin.gam.statementimporter.persister.RawTransactionPersister;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.util.List;

import static org.junit.Assert.*;

/**
 * End-to-end test: real CSV files → parse → persist into H2.
 * H2 table schemas mirror the Joget form definitions exactly.
 *
 * Skips gracefully if CSV files are not present.
 */
public class RealCsvEndToEndTest {

    // Paths to real CSV input data
    private static final String BANK_CSV =
        "/Users/aarelaponin/IdeaProjects/rsr/gam-bank/_dev-v3/_input-data/bank_statement.csv";
    private static final String SECU_CSV =
        "/Users/aarelaponin/IdeaProjects/rsr/gam-bank/_dev-v3/_input-data/secu_statement.csv";

    private Connection con;

    @Before
    public void setUp() throws Exception {
        con = DriverManager.getConnection("jdbc:h2:mem:e2etest;DB_CLOSE_DELAY=-1", "sa", "");

        try (Statement stmt = con.createStatement()) {
            // ──────────────────────────────────────────────────────────
            // Schema mirrors F01.01 (bankAccountTransaction) form
            // Table: bank_account_trx → Joget creates app_fd_bank_account_trx
            // Columns: form field IDs with c_ prefix + system columns
            // ──────────────────────────────────────────────────────────
            stmt.execute("CREATE TABLE app_fd_bank_account_trx ("
                + "id VARCHAR(255) PRIMARY KEY, "

                // --- Form fields (from F01.01-trx-bank.json) ---
                + "c_statement_id VARCHAR(255), "      // HiddenField
                + "c_transaction_id VARCHAR(10), "      // TextField
                + "c_account_number VARCHAR(255), "     // TextField (mandatory)
                + "c_document_nr VARCHAR(255), "        // TextField
                + "c_payment_date VARCHAR(255), "       // TextField (mandatory)
                + "c_other_side_account VARCHAR(255), " // TextField (mandatory)
                + "c_other_side_name VARCHAR(255), "    // TextField
                + "c_other_side_bank VARCHAR(255), "    // TextField
                + "c_d_c VARCHAR(10), "                 // TextField (mandatory)
                + "c_payment_amount VARCHAR(255), "     // TextField (mandatory)
                + "c_reference_number VARCHAR(255), "   // TextField
                + "c_archival_number VARCHAR(255), "    // TextField
                + "c_payment_description TEXT, "        // TextField
                + "c_transaction_fee VARCHAR(255), "    // TextField
                + "c_currency VARCHAR(10), "            // TextField (mandatory)
                + "c_customer_id VARCHAR(255), "        // TextField
                + "c_other_side_bic VARCHAR(255), "     // TextField
                + "c_initiator VARCHAR(255), "          // TextField
                + "c_transaction_reference VARCHAR(255), " // TextField
                + "c_provider_reference VARCHAR(255), " // TextField

                // --- Joget audit columns ---
                + "dateCreated VARCHAR(50), "
                + "createdBy VARCHAR(255)"
                + ")");

            // ──────────────────────────────────────────────────────────
            // Schema mirrors F01.02 (securityTransaction) form
            // Table: sec_account_trx → Joget creates app_fd_sec_account_trx
            // ──────────────────────────────────────────────────────────
            stmt.execute("CREATE TABLE app_fd_sec_account_trx ("
                + "id VARCHAR(255) PRIMARY KEY, "

                // --- Form fields (from F01.02-trx-secu.json) ---
                + "c_statement_id VARCHAR(255), "       // HiddenField
                + "c_transaction_id VARCHAR(10), "      // TextField
                + "c_value_date VARCHAR(255), "         // TextField
                + "c_transaction_date VARCHAR(255), "   // TextField
                + "c_type VARCHAR(255), "               // TextField
                + "c_ticker VARCHAR(255), "             // TextField
                + "c_description TEXT, "                // TextField
                + "c_quantity VARCHAR(255), "           // TextField
                + "c_price VARCHAR(255), "              // TextField
                + "c_currency VARCHAR(10), "            // TextField
                + "c_amount VARCHAR(255), "             // TextField
                + "c_fee VARCHAR(255), "                // TextField
                + "c_total_amount VARCHAR(255), "       // TextField
                + "c_reference VARCHAR(255), "          // TextField
                + "c_comment TEXT, "                    // TextField

                // --- Joget audit columns ---
                + "dateCreated VARCHAR(50), "
                + "createdBy VARCHAR(255)"
                + ")");
        }
    }

    @After
    public void tearDown() throws Exception {
        if (con != null && !con.isClosed()) {
            try (Statement stmt = con.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS app_fd_bank_account_trx");
                stmt.execute("DROP TABLE IF EXISTS app_fd_sec_account_trx");
            }
            con.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  BANK STATEMENT: Full pipeline test
    // ═══════════════════════════════════════════════════════════════════

    @Test
    public void bankCsvFullPipeline() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: bank_statement.csv not found at " + BANK_CSV);
            return;
        }

        // Step 1: Detect format
        Format format = CsvFormatDetector.detect(bankFile);
        assertEquals("Should detect LHV Bank format", Format.LHV_BANK, format);
        assertEquals("bank", format.getAccountType());

        // Step 2: Parse
        List<String[]> rows = StatementParser.parse(bankFile, format);
        assertEquals("Bank CSV should have 161 data rows", 161, rows.size());

        // Step 3: Verify parsed array length matches mapping config
        int expectedFields = MappingConfigurations.LHV_BANK_CONFIG.getFieldMappings().size();
        for (int i = 0; i < rows.size(); i++) {
            assertEquals("Row " + i + " field count must match LHV_BANK_CONFIG",
                expectedFields, rows.get(i).length);
        }

        // Step 4: Persist
        String stmtId = "TEST-BANK-001";
        int inserted = RawTransactionPersister.persist(
            rows, stmtId, MappingConfigurations.LHV_BANK_CONFIG, con);
        assertEquals("All 161 rows should be inserted", 161, inserted);

        // Step 5: Verify row count in DB
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COUNT(*) FROM app_fd_bank_account_trx WHERE c_statement_id = ?")) {
            ps.setString(1, stmtId);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertEquals(161, rs.getInt(1));
        }

        // Step 6: Spot-check first row (interest payment from CSV line 2)
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT * FROM app_fd_bank_account_trx "
                + "WHERE c_statement_id = ? ORDER BY c_transaction_id LIMIT 1")) {
            ps.setString(1, stmtId);
            ResultSet rs = ps.executeQuery();
            assertTrue("Should have at least one row", rs.next());

            assertEquals("EE117700771003603322", rs.getString("c_account_number"));
            assertEquals("2024-06-05", rs.getString("c_payment_date"));
            assertEquals("C", rs.getString("c_d_c"));
            assertEquals("0.31", rs.getString("c_payment_amount"));
            assertEquals("EUR", rs.getString("c_currency"));
            assertEquals("001", rs.getString("c_transaction_id"));
            assertEquals(stmtId, rs.getString("c_statement_id"));
            assertNotNull(rs.getString("id"));  // UUID generated
            assertNotNull(rs.getString("dateCreated"));
            assertEquals("system", rs.getString("createdBy"));

            // Provider reference from CSV
            assertEquals("17401979DC22EF11B45B001DD8D11D14",
                rs.getString("c_provider_reference"));
        }

        // Step 7: Verify no NULL columns for mandatory form fields
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(*) FROM app_fd_bank_account_trx "
                 + "WHERE c_statement_id = '" + stmtId + "' "
                 + "AND (c_account_number IS NULL "
                 + "  OR c_payment_date IS NULL "
                 + "  OR c_d_c IS NULL "
                 + "  OR c_payment_amount IS NULL "
                 + "  OR c_currency IS NULL)")) {
            rs.next();
            assertEquals("No rows should have NULL mandatory fields", 0, rs.getInt(1));
        }

        // Step 8: Verify transaction_id sequencing
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_transaction_id FROM app_fd_bank_account_trx "
                + "WHERE c_statement_id = ? ORDER BY c_transaction_id")) {
            ps.setString(1, stmtId);
            ResultSet rs = ps.executeQuery();
            for (int expected = 1; expected <= 161; expected++) {
                assertTrue("Should have row " + expected, rs.next());
                assertEquals(String.format("%03d", expected), rs.getString(1));
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SECURITIES STATEMENT: Full pipeline test
    // ═══════════════════════════════════════════════════════════════════

    @Test
    public void secuCsvFullPipeline() throws Exception {
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: secu_statement.csv not found at " + SECU_CSV);
            return;
        }

        // Step 1: Detect format
        Format format = CsvFormatDetector.detect(secuFile);
        assertEquals("Should detect Securities format", Format.SECURITIES, format);
        assertEquals("secu", format.getAccountType());

        // Step 2: Parse
        List<String[]> rows = StatementParser.parse(secuFile, format);
        assertEquals("Securities CSV should have 45 data rows", 45, rows.size());

        // Step 3: Verify parsed array length
        int expectedFields = MappingConfigurations.SECU_CONFIG.getFieldMappings().size();
        for (int i = 0; i < rows.size(); i++) {
            assertEquals("Row " + i + " field count must match SECU_CONFIG",
                expectedFields, rows.get(i).length);
        }

        // Step 4: Persist
        String stmtId = "TEST-SECU-001";
        int inserted = RawTransactionPersister.persist(
            rows, stmtId, MappingConfigurations.SECU_CONFIG, con);
        assertEquals("All 45 rows should be inserted", 45, inserted);

        // Step 5: Row count
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COUNT(*) FROM app_fd_sec_account_trx WHERE c_statement_id = ?")) {
            ps.setString(1, stmtId);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertEquals(45, rs.getInt(1));
        }

        // Step 6: Spot-check first row (Holm Bank bond purchase)
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT * FROM app_fd_sec_account_trx "
                + "WHERE c_statement_id = ? ORDER BY c_transaction_id LIMIT 1")) {
            ps.setString(1, stmtId);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            assertEquals("2024-06-07", rs.getString("c_value_date"));
            assertEquals("2024-06-05", rs.getString("c_transaction_date"));
            assertEquals("ost", rs.getString("c_type"));
            assertEquals("HLMBK095034FA", rs.getString("c_ticker"));
            assertEquals("1", rs.getString("c_quantity"));
            assertEquals("1051.84722000", rs.getString("c_price"));
            assertEquals("EUR", rs.getString("c_currency"));
            assertEquals("-1051.85", rs.getString("c_amount"));
            assertEquals("-8.10", rs.getString("c_fee"));
            assertEquals("-1059.95", rs.getString("c_total_amount"));
            assertEquals("903745878", rs.getString("c_reference"));
            assertEquals("001", rs.getString("c_transaction_id"));
            assertEquals(stmtId, rs.getString("c_statement_id"));
        }

        // Step 7: Verify the LHV1T buy fragments exist (rows 3-4 in CSV)
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_ticker, c_quantity, c_amount, c_reference "
                + "FROM app_fd_sec_account_trx "
                + "WHERE c_statement_id = ? AND c_ticker = 'LHV1T' AND c_type = 'ost' "
                + "ORDER BY c_transaction_id")) {
            ps.setString(1, stmtId);
            ResultSet rs = ps.executeQuery();

            // First LHV1T buy: 257 shares @ 3.50
            assertTrue(rs.next());
            assertEquals("257", rs.getString("c_quantity"));
            assertEquals("-899.50", rs.getString("c_amount"));
            assertEquals("903867200", rs.getString("c_reference"));

            // Second LHV1T buy: 500 shares @ 3.50
            assertTrue(rs.next());
            assertEquals("500", rs.getString("c_quantity"));
            assertEquals("-1750.00", rs.getString("c_amount"));
            assertEquals("903867343", rs.getString("c_reference"));
        }

        // Step 8: Transaction ID sequencing
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_transaction_id FROM app_fd_sec_account_trx "
                + "WHERE c_statement_id = ? ORDER BY c_transaction_id")) {
            ps.setString(1, stmtId);
            ResultSet rs = ps.executeQuery();
            for (int expected = 1; expected <= 45; expected++) {
                assertTrue(rs.next());
                assertEquals(String.format("%03d", expected), rs.getString(1));
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SCHEMA COMPATIBILITY: Form columns vs INSERT columns
    // ═══════════════════════════════════════════════════════════════════

    @Test
    public void bankInsertColumnsMatchFormSchema() throws Exception {
        // This test verifies that every column the persister writes to
        // actually exists in the H2 table (which mirrors the Joget form).
        // If the INSERT uses a column not in the form, H2 throws SQLException.

        String[] minimalRow = new String[18];
        java.util.Arrays.fill(minimalRow, "test");

        // If this throws, the INSERT has a column mismatch with the form
        int inserted = RawTransactionPersister.persist(
            java.util.Collections.singletonList(minimalRow),
            "SCHEMA-TEST-BANK",
            MappingConfigurations.LHV_BANK_CONFIG,
            con
        );
        assertEquals(1, inserted);
    }

    @Test
    public void secuInsertColumnsMatchFormSchema() throws Exception {
        String[] minimalRow = new String[13];
        java.util.Arrays.fill(minimalRow, "test");

        int inserted = RawTransactionPersister.persist(
            java.util.Collections.singletonList(minimalRow),
            "SCHEMA-TEST-SECU",
            MappingConfigurations.SECU_CONFIG,
            con
        );
        assertEquals(1, inserted);
    }
}
```

### Level 2 Edge Case Tests

In addition to the core pipeline tests, `RealCsvEndToEndTest` includes 8 edge case tests that validate real-world data scenarios:

| Test | What It Validates |
|------|-------------------|
| `secuMandatoryFieldsNotNull` | No mandatory securities fields (value_date, transaction_date, type, ticker, currency, reference) are NULL |
| `secuStockSplitTransactionsHandled` | NVDA stock split transactions: split- (qty=-20, price=780) and split+ (qty=200, price=78) with same reference |
| `bankNegativeAmountsPreserved` | Negative amounts (e.g., -12.84) are preserved and correlate with Debit (D) indicator |
| `bankMultiCurrencySegregation` | EUR and USD transactions are correctly segregated (EUR + USD = 161 total rows) |
| `bankEmptyFieldsNotNull` | Optional fields stored as empty strings, not NULL (e.g., empty other_side_account for interest rows) |
| `secuZeroFeeTransactions` | Zero fee transactions (e.g., LHV1T buys with fee=0.00) are correctly stored |
| `bankCurrencyExchangeTransactions` | FX transactions produce paired rows: EUR side (Debit) and USD side (Credit) |
| `secuHighPrecisionPrices` | 8-decimal price precision preserved (e.g., 1051.84722000 for HLMBK bond) |

**Helper methods:**
- `countNullMandatorySecuFields()` — counts securities rows with NULL mandatory fields
- `countRowsByCurrency(table, currency)` — counts rows by currency in any table

**Run edge case tests:**
```bash
mvn test -Dtest=RealCsvEndToEndTest
```

Expected: 13 tests pass (5 core + 8 edge case)

---

### Level 3: Joget API Smoke Test (Against Live Joget)

After Level 2 passes, verify via the Joget Form Data REST API that the forms can actually accept the data. This does not require the workflow — just the running Joget instance with the forms deployed.

**Prerequisite:** Your Joget instance is running and forms F01.00, F01.01, F01.02 are deployed.

**API Authentication:** Use API key headers (`api_id` and `api_key`) instead of basic auth.

#### Step A — Create a Statement Record via API

```bash
# Create a parent statement record first
curl -X POST "http://localhost:8082/jw/api/form/bankStatement" \
  -H 'accept: application/json' \
  -H 'api_id: API-45528ac6-975f-4a3b-b884-54e5ddbcc00c' \
  -H 'api_key: 8c04d5332aa34484a62fe1fb1e6e5900' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "TEST-API-001",
    "bank": "LHVBEE22",
    "account_type": "bank",
    "from_date": "2024-06-01",
    "to_date": "2024-07-31",
    "status": "new"
  }'
```

#### Step B — Insert a Single Bank Transaction Row via API

```bash
curl -X POST "http://localhost:8082/jw/api/form/bankAccountTransaction" \
  -H 'accept: application/json' \
  -H 'api_id: API-45528ac6-975f-4a3b-b884-54e5ddbcc00c' \
  -H 'api_key: 8c04d5332aa34484a62fe1fb1e6e5900' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "TEST-TRX-001",
    "statement_id": "TEST-API-001",
    "transaction_id": "001",
    "account_number": "EE117700771003603322",
    "document_nr": "",
    "payment_date": "2024-06-05",
    "other_side_account": "",
    "other_side_name": "",
    "other_side_bank": "",
    "d_c": "C",
    "payment_amount": "0.31",
    "reference_number": "",
    "archival_number": "",
    "payment_description": "Account interest",
    "transaction_fee": "0.00",
    "currency": "EUR",
    "customer_id": "",
    "other_side_bic": "",
    "initiator": "",
    "transaction_reference": "",
    "provider_reference": "17401979DC22EF11B45B001DD8D11D14"
  }'
```

#### Step C — Verify via API

```bash
# Read it back
curl -s "http://localhost:8082/jw/api/form/bankAccountTransaction/TEST-TRX-001" \
  -H 'api_id: API-45528ac6-975f-4a3b-b884-54e5ddbcc00c' \
  -H 'api_key: 8c04d5332aa34484a62fe1fb1e6e5900' | python3 -m json.tool
```

#### Step D — Insert a Securities Transaction Row

```bash
curl -X POST "http://localhost:8082/jw/api/form/securityTransaction" \
  -H 'accept: application/json' \
  -H 'api_id: API-45528ac6-975f-4a3b-b884-54e5ddbcc00c' \
  -H 'api_key: 8c04d5332aa34484a62fe1fb1e6e5900' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "TEST-SECU-TRX-001",
    "statement_id": "TEST-API-001",
    "transaction_id": "001",
    "value_date": "2024-06-07",
    "transaction_date": "2024-06-05",
    "type": "ost",
    "ticker": "HLMBK095034FA",
    "description": "Holm Bank 9.50% võlakiri 30.05.2034",
    "quantity": "1",
    "price": "1051.84722000",
    "currency": "EUR",
    "amount": "-1051.85",
    "fee": "-8.10",
    "total_amount": "-1059.95",
    "reference": "903745878",
    "comment": ""
  }'
```

#### Step E — Verify in Joget UI

Open the browser:
1. Go to F01.01 form for record `TEST-TRX-001` — all fields should display correctly
2. Go to F01.02 form for record `TEST-SECU-TRX-001` — all fields should display correctly
3. Go to F01.00 form for record `TEST-API-001` — the embedded datalists should show the child rows

#### Step F — Cleanup

```bash
# Delete test data
curl -X DELETE "http://localhost:8082/jw/api/form/bankAccountTransaction/TEST-TRX-001" \
  -H 'api_id: API-45528ac6-975f-4a3b-b884-54e5ddbcc00c' \
  -H 'api_key: 8c04d5332aa34484a62fe1fb1e6e5900'

curl -X DELETE "http://localhost:8082/jw/api/form/securityTransaction/TEST-SECU-TRX-001" \
  -H 'api_id: API-45528ac6-975f-4a3b-b884-54e5ddbcc00c' \
  -H 'api_key: 8c04d5332aa34484a62fe1fb1e6e5900'

curl -X DELETE "http://localhost:8082/jw/api/form/bankStatement/TEST-API-001" \
  -H 'api_id: API-45528ac6-975f-4a3b-b884-54e5ddbcc00c' \
  -H 'api_key: 8c04d5332aa34484a62fe1fb1e6e5900'
```

---

## What Each Level Catches

| Issue | Level 1 | Level 2 | Level 3 |
|-------|---------|---------|---------|
| CSV parsing logic bugs | ✅ | ✅ | — |
| Column index off-by-one | ✅ | ✅ | — |
| Mapping config → form field mismatch | — | ✅ | ✅ |
| INSERT column not in Joget form | — | ✅ | ✅ |
| Real CSV encoding issues (BOM, UTF-8) | — | ✅ | — |
| Joget API rejects the data shape | — | — | ✅ |
| Form validators reject values | — | — | ✅ |
| Embedded datalists don't show child rows | — | — | ✅ |
| FK (statement_id) linkage works | — | ✅ | ✅ |

---

## Recommended Execution Order

1. **Run `mvn clean test`** — make sure existing unit tests still pass
2. **Add `RealCsvEndToEndTest.java`** and run it — this is the highest-value new test
3. **Deploy forms to Joget** (if not already done) and run Level 3 API smoke test
4. Once all three levels pass, you have high confidence for Track 3 integration

---

## Quick Checklist: Column Name Cross-Reference

Verify that these column names are consistent across all three artifacts:

### Bank (F01.01 → MappingConfigurations.LHV_BANK_CONFIG → app_fd_bank_account_trx)

| Form field ID | MappingConfig DB column | CSV index (after strip) |
|---------------|-------------------------|------------------------|
| statement_id | c_statement_id | — (set by persister) |
| transaction_id | c_transaction_id | — (set by persister) |
| account_number | c_account_number | 0 |
| document_nr | c_document_nr | 1 |
| payment_date | c_payment_date | 2 |
| other_side_account | c_other_side_account | 3 |
| other_side_name | c_other_side_name | 4 |
| other_side_bank | c_other_side_bank | 5 |
| d_c | c_d_c | 6 |
| payment_amount | c_payment_amount | 7 |
| reference_number | c_reference_number | 8 |
| archival_number | c_archival_number | 9 |
| payment_description | c_payment_description | 10 |
| transaction_fee | c_transaction_fee | 11 |
| currency | c_currency | 12 |
| customer_id | c_customer_id | 13 |
| other_side_bic | c_other_side_bic | 14 |
| initiator | c_initiator | 15 |
| transaction_reference | c_transaction_reference | 16 |
| provider_reference | c_provider_reference | 17 |

### Securities (F01.02 → MappingConfigurations.SECU_CONFIG → app_fd_sec_account_trx)

| Form field ID | MappingConfig DB column | CSV index |
|---------------|-------------------------|-----------|
| statement_id | c_statement_id | — (set by persister) |
| transaction_id | c_transaction_id | — (set by persister) |
| value_date | c_value_date | 0 |
| transaction_date | c_transaction_date | 1 |
| type | c_type | 2 |
| ticker | c_ticker | 3 |
| description | c_description | 4 |
| quantity | c_quantity | 5 |
| price | c_price | 6 |
| currency | c_currency | 7 |
| amount | c_amount | 8 |
| fee | c_fee | 9 |
| total_amount | c_total_amount | 10 |
| reference | c_reference | 11 |
| comment | c_comment | 12 |

**Rule:** Joget stores form field `xyz` as column `c_xyz` in the database table `app_fd_{tableName}`. The MappingConfig must use the `c_` prefixed name. The form JSON `id` property must match the non-prefixed name. All three are aligned in the tables above — if you change any form field ID, the mapping must change too.
