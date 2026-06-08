package com.fiscaladmin.gam.statementimporter.consolidation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Unit tests for bank transaction consolidation.
 * <p>
 * All tests are self-contained using synthetic raw rows inserted via JDBC.
 * No external CSV files required.
 */
public class BankConsolidationTest extends ConsolidationTestBase {

    private static final String STATEMENT_ID = "TEST-BANK-CONSOL";

    private int nextTxnId = 1;

    @Before
    public void setUp() throws Exception {
        setUpDatabase();
        nextTxnId = 1;
    }

    @After
    public void tearDown() throws Exception {
        tearDownDatabase();
    }

    // -------------------------------------------------------------------------
    // Helper: insert a raw bank row
    // -------------------------------------------------------------------------

    private void insertRawBankRow(String statementId, String accountNumber, String docNr,
            String paymentDate, String otherSideAccount, String otherSideName,
            String otherSideBank, String dC, String paymentAmount,
            String paymentDescription, String transactionFee, String currency,
            String customerId, String otherSideBic,
            String transactionRef, String providerRef) throws SQLException {
        String sql = "INSERT INTO app_fd_bank_account_trx ("
                + "id, c_statement_id, c_transaction_id, "
                + "c_account_number, c_document_nr, c_payment_date, "
                + "c_other_side_account, c_other_side_name, c_other_side_bank, "
                + "c_d_c, c_payment_amount, c_reference_number, "
                + "c_archival_number, c_payment_description, c_transaction_fee, "
                + "c_currency, c_customer_id, c_other_side_bic, "
                + "c_initiator, c_transaction_reference, c_provider_reference, "
                + "dateCreated, createdBy"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = con.prepareStatement(sql)) {
            String txnId = String.valueOf(nextTxnId++);
            ps.setString(1, UUID.randomUUID().toString());
            ps.setString(2, statementId);
            ps.setString(3, txnId);
            ps.setString(4, accountNumber);
            ps.setString(5, docNr);
            ps.setString(6, paymentDate);
            ps.setString(7, otherSideAccount);
            ps.setString(8, otherSideName);
            ps.setString(9, otherSideBank);
            ps.setString(10, dC);
            ps.setString(11, paymentAmount);
            ps.setString(12, "");
            ps.setString(13, "");
            ps.setString(14, paymentDescription);
            ps.setString(15, transactionFee);
            ps.setString(16, currency);
            ps.setString(17, customerId);
            ps.setString(18, otherSideBic);
            ps.setString(19, "");
            ps.setString(20, transactionRef);
            ps.setString(21, providerRef);
            ps.setString(22, "2026-01-15");
            ps.setString(23, "system");
            ps.executeUpdate();
        }
    }

    /**
     * Insert a raw bank row with default values for non-key fields.
     * Varying only the fields under test keeps tests concise.
     */
    private void insertDefaultRow(String statementId,
            String paymentAmount, String transactionFee,
            String transactionRef, String providerRef) throws SQLException {
        insertRawBankRow(statementId,
                "EE123456", "DOC001", "2026-01-15",
                "DE999999", "Acme Corp", "DEUTDEFF",
                "D", paymentAmount,
                "Wire transfer", transactionFee, "EUR",
                "CUST001", "DEUTDEFFXXX",
                transactionRef, providerRef);
    }

    /**
     * Insert a raw bank row with a specific varying dimension (overrides one GROUP BY field).
     */
    private void insertRowWithOverride(String statementId, String field, String value,
            String paymentAmount, String transactionFee,
            String transactionRef, String providerRef) throws SQLException {
        String accountNumber = "EE123456";
        String docNr = "DOC001";
        String paymentDate = "2026-01-15";
        String otherSideAccount = "DE999999";
        String otherSideName = "Acme Corp";
        String otherSideBank = "DEUTDEFF";
        String dC = "D";
        String paymentDescription = "Wire transfer";
        String currency = "EUR";
        String customerId = "CUST001";
        String otherSideBic = "DEUTDEFFXXX";

        switch (field) {
            case "payment_date": paymentDate = value; break;
            case "d_c": dC = value; break;
            case "payment_description": paymentDescription = value; break;
            case "currency": currency = value; break;
            default: throw new IllegalArgumentException("Unknown field: " + field);
        }

        insertRawBankRow(statementId, accountNumber, docNr, paymentDate,
                otherSideAccount, otherSideName, otherSideBank, dC, paymentAmount,
                paymentDescription, transactionFee, currency, customerId, otherSideBic,
                transactionRef, providerRef);
    }

    // -------------------------------------------------------------------------
    // Helper: execute GROUP BY
    // -------------------------------------------------------------------------

    private List<Map<String, String>> executeGroupBy(String statementId) throws SQLException {
        List<Map<String, String>> results = new ArrayList<>();
        try (PreparedStatement ps = con.prepareStatement(BankConsolidationQuery.GROUP_BY_SQL)) {
            ps.setString(1, statementId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(BankConsolidationQuery.readRow(rs));
                }
            }
        }
        return results;
    }

    // =========================================================================
    // GROUP BY correctness (5 tests)
    // =========================================================================

    @Test
    public void sameGroupMergesIntoOneRow() throws Exception {
        // 3 rows identical on all 11 GROUP BY dimensions → 1 consolidated row
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "PREF1");
        insertDefaultRow(STATEMENT_ID, "200.00", "2.00", "TREF2", "PREF2");
        insertDefaultRow(STATEMENT_ID, "50.00", "0.50", "TREF3", "PREF3");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals("3 identical-dimension rows should merge into 1", 1, consolidated.size());
    }

    @Test
    public void differentPaymentDateProducesSeparateRows() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "PREF1");
        insertRowWithOverride(STATEMENT_ID, "payment_date", "2026-01-16",
                "200.00", "2.00", "TREF2", "PREF2");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals("Different payment_date → separate rows", 2, consolidated.size());
    }

    @Test
    public void differentDebitCreditProducesSeparateRows() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "PREF1");
        insertRowWithOverride(STATEMENT_ID, "d_c", "C",
                "200.00", "2.00", "TREF2", "PREF2");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals("Different d_c → separate rows", 2, consolidated.size());
    }

    @Test
    public void differentDescriptionProducesSeparateRows() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "PREF1");
        insertRowWithOverride(STATEMENT_ID, "payment_description", "Salary payment",
                "200.00", "2.00", "TREF2", "PREF2");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals("Different description → separate rows", 2, consolidated.size());
    }

    @Test
    public void differentCurrencyProducesSeparateRows() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "PREF1");
        insertRowWithOverride(STATEMENT_ID, "currency", "USD",
                "200.00", "2.00", "TREF2", "PREF2");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals("Different currency → separate rows", 2, consolidated.size());
    }

    // =========================================================================
    // Aggregation (3 tests)
    // =========================================================================

    @Test
    public void amountsSummedCorrectly() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.50", "0.00", "T1", "P1");
        insertDefaultRow(STATEMENT_ID, "200.75", "0.00", "T2", "P2");
        insertDefaultRow(STATEMENT_ID, "50.25", "0.00", "T3", "P3");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals(1, consolidated.size());
        assertEquals("351.50", consolidated.get(0).get("c_payment_amount"));
    }

    @Test
    public void feesSummedCorrectly() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.50", "T1", "P1");
        insertDefaultRow(STATEMENT_ID, "200.00", "2.75", "T2", "P2");
        insertDefaultRow(STATEMENT_ID, "300.00", "0.25", "T3", "P3");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals(1, consolidated.size());
        assertEquals("4.50", consolidated.get(0).get("c_transaction_fee"));
    }

    @Test
    public void amountRoundingToTwoDecimals() throws Exception {
        // 33.33 + 33.33 + 33.34 = 100.00 (exact)
        // 10.10 + 10.10 + 10.10 = 30.30 (exact with DECIMAL(15,2))
        insertDefaultRow(STATEMENT_ID, "10.10", "0.00", "T1", "P1");
        insertDefaultRow(STATEMENT_ID, "10.10", "0.00", "T2", "P2");
        insertDefaultRow(STATEMENT_ID, "10.10", "0.00", "T3", "P3");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals(1, consolidated.size());
        String amount = consolidated.get(0).get("c_payment_amount");
        assertEquals("30.30", amount);
    }

    // =========================================================================
    // GROUP_CONCAT (3 tests)
    // =========================================================================

    @Test
    public void providerReferencesConcatenated() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "T1", "PREF1");
        insertDefaultRow(STATEMENT_ID, "200.00", "2.00", "T2", "PREF2");
        insertDefaultRow(STATEMENT_ID, "300.00", "3.00", "T3", "PREF3");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals(1, consolidated.size());
        assertEquals("PREF1,PREF2,PREF3", consolidated.get(0).get("c_provider_reference"));
    }

    @Test
    public void transactionReferencesConcatenated() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "P1");
        insertDefaultRow(STATEMENT_ID, "200.00", "2.00", "TREF2", "P2");
        insertDefaultRow(STATEMENT_ID, "300.00", "3.00", "TREF3", "P3");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals(1, consolidated.size());
        assertEquals("TREF1,TREF2,TREF3", consolidated.get(0).get("c_transaction_reference"));
    }

    @Test
    public void singleRowReferenceNotConcatenated() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "PREF1");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals(1, consolidated.size());
        assertEquals("TREF1", consolidated.get(0).get("c_transaction_reference"));
        assertEquals("PREF1", consolidated.get(0).get("c_provider_reference"));
    }

    // =========================================================================
    // readRow() mapping (1 test)
    // =========================================================================

    @Test
    public void readRowReturnsAllExpectedKeys() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "PREF1");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        assertEquals(1, consolidated.size());
        Map<String, String> row = consolidated.get(0);

        // 11 GROUP BY columns + 4 aggregated columns = 15 keys
        assertEquals(15, row.size());
        assertNotNull(row.get("c_account_number"));
        assertNotNull(row.get("c_document_nr"));
        assertNotNull(row.get("c_payment_date"));
        assertNotNull(row.get("c_other_side_account"));
        assertNotNull(row.get("c_other_side_name"));
        assertNotNull(row.get("c_other_side_bank"));
        assertNotNull(row.get("c_d_c"));
        assertNotNull(row.get("c_payment_description"));
        assertNotNull(row.get("c_currency"));
        assertNotNull(row.get("c_customer_id"));
        assertNotNull(row.get("c_other_side_bic"));
        assertNotNull(row.get("c_payment_amount"));
        assertNotNull(row.get("c_transaction_fee"));
        assertNotNull(row.get("c_provider_reference"));
        assertNotNull(row.get("c_transaction_reference"));
    }

    // =========================================================================
    // Statement filter (1 test)
    // =========================================================================

    @Test
    public void onlyMatchingStatementIdConsolidated() throws Exception {
        // Insert rows for two different statements
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "T1", "P1");
        insertDefaultRow(STATEMENT_ID, "200.00", "2.00", "T2", "P2");
        insertDefaultRow("OTHER-STMT", "500.00", "5.00", "T3", "P3");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        // Only STATEMENT_ID rows should be included
        assertEquals(1, consolidated.size());
        assertEquals("300.00", consolidated.get(0).get("c_payment_amount"));
    }

    // =========================================================================
    // End-to-end with ConsolidatedRowPersister (2 tests)
    // =========================================================================

    @Test
    public void consolidatedRowsPersistedWithReferences() throws Exception {
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "TREF1", "PREF1");
        insertDefaultRow(STATEMENT_ID, "200.00", "2.00", "TREF2", "PREF2");
        insertDefaultRow(STATEMENT_ID, "300.00", "3.00", "TREF3", "PREF3");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);
        assertEquals(1, consolidated.size());

        int inserted = ConsolidatedRowPersister.persist(
                consolidated, STATEMENT_ID, "STMT2026",
                BankConsolidationQuery.INSERT_SQL,
                BankConsolidationQuery.INSERT_COLUMNS,
                BankConsolidationQuery.TARGET_TABLE,
                con);

        assertEquals(1, inserted);

        // Verify both reference fields in target table
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_provider_reference, c_transaction_reference, c_statement_reference "
                + "FROM app_fd_bank_total_trx WHERE c_statement_id = ?")) {
            ps.setString(1, STATEMENT_ID);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("PREF1,PREF2,PREF3", rs.getString("c_provider_reference"));
                assertEquals("TREF1,TREF2,TREF3", rs.getString("c_transaction_reference"));
                assertEquals("STMT2026.001", rs.getString("c_statement_reference"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void persistedRowsHaveCorrectStatus() throws Exception {
        // Insert 2 groups so we get 2 consolidated rows
        insertDefaultRow(STATEMENT_ID, "100.00", "1.00", "T1", "P1");
        insertRowWithOverride(STATEMENT_ID, "d_c", "C",
                "200.00", "2.00", "T2", "P2");

        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);
        assertEquals(2, consolidated.size());

        ConsolidatedRowPersister.persist(
                consolidated, STATEMENT_ID, "STMT2026",
                BankConsolidationQuery.INSERT_SQL,
                BankConsolidationQuery.INSERT_COLUMNS,
                BankConsolidationQuery.TARGET_TABLE,
                con);

        // All rows should have status='new'
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COUNT(*) FROM app_fd_bank_total_trx WHERE c_status != 'new'")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals("All consolidated rows should have status=new", 0, rs.getInt(1));
            }
        }
    }

    // =========================================================================
    // Structural (2 tests)
    // =========================================================================

    @Test
    public void insertColumnsCountMatchesPlaceholders() {
        // Count '?' placeholders in INSERT_SQL
        long placeholderCount = BankConsolidationQuery.INSERT_SQL.chars()
                .filter(c -> c == '?').count();

        assertEquals("INSERT_COLUMNS size must match placeholder count",
                BankConsolidationQuery.INSERT_COLUMNS.size(), (int) placeholderCount);
    }

    @Test
    public void insertSqlTableMatchesTargetTable() {
        assertTrue("INSERT_SQL should reference TARGET_TABLE",
                BankConsolidationQuery.INSERT_SQL.contains(BankConsolidationQuery.TARGET_TABLE));
    }
}
