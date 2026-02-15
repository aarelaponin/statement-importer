package com.fiscaladmin.gam.statementimporter.persister;

import com.fiscaladmin.gam.statementimporter.mapping.MappingConfigurations;
import com.fiscaladmin.gam.statementimporter.mapping.MappingConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link RawTransactionPersister}.
 * <p>
 * All tests use the package-private {@code persist(rows, statementId, config, connection)}
 * overload with an in-memory H2 database — no running Joget/MySQL instance required.
 */
public class RawTransactionPersisterTest {

    private Connection con;

    @Before
    public void setUp() throws Exception {
        con = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "sa", "");

        try (Statement stmt = con.createStatement()) {
            // Bank account transactions (LHV: 18 mapped + 5 system/audit = 23 columns)
            stmt.execute("CREATE TABLE IF NOT EXISTS app_fd_bank_account_trx ("
                    + "id VARCHAR(255) PRIMARY KEY, "
                    + "c_account_number VARCHAR(255), "
                    + "c_document_nr VARCHAR(255), "
                    + "c_payment_date VARCHAR(255), "
                    + "c_other_side_account VARCHAR(255), "
                    + "c_other_side_name VARCHAR(255), "
                    + "c_other_side_bank VARCHAR(255), "
                    + "c_d_c VARCHAR(10), "
                    + "c_payment_amount VARCHAR(255), "
                    + "c_reference_number VARCHAR(255), "
                    + "c_archival_number VARCHAR(255), "
                    + "c_payment_description TEXT, "
                    + "c_transaction_fee VARCHAR(255), "
                    + "c_currency VARCHAR(10), "
                    + "c_customer_id VARCHAR(255), "
                    + "c_other_side_bic VARCHAR(255), "
                    + "c_initiator VARCHAR(255), "
                    + "c_transaction_reference VARCHAR(255), "
                    + "c_provider_reference VARCHAR(255), "
                    + "c_transaction_id VARCHAR(10), "
                    + "c_statement_id VARCHAR(255), "
                    + "dateCreated VARCHAR(50), "
                    + "createdBy VARCHAR(255)"
                    + ")");

            // Securities account transactions (13 mapped + 5 system/audit = 18 columns)
            stmt.execute("CREATE TABLE IF NOT EXISTS app_fd_sec_account_trx ("
                    + "id VARCHAR(255) PRIMARY KEY, "
                    + "c_value_date VARCHAR(255), "
                    + "c_transaction_date VARCHAR(255), "
                    + "c_type VARCHAR(255), "
                    + "c_ticker VARCHAR(255), "
                    + "c_description TEXT, "
                    + "c_quantity VARCHAR(255), "
                    + "c_price VARCHAR(255), "
                    + "c_currency VARCHAR(10), "
                    + "c_amount VARCHAR(255), "
                    + "c_fee VARCHAR(255), "
                    + "c_total_amount VARCHAR(255), "
                    + "c_reference VARCHAR(255), "
                    + "c_comment TEXT, "
                    + "c_transaction_id VARCHAR(10), "
                    + "c_statement_id VARCHAR(255), "
                    + "dateCreated VARCHAR(50), "
                    + "createdBy VARCHAR(255)"
                    + ")");
        }
    }

    @After
    public void tearDown() throws Exception {
        try (Statement stmt = con.createStatement()) {
            stmt.execute("DELETE FROM app_fd_bank_account_trx");
            stmt.execute("DELETE FROM app_fd_sec_account_trx");
        }
        con.close();
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    /**
     * Creates an 18-element array matching the LHV_BANK_CONFIG layout
     * (after field-6 stripping by StatementParser).
     */
    private String[] bankRow(String accountNumber, String docNr, String paymentDate,
                             String amount, String currency, String providerRef) {
        String[] row = new String[18];
        Arrays.fill(row, "");
        row[0] = accountNumber;
        row[1] = docNr;
        row[2] = paymentDate;
        row[7] = amount;
        row[12] = currency;
        row[17] = providerRef;
        return row;
    }

    /**
     * Creates a 13-element array matching the SECU_CONFIG layout.
     */
    private String[] secuRow(String valueDate, String trxDate, String type,
                             String ticker, String amount, String currency, String reference) {
        String[] row = new String[13];
        Arrays.fill(row, "");
        row[0] = valueDate;
        row[1] = trxDate;
        row[2] = type;
        row[3] = ticker;
        row[7] = currency;
        row[8] = amount;
        row[11] = reference;
        return row;
    }

    private int countRows(String table) throws SQLException {
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private int countRowsWithStatementId(String table, String statementId) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COUNT(*) FROM " + table + " WHERE c_statement_id = ?")) {
            ps.setString(1, statementId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private String getColumnValue(String table, String column, String whereClause) throws SQLException {
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + column + " FROM " + table + " WHERE " + whereClause)) {
            if (rs.next()) return rs.getString(1);
            return null;
        }
    }

    private List<String> getColumnValues(String table, String column, String orderBy) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + column + " FROM " + table + " ORDER BY " + orderBy)) {
            while (rs.next()) {
                values.add(rs.getString(1));
            }
        }
        return values;
    }

    // -------------------------------------------------------------------------
    // Bank row insert tests
    // -------------------------------------------------------------------------

    @Test
    public void persistBankRowsInsertsCorrectCount() throws SQLException {
        List<String[]> rows = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF002"),
                bankRow("EE1234", "003", "2024-06-03", "300.00", "EUR", "REF003")
        );

        int inserted = RawTransactionPersister.persist(rows, "STMT001", MappingConfigurations.LHV_BANK_CONFIG, con);

        assertEquals(3, inserted);
        assertEquals(3, countRowsWithStatementId("app_fd_bank_account_trx", "STMT001"));
    }

    // -------------------------------------------------------------------------
    // Securities row insert tests
    // -------------------------------------------------------------------------

    @Test
    public void persistSecuRowsInsertsCorrectCount() throws SQLException {
        List<String[]> rows = Arrays.asList(
                secuRow("2024-06-01", "2024-06-01", "BUY", "AAPL", "5000.00", "USD", "SREF001"),
                secuRow("2024-06-02", "2024-06-02", "SELL", "MSFT", "3000.00", "USD", "SREF002")
        );

        int inserted = RawTransactionPersister.persist(rows, "STMT002", MappingConfigurations.SECU_CONFIG, con);

        assertEquals(2, inserted);
        assertEquals(2, countRowsWithStatementId("app_fd_sec_account_trx", "STMT002"));
    }

    // -------------------------------------------------------------------------
    // Statement ID tests
    // -------------------------------------------------------------------------

    @Test
    public void persistSetsStatementIdOnEveryRow() throws SQLException {
        List<String[]> rows = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF002"),
                bankRow("EE1234", "003", "2024-06-03", "300.00", "EUR", "REF003")
        );

        RawTransactionPersister.persist(rows, "STMT-ABC", MappingConfigurations.LHV_BANK_CONFIG, con);

        assertEquals(3, countRowsWithStatementId("app_fd_bank_account_trx", "STMT-ABC"));
    }

    // -------------------------------------------------------------------------
    // Transaction ID sequencing tests
    // -------------------------------------------------------------------------

    @Test
    public void persistSetsSequentialTransactionIds() throws SQLException {
        List<String[]> rows = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF002"),
                bankRow("EE1234", "003", "2024-06-03", "300.00", "EUR", "REF003")
        );

        RawTransactionPersister.persist(rows, "STMT001", MappingConfigurations.LHV_BANK_CONFIG, con);

        List<String> txIds = getColumnValues("app_fd_bank_account_trx", "c_transaction_id", "c_transaction_id");
        assertEquals(Arrays.asList("001", "002", "003"), txIds);
    }

    // -------------------------------------------------------------------------
    // UUID generation tests
    // -------------------------------------------------------------------------

    @Test
    public void persistGeneratesUniqueIds() throws SQLException {
        List<String[]> rows = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            rows.add(bankRow("EE1234", String.valueOf(i), "2024-06-01", "100.00", "EUR", "REF" + i));
        }

        RawTransactionPersister.persist(rows, "STMT001", MappingConfigurations.LHV_BANK_CONFIG, con);

        List<String> ids = getColumnValues("app_fd_bank_account_trx", "id", "id");
        assertEquals(5, ids.size());

        // All distinct
        Set<String> uniqueIds = new HashSet<>(ids);
        assertEquals(5, uniqueIds.size());

        // All match UUID format (36 chars with dashes: 8-4-4-4-12)
        for (String id : ids) {
            assertEquals(36, id.length());
            assertTrue("UUID should contain dashes", id.contains("-"));
        }
    }

    // -------------------------------------------------------------------------
    // Audit field tests
    // -------------------------------------------------------------------------

    @Test
    public void persistSetsDateCreatedAndCreatedBy() throws SQLException {
        List<String[]> rows = Collections.singletonList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001")
        );

        RawTransactionPersister.persist(rows, "STMT001", MappingConfigurations.LHV_BANK_CONFIG, con);

        String dateCreated = getColumnValue("app_fd_bank_account_trx", "dateCreated",
                "c_statement_id = 'STMT001'");
        String createdBy = getColumnValue("app_fd_bank_account_trx", "createdBy",
                "c_statement_id = 'STMT001'");

        assertNotNull(dateCreated);
        assertFalse(dateCreated.isEmpty());
        // Should be in yyyy-MM-dd HH:mm:ss format
        assertTrue("dateCreated should match timestamp format", dateCreated.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));

        assertNotNull(createdBy);
        assertEquals("system", createdBy);
    }

    // -------------------------------------------------------------------------
    // Field value mapping tests
    // -------------------------------------------------------------------------

    @Test
    public void persistMapsFieldValuesCorrectly() throws SQLException {
        String[] row = new String[18];
        row[0] = "EE117700771003603322";
        row[1] = "DOC123";
        row[2] = "2024-06-05";
        row[3] = "EE999";
        row[4] = "John Doe";
        row[5] = "SomeBank";
        row[6] = "C";
        row[7] = "100.50";
        row[8] = "REF001";
        row[9] = "ARCH001";
        row[10] = "Test payment";
        row[11] = "0.50";
        row[12] = "EUR";
        row[13] = "12345678";
        row[14] = "SWIFT123";
        row[15] = "Initiator";
        row[16] = "TXREF001";
        row[17] = "PROVREF001";

        RawTransactionPersister.persist(Collections.singletonList(row), "STMT-MAP",
                MappingConfigurations.LHV_BANK_CONFIG, con);

        String where = "c_statement_id = 'STMT-MAP'";
        assertEquals("EE117700771003603322", getColumnValue("app_fd_bank_account_trx", "c_account_number", where));
        assertEquals("DOC123", getColumnValue("app_fd_bank_account_trx", "c_document_nr", where));
        assertEquals("2024-06-05", getColumnValue("app_fd_bank_account_trx", "c_payment_date", where));
        assertEquals("EE999", getColumnValue("app_fd_bank_account_trx", "c_other_side_account", where));
        assertEquals("John Doe", getColumnValue("app_fd_bank_account_trx", "c_other_side_name", where));
        assertEquals("SomeBank", getColumnValue("app_fd_bank_account_trx", "c_other_side_bank", where));
        assertEquals("C", getColumnValue("app_fd_bank_account_trx", "c_d_c", where));
        assertEquals("100.50", getColumnValue("app_fd_bank_account_trx", "c_payment_amount", where));
        assertEquals("REF001", getColumnValue("app_fd_bank_account_trx", "c_reference_number", where));
        assertEquals("ARCH001", getColumnValue("app_fd_bank_account_trx", "c_archival_number", where));
        assertEquals("Test payment", getColumnValue("app_fd_bank_account_trx", "c_payment_description", where));
        assertEquals("0.50", getColumnValue("app_fd_bank_account_trx", "c_transaction_fee", where));
        assertEquals("EUR", getColumnValue("app_fd_bank_account_trx", "c_currency", where));
        assertEquals("12345678", getColumnValue("app_fd_bank_account_trx", "c_customer_id", where));
        assertEquals("SWIFT123", getColumnValue("app_fd_bank_account_trx", "c_other_side_bic", where));
        assertEquals("Initiator", getColumnValue("app_fd_bank_account_trx", "c_initiator", where));
        assertEquals("TXREF001", getColumnValue("app_fd_bank_account_trx", "c_transaction_reference", where));
        assertEquals("PROVREF001", getColumnValue("app_fd_bank_account_trx", "c_provider_reference", where));
    }

    @Test
    public void persistMapsSecuFieldValuesCorrectly() throws SQLException {
        String[] row = new String[13];
        row[0] = "2024-07-01";
        row[1] = "2024-06-28";
        row[2] = "BUY";
        row[3] = "AAPL";
        row[4] = "Apple Inc. purchase";
        row[5] = "10";
        row[6] = "185.50";
        row[7] = "USD";
        row[8] = "1855.00";
        row[9] = "5.25";
        row[10] = "1860.25";
        row[11] = "SECREF001";
        row[12] = "Test buy order";

        RawTransactionPersister.persist(Collections.singletonList(row), "STMT-SECU",
                MappingConfigurations.SECU_CONFIG, con);

        String where = "c_statement_id = 'STMT-SECU'";
        assertEquals("2024-07-01", getColumnValue("app_fd_sec_account_trx", "c_value_date", where));
        assertEquals("2024-06-28", getColumnValue("app_fd_sec_account_trx", "c_transaction_date", where));
        assertEquals("BUY", getColumnValue("app_fd_sec_account_trx", "c_type", where));
        assertEquals("AAPL", getColumnValue("app_fd_sec_account_trx", "c_ticker", where));
        assertEquals("Apple Inc. purchase", getColumnValue("app_fd_sec_account_trx", "c_description", where));
        assertEquals("10", getColumnValue("app_fd_sec_account_trx", "c_quantity", where));
        assertEquals("185.50", getColumnValue("app_fd_sec_account_trx", "c_price", where));
        assertEquals("USD", getColumnValue("app_fd_sec_account_trx", "c_currency", where));
        assertEquals("1855.00", getColumnValue("app_fd_sec_account_trx", "c_amount", where));
        assertEquals("5.25", getColumnValue("app_fd_sec_account_trx", "c_fee", where));
        assertEquals("1860.25", getColumnValue("app_fd_sec_account_trx", "c_total_amount", where));
        assertEquals("SECREF001", getColumnValue("app_fd_sec_account_trx", "c_reference", where));
        assertEquals("Test buy order", getColumnValue("app_fd_sec_account_trx", "c_comment", where));
    }

    // -------------------------------------------------------------------------
    // Empty/null field handling
    // -------------------------------------------------------------------------

    @Test
    public void persistHandlesEmptyFieldsAsEmptyStrings() throws SQLException {
        // Row where most fields are empty strings
        String[] row = new String[18];
        Arrays.fill(row, "");
        row[0] = "EE1234";  // only account_number populated

        RawTransactionPersister.persist(Collections.singletonList(row), "STMT-EMPTY",
                MappingConfigurations.LHV_BANK_CONFIG, con);

        assertEquals(1, countRowsWithStatementId("app_fd_bank_account_trx", "STMT-EMPTY"));

        String where = "c_statement_id = 'STMT-EMPTY'";
        assertEquals("EE1234", getColumnValue("app_fd_bank_account_trx", "c_account_number", where));
        assertEquals("", getColumnValue("app_fd_bank_account_trx", "c_payment_amount", where));
        assertEquals("", getColumnValue("app_fd_bank_account_trx", "c_currency", where));
    }

    @Test
    public void persistHandlesShortRowWithNullsForMissingIndices() throws SQLException {
        // Row shorter than 18 elements — missing indices should be stored as NULL
        String[] row = new String[5];
        row[0] = "EE1234";
        row[1] = "DOC001";
        row[2] = "2024-06-01";
        row[3] = "EE999";
        row[4] = "John Doe";

        RawTransactionPersister.persist(Collections.singletonList(row), "STMT-SHORT",
                MappingConfigurations.LHV_BANK_CONFIG, con);

        assertEquals(1, countRowsWithStatementId("app_fd_bank_account_trx", "STMT-SHORT"));

        String where = "c_statement_id = 'STMT-SHORT'";
        assertEquals("EE1234", getColumnValue("app_fd_bank_account_trx", "c_account_number", where));
        // Fields beyond row length should be NULL
        assertNull(getColumnValue("app_fd_bank_account_trx", "c_payment_amount", where));
        assertNull(getColumnValue("app_fd_bank_account_trx", "c_provider_reference", where));
    }

    // -------------------------------------------------------------------------
    // Empty/null list edge cases
    // -------------------------------------------------------------------------

    @Test
    public void persistEmptyListReturnsZero() throws SQLException {
        int inserted = RawTransactionPersister.persist(Collections.emptyList(), "STMT001",
                MappingConfigurations.LHV_BANK_CONFIG, con);

        assertEquals(0, inserted);
        assertEquals(0, countRows("app_fd_bank_account_trx"));
    }

    @Test
    public void persistNullListReturnsZero() throws SQLException {
        int inserted = RawTransactionPersister.persist(null, "STMT001",
                MappingConfigurations.LHV_BANK_CONFIG, con);

        assertEquals(0, inserted);
        assertEquals(0, countRows("app_fd_bank_account_trx"));
    }

    // -------------------------------------------------------------------------
    // Large dataset test
    // -------------------------------------------------------------------------

    @Test
    public void persistLargeDataset() throws SQLException {
        List<String[]> rows = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            rows.add(bankRow("EE1234", String.format("DOC%03d", i),
                    "2024-06-01", String.valueOf(i * 10), "EUR", "REF" + i));
        }

        int inserted = RawTransactionPersister.persist(rows, "STMT-LARGE",
                MappingConfigurations.LHV_BANK_CONFIG, con);

        assertEquals(200, inserted);
        assertEquals(200, countRowsWithStatementId("app_fd_bank_account_trx", "STMT-LARGE"));

        // Verify transaction ID range
        List<String> txIds = getColumnValues("app_fd_bank_account_trx",
                "c_transaction_id", "c_transaction_id");
        assertEquals("001", txIds.get(0));
        assertEquals("200", txIds.get(199));
    }

    // -------------------------------------------------------------------------
    // Multiple batches / isolation test
    // -------------------------------------------------------------------------

    @Test
    public void persistMultipleStatementsDoNotInterfere() throws SQLException {
        List<String[]> rows1 = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF_A1"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF_A2")
        );
        List<String[]> rows2 = Arrays.asList(
                bankRow("EE5678", "001", "2024-07-01", "500.00", "EUR", "REF_B1"),
                bankRow("EE5678", "002", "2024-07-02", "600.00", "EUR", "REF_B2"),
                bankRow("EE5678", "003", "2024-07-03", "700.00", "EUR", "REF_B3")
        );

        RawTransactionPersister.persist(rows1, "STMT-A", MappingConfigurations.LHV_BANK_CONFIG, con);
        RawTransactionPersister.persist(rows2, "STMT-B", MappingConfigurations.LHV_BANK_CONFIG, con);

        assertEquals(2, countRowsWithStatementId("app_fd_bank_account_trx", "STMT-A"));
        assertEquals(3, countRowsWithStatementId("app_fd_bank_account_trx", "STMT-B"));
        assertEquals(5, countRows("app_fd_bank_account_trx"));
    }

    // -------------------------------------------------------------------------
    // Swedbank config test
    // -------------------------------------------------------------------------

    @Test
    public void persistSwedbankRowsInsertsCorrectCount() throws SQLException {
        // Swedbank has 14 mapped fields
        String[] row = new String[14];
        Arrays.fill(row, "");
        row[0] = "EE9876";
        row[1] = "SWDOC001";
        row[2] = "2024-06-15";
        row[6] = "D";
        row[7] = "350.00";
        row[12] = "EUR";
        row[13] = "99887766";

        List<String[]> rows = Collections.singletonList(row);

        int inserted = RawTransactionPersister.persist(rows, "STMT-SWED",
                MappingConfigurations.SWED_BANK_CONFIG, con);

        assertEquals(1, inserted);
        assertEquals(1, countRowsWithStatementId("app_fd_bank_account_trx", "STMT-SWED"));

        String where = "c_statement_id = 'STMT-SWED'";
        assertEquals("EE9876", getColumnValue("app_fd_bank_account_trx", "c_account_number", where));
        assertEquals("D", getColumnValue("app_fd_bank_account_trx", "c_d_c", where));
        assertEquals("350.00", getColumnValue("app_fd_bank_account_trx", "c_payment_amount", where));
        assertEquals("99887766", getColumnValue("app_fd_bank_account_trx", "c_customer_id", where));
    }

    // -------------------------------------------------------------------------
    // Transaction ID starts at 001 for each batch
    // -------------------------------------------------------------------------

    @Test
    public void persistTransactionIdResetsPerBatch() throws SQLException {
        // First batch
        List<String[]> rows1 = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF002")
        );
        RawTransactionPersister.persist(rows1, "STMT-1", MappingConfigurations.LHV_BANK_CONFIG, con);

        // Second batch — transaction IDs should start at 001 again
        List<String[]> rows2 = Arrays.asList(
                bankRow("EE5678", "001", "2024-07-01", "500.00", "EUR", "REF003"),
                bankRow("EE5678", "002", "2024-07-02", "600.00", "EUR", "REF004"),
                bankRow("EE5678", "003", "2024-07-03", "700.00", "EUR", "REF005")
        );
        RawTransactionPersister.persist(rows2, "STMT-2", MappingConfigurations.LHV_BANK_CONFIG, con);

        // Verify STMT-1 has 001, 002
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_transaction_id FROM app_fd_bank_account_trx WHERE c_statement_id = ? ORDER BY c_transaction_id")) {
            ps.setString(1, "STMT-1");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals("001", rs.getString(1));
                rs.next();
                assertEquals("002", rs.getString(1));
            }
        }

        // Verify STMT-2 also has 001, 002, 003
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_transaction_id FROM app_fd_bank_account_trx WHERE c_statement_id = ? ORDER BY c_transaction_id")) {
            ps.setString(1, "STMT-2");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals("001", rs.getString(1));
                rs.next();
                assertEquals("002", rs.getString(1));
                rs.next();
                assertEquals("003", rs.getString(1));
            }
        }
    }
}
