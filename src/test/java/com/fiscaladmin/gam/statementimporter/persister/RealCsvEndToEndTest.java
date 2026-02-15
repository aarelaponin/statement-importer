package com.fiscaladmin.gam.statementimporter.persister;

import com.fiscaladmin.gam.statementimporter.mapping.MappingConfig;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * End-to-end integration tests that read the <b>actual</b> production CSV files,
 * run them through the full parsing pipeline (detect → parse → persist), and
 * insert into H2 in-memory tables whose schemas exactly mirror the Joget form
 * definitions.
 * <p>
 * This test class validates:
 * <ol>
 *   <li>CSV format detection works on real files</li>
 *   <li>Parsing produces the correct number of rows with correct field counts</li>
 *   <li>Every column name in the MappingConfig exists in the corresponding Joget form table</li>
 *   <li>Field values from the CSV are correctly mapped to database columns</li>
 *   <li>System/audit fields (id, transaction_id, statement_id, dateCreated, createdBy) are set</li>
 *   <li>Mandatory form fields contain no NULLs</li>
 *   <li>Bank and securities imports do not interfere with each other</li>
 * </ol>
 * <p>
 * Tests that depend on the real CSV files are guarded with a file-existence check
 * and skip silently in environments where sample data is not available.
 */
public class RealCsvEndToEndTest {

    private static final String BANK_CSV = "test-data/bank_statement.csv";
    private static final String SECU_CSV = "test-data/secu_statement.csv";

    private static final MappingConfig LHV_BANK_CONFIG = MappingConfigurations.LHV_BANK_CONFIG;
    private static final MappingConfig SECU_CONFIG = MappingConfigurations.SECU_CONFIG;

    private Connection con;

    // -------------------------------------------------------------------------
    // Setup / teardown
    // -------------------------------------------------------------------------

    @Before
    public void setUp() throws Exception {
        con = DriverManager.getConnection("jdbc:h2:mem:e2etest;DB_CLOSE_DELAY=-1", "sa", "");

        try (Statement stmt = con.createStatement()) {
            // Bank account transactions — mirrors Joget form F01.01
            stmt.execute("CREATE TABLE IF NOT EXISTS app_fd_bank_account_trx ("
                    + "id VARCHAR(255) PRIMARY KEY, "
                    + "c_statement_id VARCHAR(255), "
                    + "c_transaction_id VARCHAR(10), "
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
                    + "c_currency VARCHAR(255), "
                    + "c_customer_id VARCHAR(255), "
                    + "c_other_side_bic VARCHAR(255), "
                    + "c_initiator VARCHAR(255), "
                    + "c_transaction_reference VARCHAR(255), "
                    + "c_provider_reference VARCHAR(255), "
                    + "dateCreated VARCHAR(50), "
                    + "createdBy VARCHAR(255)"
                    + ")");

            // Securities account transactions — mirrors Joget form F01.02
            stmt.execute("CREATE TABLE IF NOT EXISTS app_fd_sec_account_trx ("
                    + "id VARCHAR(255) PRIMARY KEY, "
                    + "c_statement_id VARCHAR(255), "
                    + "c_transaction_id VARCHAR(10), "
                    + "c_value_date VARCHAR(255), "
                    + "c_transaction_date VARCHAR(255), "
                    + "c_type VARCHAR(255), "
                    + "c_ticker VARCHAR(255), "
                    + "c_description TEXT, "
                    + "c_quantity VARCHAR(255), "
                    + "c_price VARCHAR(255), "
                    + "c_currency VARCHAR(255), "
                    + "c_amount VARCHAR(255), "
                    + "c_fee VARCHAR(255), "
                    + "c_total_amount VARCHAR(255), "
                    + "c_reference VARCHAR(255), "
                    + "c_comment TEXT, "
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

    // -------------------------------------------------------------------------
    // Test 1: Full bank CSV pipeline
    // -------------------------------------------------------------------------

    @Test
    public void bankCsvFullPipeline() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: bankCsvFullPipeline — bank_statement.csv not found at " + BANK_CSV);
            return;
        }

        // 1. Detect format
        Format format = CsvFormatDetector.detect(bankFile);
        assertEquals("Format should be LHV_BANK", Format.LHV_BANK, format);

        // 2. Parse
        List<String[]> rows = StatementParser.parse(bankFile, format);
        assertEquals("Should parse 161 data rows", 161, rows.size());
        assertEquals("Each row should have 18 fields (after stripping empty col 6)",
                LHV_BANK_CONFIG.getFieldMappings().size(), rows.get(0).length);

        // 3. Persist
        int inserted = RawTransactionPersister.persist(rows, "TEST-BANK-001", LHV_BANK_CONFIG, con);
        assertEquals("Should insert 161 rows", 161, inserted);

        // 4. Verify row count in DB
        assertEquals("DB should contain 161 rows for TEST-BANK-001",
                161, countRowsWithStatementId("app_fd_bank_account_trx", "TEST-BANK-001"));

        // 5. Spot-check first row (transaction_id = "001")
        assertColumnEquals("app_fd_bank_account_trx", "c_account_number", "c_transaction_id = '001'",
                "EE117700771003603322", "First row account_number");
        assertColumnEquals("app_fd_bank_account_trx", "c_payment_date", "c_transaction_id = '001'",
                "2024-06-05", "First row payment_date");
        assertColumnEquals("app_fd_bank_account_trx", "c_d_c", "c_transaction_id = '001'",
                "C", "First row d_c");
        assertColumnEquals("app_fd_bank_account_trx", "c_payment_amount", "c_transaction_id = '001'",
                "0.31", "First row payment_amount");
        assertColumnEquals("app_fd_bank_account_trx", "c_currency", "c_transaction_id = '001'",
                "EUR", "First row currency");
        assertColumnEquals("app_fd_bank_account_trx", "c_provider_reference", "c_transaction_id = '001'",
                "17401979DC22EF11B45B001DD8D11D14", "First row provider_reference");

        // Verify system fields on first row
        String firstId = getColumnValue("app_fd_bank_account_trx", "id", "c_transaction_id = '001'");
        assertNotNull("First row id (UUID) should not be null", firstId);
        assertEquals("UUID should be 36 chars", 36, firstId.length());

        String firstDateCreated = getColumnValue("app_fd_bank_account_trx", "dateCreated", "c_transaction_id = '001'");
        assertNotNull("First row dateCreated should not be null", firstDateCreated);
        assertTrue("dateCreated should match yyyy-MM-dd HH:mm:ss",
                firstDateCreated.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));

        assertColumnEquals("app_fd_bank_account_trx", "createdBy", "c_transaction_id = '001'",
                "system", "First row createdBy");

        // 6. Spot-check last row (transaction_id = "161")
        assertColumnEquals("app_fd_bank_account_trx", "c_payment_date", "c_transaction_id = '161'",
                "2024-07-30", "Last row payment_date");
        assertColumnEquals("app_fd_bank_account_trx", "c_d_c", "c_transaction_id = '161'",
                "D", "Last row d_c");
        assertColumnEquals("app_fd_bank_account_trx", "c_payment_amount", "c_transaction_id = '161'",
                "-12.84", "Last row payment_amount");
        assertColumnEquals("app_fd_bank_account_trx", "c_currency", "c_transaction_id = '161'",
                "USD", "Last row currency");
        assertColumnEquals("app_fd_bank_account_trx", "c_provider_reference", "c_transaction_id = '161'",
                "C5D38254B04DEF11B45B001DD8D11D14", "Last row provider_reference");

        // 7. Verify no NULLs in mandatory fields
        int nullCount = countNullMandatoryBankFields();
        assertEquals("No mandatory fields should be NULL", 0, nullCount);

        // 8. Verify transaction_id sequence is continuous "001" through "161"
        List<String> txIds = getColumnValues("app_fd_bank_account_trx", "c_transaction_id", "c_transaction_id");
        assertEquals(161, txIds.size());
        for (int i = 0; i < 161; i++) {
            assertEquals("Transaction ID should be sequential",
                    String.format("%03d", i + 1), txIds.get(i));
        }
    }

    // -------------------------------------------------------------------------
    // Test 2: Full securities CSV pipeline
    // -------------------------------------------------------------------------

    @Test
    public void secuCsvFullPipeline() throws Exception {
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: secuCsvFullPipeline — secu_statement.csv not found at " + SECU_CSV);
            return;
        }

        // 1. Detect format
        Format format = CsvFormatDetector.detect(secuFile);
        assertEquals("Format should be SECURITIES", Format.SECURITIES, format);

        // 2. Parse
        List<String[]> rows = StatementParser.parse(secuFile, format);
        assertEquals("Should parse 45 data rows", 45, rows.size());
        assertEquals("Each row should have 13 fields",
                SECU_CONFIG.getFieldMappings().size(), rows.get(0).length);

        // 3. Persist
        int inserted = RawTransactionPersister.persist(rows, "TEST-SECU-001", SECU_CONFIG, con);
        assertEquals("Should insert 45 rows", 45, inserted);

        // 4. Verify row count in DB
        assertEquals("DB should contain 45 rows for TEST-SECU-001",
                45, countRowsWithStatementId("app_fd_sec_account_trx", "TEST-SECU-001"));

        // 5. Spot-check first row (Holm Bank bond purchase)
        assertColumnEquals("app_fd_sec_account_trx", "c_value_date", "c_transaction_id = '001'",
                "2024-06-07", "First row value_date");
        assertColumnEquals("app_fd_sec_account_trx", "c_transaction_date", "c_transaction_id = '001'",
                "2024-06-05", "First row transaction_date");
        assertColumnEquals("app_fd_sec_account_trx", "c_type", "c_transaction_id = '001'",
                "ost", "First row type");
        assertColumnEquals("app_fd_sec_account_trx", "c_ticker", "c_transaction_id = '001'",
                "HLMBK095034FA", "First row ticker");
        assertColumnEquals("app_fd_sec_account_trx", "c_quantity", "c_transaction_id = '001'",
                "1", "First row quantity");
        assertColumnEquals("app_fd_sec_account_trx", "c_price", "c_transaction_id = '001'",
                "1051.84722000", "First row price");
        assertColumnEquals("app_fd_sec_account_trx", "c_currency", "c_transaction_id = '001'",
                "EUR", "First row currency");
        assertColumnEquals("app_fd_sec_account_trx", "c_amount", "c_transaction_id = '001'",
                "-1051.85", "First row amount");
        assertColumnEquals("app_fd_sec_account_trx", "c_fee", "c_transaction_id = '001'",
                "-8.10", "First row fee");
        assertColumnEquals("app_fd_sec_account_trx", "c_total_amount", "c_transaction_id = '001'",
                "-1059.95", "First row total_amount");
        assertColumnEquals("app_fd_sec_account_trx", "c_reference", "c_transaction_id = '001'",
                "903745878", "First row reference");

        // 6. Spot-check last row (SMCI buy)
        assertColumnEquals("app_fd_sec_account_trx", "c_value_date", "c_transaction_id = '045'",
                "2024-08-01", "Last row value_date");
        assertColumnEquals("app_fd_sec_account_trx", "c_type", "c_transaction_id = '045'",
                "ost", "Last row type");
        assertColumnEquals("app_fd_sec_account_trx", "c_ticker", "c_transaction_id = '045'",
                "SMCI", "Last row ticker");
        assertColumnEquals("app_fd_sec_account_trx", "c_currency", "c_transaction_id = '045'",
                "USD", "Last row currency");
        assertColumnEquals("app_fd_sec_account_trx", "c_reference", "c_transaction_id = '045'",
                "940524330", "Last row reference");

        // 7. Verify LHV1T buy fragments (rows 3 and 4 in CSV → transaction_ids vary)
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_quantity, c_amount, c_reference FROM app_fd_sec_account_trx "
                        + "WHERE c_ticker = 'LHV1T' AND c_type = 'ost' ORDER BY c_transaction_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("Should find first LHV1T buy fragment", rs.next());
                assertEquals("First LHV1T fragment quantity", "257", rs.getString("c_quantity"));
                assertEquals("First LHV1T fragment amount", "-899.50", rs.getString("c_amount"));
                assertEquals("First LHV1T fragment reference", "903867200", rs.getString("c_reference"));

                assertTrue("Should find second LHV1T buy fragment", rs.next());
                assertEquals("Second LHV1T fragment quantity", "500", rs.getString("c_quantity"));
                assertEquals("Second LHV1T fragment amount", "-1750.00", rs.getString("c_amount"));
                assertEquals("Second LHV1T fragment reference", "903867343", rs.getString("c_reference"));
            }
        }

        // 8. Verify transaction_id sequence "001" through "045"
        List<String> txIds = getColumnValues("app_fd_sec_account_trx", "c_transaction_id", "c_transaction_id");
        assertEquals(45, txIds.size());
        for (int i = 0; i < 45; i++) {
            assertEquals("Transaction ID should be sequential",
                    String.format("%03d", i + 1), txIds.get(i));
        }
    }

    // -------------------------------------------------------------------------
    // Test 3: Bank INSERT columns match form schema
    // -------------------------------------------------------------------------

    @Test
    public void bankInsertColumnsMatchFormSchema() throws SQLException {
        String[] testRow = new String[LHV_BANK_CONFIG.getFieldMappings().size()];
        Arrays.fill(testRow, "test");

        int inserted = RawTransactionPersister.persist(
                java.util.Collections.singletonList(testRow),
                "SCHEMA-TEST-BANK",
                LHV_BANK_CONFIG,
                con);

        assertEquals("INSERT with all mapped columns should succeed (proves column names match form schema)",
                1, inserted);
    }

    // -------------------------------------------------------------------------
    // Test 4: Securities INSERT columns match form schema
    // -------------------------------------------------------------------------

    @Test
    public void secuInsertColumnsMatchFormSchema() throws SQLException {
        String[] testRow = new String[SECU_CONFIG.getFieldMappings().size()];
        Arrays.fill(testRow, "test");

        int inserted = RawTransactionPersister.persist(
                java.util.Collections.singletonList(testRow),
                "SCHEMA-TEST-SECU",
                SECU_CONFIG,
                con);

        assertEquals("INSERT with all mapped columns should succeed (proves column names match form schema)",
                1, inserted);
    }

    // -------------------------------------------------------------------------
    // Test 5: Bank and securities do not interfere
    // -------------------------------------------------------------------------

    @Test
    public void bankAndSecuDoNotInterfere() throws Exception {
        File bankFile = new File(BANK_CSV);
        File secuFile = new File(SECU_CSV);
        if (!bankFile.exists() || !secuFile.exists()) {
            System.out.println("SKIP: bankAndSecuDoNotInterfere — CSV files not found");
            return;
        }

        // Parse and persist bank
        List<String[]> bankRows = StatementParser.parse(bankFile, Format.LHV_BANK);
        RawTransactionPersister.persist(bankRows, "ISOLATION-BANK", LHV_BANK_CONFIG, con);

        // Parse and persist securities
        List<String[]> secuRows = StatementParser.parse(secuFile, Format.SECURITIES);
        RawTransactionPersister.persist(secuRows, "ISOLATION-SECU", SECU_CONFIG, con);

        // Bank table: exactly 161 rows for ISOLATION-BANK
        assertEquals("Bank table should have 161 rows",
                161, countRowsWithStatementId("app_fd_bank_account_trx", "ISOLATION-BANK"));

        // Securities table: exactly 45 rows for ISOLATION-SECU
        assertEquals("Secu table should have 45 rows",
                45, countRowsWithStatementId("app_fd_sec_account_trx", "ISOLATION-SECU"));

        // Bank table should have NO rows with the secu statement ID
        assertEquals("Bank table should have no secu rows",
                0, countRowsWithStatementId("app_fd_bank_account_trx", "ISOLATION-SECU"));

        // Securities table should have NO rows with the bank statement ID
        assertEquals("Secu table should have no bank rows",
                0, countRowsWithStatementId("app_fd_sec_account_trx", "ISOLATION-BANK"));
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

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

    private void assertColumnEquals(String table, String column, String whereClause,
                                    String expected, String message) throws SQLException {
        String actual = getColumnValue(table, column, whereClause);
        assertEquals(message, expected, actual);
    }

    /**
     * Counts rows in bank_account_trx where any mandatory field is NULL.
     * Mandatory fields: account_number, payment_date, d_c, payment_amount, currency.
     */
    private int countNullMandatoryBankFields() throws SQLException {
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM app_fd_bank_account_trx "
                             + "WHERE c_account_number IS NULL "
                             + "   OR c_payment_date IS NULL "
                             + "   OR c_d_c IS NULL "
                             + "   OR c_payment_amount IS NULL "
                             + "   OR c_currency IS NULL")) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
