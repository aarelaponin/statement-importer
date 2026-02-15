package com.fiscaladmin.gam.statementimporter.consolidation;

import com.fiscaladmin.gam.statementimporter.mapping.MappingConfig;
import com.fiscaladmin.gam.statementimporter.mapping.MappingConfigurations;
import com.fiscaladmin.gam.statementimporter.parser.CsvFormatDetector;
import com.fiscaladmin.gam.statementimporter.parser.Format;
import com.fiscaladmin.gam.statementimporter.parser.StatementParser;
import com.fiscaladmin.gam.statementimporter.persister.RawTransactionPersister;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for consolidation tests providing H2 database setup and helpers.
 * <p>
 * Uses H2 in MySQL mode for GROUP_CONCAT support.
 * Creates all 4 tables:
 * <ul>
 *   <li>app_fd_bank_account_trx (source: raw bank transactions)</li>
 *   <li>app_fd_sec_account_trx (source: raw securities transactions)</li>
 *   <li>app_fd_bank_total_trx (target: consolidated bank transactions)</li>
 *   <li>app_fd_secu_total_trx (target: consolidated securities transactions)</li>
 * </ul>
 */
public abstract class ConsolidationTestBase {

    protected static final String BANK_CSV = "test-data/bank_statement.csv";
    protected static final String SECU_CSV = "test-data/secu_statement.csv";

    protected Connection con;

    /**
     * Creates a new H2 connection with MySQL mode enabled.
     * Call this in @Before method.
     */
    protected void setUpDatabase() throws SQLException {
        con = DriverManager.getConnection(
            "jdbc:h2:mem:consolidation_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");

        try (Statement stmt = con.createStatement()) {
            // Source table: raw bank transactions (same as RawTransactionPersisterTest)
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

            // Source table: raw securities transactions
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

            // Target table: consolidated bank transactions
            stmt.execute("CREATE TABLE IF NOT EXISTS app_fd_bank_total_trx ("
                + "id VARCHAR(255) PRIMARY KEY, "
                + "c_statement_id VARCHAR(255), "
                + "c_statement_reference VARCHAR(50), "
                + "c_account_number VARCHAR(255), "
                + "c_document_nr VARCHAR(255), "
                + "c_payment_date VARCHAR(255), "
                + "c_other_side_account VARCHAR(255), "
                + "c_other_side_name VARCHAR(255), "
                + "c_other_side_bank VARCHAR(255), "
                + "c_d_c VARCHAR(10), "
                + "c_payment_description TEXT, "
                + "c_currency VARCHAR(255), "
                + "c_customer_id VARCHAR(255), "
                + "c_other_side_bic VARCHAR(255), "
                + "c_payment_amount VARCHAR(255), "
                + "c_transaction_fee VARCHAR(255), "
                + "c_provider_reference TEXT, "
                + "c_status VARCHAR(50), "
                + "dateCreated VARCHAR(50), "
                + "createdBy VARCHAR(255)"
                + ")");

            // Target table: consolidated securities transactions
            stmt.execute("CREATE TABLE IF NOT EXISTS app_fd_secu_total_trx ("
                + "id VARCHAR(255) PRIMARY KEY, "
                + "c_statement_id VARCHAR(255), "
                + "c_statement_reference VARCHAR(50), "
                + "c_value_date VARCHAR(255), "
                + "c_transaction_date VARCHAR(255), "
                + "c_type VARCHAR(255), "
                + "c_ticker VARCHAR(255), "
                + "c_description TEXT, "
                + "c_currency VARCHAR(255), "
                + "c_quantity VARCHAR(255), "
                + "c_price VARCHAR(255), "
                + "c_amount VARCHAR(255), "
                + "c_fee VARCHAR(255), "
                + "c_total_amount VARCHAR(255), "
                + "c_reference TEXT, "
                + "c_status VARCHAR(50), "
                + "dateCreated VARCHAR(50), "
                + "createdBy VARCHAR(255)"
                + ")");
        }
    }

    /**
     * Drops all tables and closes connection.
     * Call this in @After method.
     */
    protected void tearDownDatabase() throws SQLException {
        if (con != null && !con.isClosed()) {
            try (Statement stmt = con.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS app_fd_bank_account_trx");
                stmt.execute("DROP TABLE IF EXISTS app_fd_sec_account_trx");
                stmt.execute("DROP TABLE IF EXISTS app_fd_bank_total_trx");
                stmt.execute("DROP TABLE IF EXISTS app_fd_secu_total_trx");
            }
            con.close();
        }
    }

    /**
     * Loads the real bank CSV, parses it, and persists raw rows.
     *
     * @param statementId the statement ID to use
     * @return number of rows persisted, or 0 if CSV not found
     */
    protected int loadAndPersistBankCsv(String statementId) throws Exception {
        File csvFile = new File(BANK_CSV);
        if (!csvFile.exists()) {
            return 0;
        }

        Format format = CsvFormatDetector.detect(csvFile);
        List<String[]> rows = StatementParser.parse(csvFile, format);
        MappingConfig config = MappingConfigurations.getConfig(format);

        return RawTransactionPersister.persist(rows, statementId, config, con);
    }

    /**
     * Loads the real securities CSV, parses it, and persists raw rows.
     *
     * @param statementId the statement ID to use
     * @return number of rows persisted, or 0 if CSV not found
     */
    protected int loadAndPersistSecuCsv(String statementId) throws Exception {
        File csvFile = new File(SECU_CSV);
        if (!csvFile.exists()) {
            return 0;
        }

        Format format = CsvFormatDetector.detect(csvFile);
        List<String[]> rows = StatementParser.parse(csvFile, format);
        MappingConfig config = MappingConfigurations.getConfig(format);

        return RawTransactionPersister.persist(rows, statementId, config, con);
    }

    /**
     * Counts rows in a table with a specific statement ID.
     */
    protected int countRowsWithStatementId(String table, String statementId) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COUNT(*) FROM " + table + " WHERE c_statement_id = ?")) {
            ps.setString(1, statementId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    /**
     * Gets a single column value from a table with a WHERE clause.
     */
    protected String getColumnValue(String table, String column, String whereClause) throws SQLException {
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + column + " FROM " + table + " WHERE " + whereClause)) {
            if (rs.next()) return rs.getString(1);
            return null;
        }
    }

    /**
     * Gets all values of a column from a table ordered by orderBy.
     */
    protected List<String> getColumnValues(String table, String column, String orderBy) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + column + " FROM " + table + " ORDER BY " + orderBy)) {
            while (rs.next()) {
                values.add(rs.getString(1));
            }
        }
        return values;
    }

    /**
     * Counts rows in a table.
     */
    protected int countRows(String table) throws SQLException {
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
