package com.fiscaladmin.gam.statementimporter.persister;

import com.fiscaladmin.gam.statementimporter.mapping.FieldMapping;
import com.fiscaladmin.gam.statementimporter.mapping.MappingConfig;
import org.joget.apps.app.service.AppUtil;
import org.joget.commons.util.LogUtil;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Batch-inserts parsed CSV rows into the target raw transaction table
 * using JDBC batch insert for performance.
 * <p>
 * Design decision (from {@code preprocessing-architecture.md} §6.5):
 * Uses JDBC batch insert (not FormDataDao) for raw transaction row tables.
 * These are bulk staging operations where performance matters — 161 rows
 * in one batch for bank, 45 for securities. Raw rows are never edited
 * after creation, so the audit trail benefit of FormDataDao is less
 * important here.
 * <p>
 * Key behaviours:
 * <ul>
 *   <li>Uses JDBC (not FormDataDao) for bulk insert performance</li>
 *   <li>Generates Joget-compatible UUIDs for the {@code id} column</li>
 *   <li>Sets {@code c_statement_id} FK on every row</li>
 *   <li>Sets {@code c_transaction_id} as sequential row number (zero-padded to 3 digits)</li>
 *   <li>Sets {@code dateCreated} and {@code createdBy} for Joget audit fields</li>
 *   <li>Target table from MappingConfig (e.g., {@code app_fd_bank_account_trx})</li>
 * </ul>
 */
public class RawTransactionPersister {

    private static final String CLASS_NAME = RawTransactionPersister.class.getName();
    private static final String CREATED_BY = "system";

    /**
     * Batch-inserts parsed CSV rows into the target raw transaction table.
     * Obtains a JDBC connection from Joget's {@code setupDataSource}.
     *
     * @param rows        parsed CSV rows (each row is String[] of field values)
     * @param statementId the parent statement record ID
     * @param config      the mapping configuration for the CSV format
     * @return number of rows inserted
     * @throws RuntimeException if the batch insert fails
     */
    public static int persist(List<String[]> rows, String statementId, MappingConfig config) {
        if (rows == null || rows.isEmpty()) {
            return 0;
        }
        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");
        try (Connection con = ds.getConnection()) {
            return persist(rows, statementId, config, con);
        } catch (SQLException e) {
            LogUtil.error(CLASS_NAME, e, "Batch insert failed for table: " + config.getTargetTable());
            throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
        }
    }

    /**
     * Overload for testability and internal use — accepts any JDBC Connection
     * (including in-memory H2 for unit tests).
     *
     * @param rows        parsed CSV rows
     * @param statementId the parent statement record ID
     * @param config      the mapping configuration
     * @param con         JDBC connection to use
     * @return number of rows inserted
     * @throws SQLException if a database error occurs
     */
    public static int persist(List<String[]> rows, String statementId, MappingConfig config, Connection con)
            throws SQLException {
        if (rows == null || rows.isEmpty()) {
            return 0;
        }

        LogUtil.info(CLASS_NAME, "Batch insert: " + rows.size() + " rows into " + config.getTargetTable());

        String sql = buildInsertSql(config);
        String now = getCurrentTimestamp();

        int seqId = 0;
        int totalInserted = 0;

        try (PreparedStatement stmt = con.prepareStatement(sql)) {
            for (String[] row : rows) {
                seqId++;
                int paramIndex = 1;

                // Set mapped field values from the parsed CSV row
                List<FieldMapping> mappings = config.getFieldMappings();
                for (FieldMapping mapping : mappings) {
                    int csvIdx = mapping.getCsvIndex();
                    String value = (csvIdx >= 0 && csvIdx < row.length) ? row[csvIdx] : null;
                    stmt.setString(paramIndex++, value);
                }

                // System fields
                stmt.setString(paramIndex++, UUID.randomUUID().toString());   // id
                stmt.setString(paramIndex++, formatTransactionId(seqId));     // c_transaction_id
                stmt.setString(paramIndex++, statementId);                    // c_statement_id

                // Joget audit fields
                stmt.setString(paramIndex++, now);          // dateCreated
                stmt.setString(paramIndex++, CREATED_BY);   // createdBy

                stmt.addBatch();
            }

            int[] results = stmt.executeBatch();
            for (int r : results) {
                if (r >= 0) {
                    totalInserted += r;
                } else if (r == Statement.SUCCESS_NO_INFO) {
                    totalInserted++;
                }
            }
        }

        LogUtil.info(CLASS_NAME, "Batch insert completed: " + totalInserted
                + " rows inserted into " + config.getTargetTable());

        return totalInserted;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Builds the INSERT SQL dynamically from MappingConfig field mappings.
     * <p>
     * Columns: [mapped CSV fields...], id, c_transaction_id, c_statement_id, dateCreated, createdBy
     */
    private static String buildInsertSql(MappingConfig config) {
        List<FieldMapping> mappings = config.getFieldMappings();
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(config.getTargetTable());
        sql.append(" (");

        // Mapped columns from CSV
        for (int i = 0; i < mappings.size(); i++) {
            sql.append(mappings.get(i).getDbColumnName());
            sql.append(", ");
        }

        // System + audit columns (no c_ prefix on id, dateCreated, createdBy)
        sql.append("id, c_transaction_id, c_statement_id, dateCreated, createdBy");
        sql.append(") VALUES (");

        // Placeholders: mapped fields + 5 system/audit columns
        int totalParams = mappings.size() + 5;
        for (int i = 0; i < totalParams; i++) {
            sql.append('?');
            if (i < totalParams - 1) {
                sql.append(", ");
            }
        }
        sql.append(')');

        return sql.toString();
    }

    /**
     * Formats a sequential row number as a zero-padded 3-digit string.
     * Matches the BeanShell's {@code Formatter.formatTransactionId()}.
     * <p>
     * Examples: 1 → "001", 12 → "012", 161 → "161"
     */
    private static String formatTransactionId(int seqId) {
        return String.format("%03d", seqId);
    }

    /**
     * Returns the current timestamp in the format Joget uses for audit fields.
     */
    private static String getCurrentTimestamp() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }
}
