package com.fiscaladmin.gam.statementimporter.consolidation;

import org.joget.apps.app.service.AppUtil;
import org.joget.commons.util.LogUtil;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Batch-inserts consolidated transaction rows into the target summary tables
 * using JDBC batch insert for performance.
 * <p>
 * Similar pattern to {@code RawTransactionPersister} but operates on
 * {@code Map<String, String>} rows from GROUP BY queries instead of
 * {@code String[]} arrays from CSV parsing.
 * <p>
 * Key behaviours:
 * <ul>
 *   <li>Uses JDBC (not FormDataDao) for bulk insert performance</li>
 *   <li>Generates Joget-compatible UUIDs for the {@code id} column</li>
 *   <li>Sets {@code c_statement_id} FK on every row</li>
 *   <li>Sets {@code c_statement_reference} for human-readable reference (STMT{YYYY}.{SEQ})</li>
 *   <li>Sets {@code c_status = "new"} for all consolidated rows</li>
 *   <li>Sets {@code dateCreated} and {@code createdBy} for Joget audit fields</li>
 * </ul>
 */
public class ConsolidatedRowPersister {

    private static final String CLASS_NAME = ConsolidatedRowPersister.class.getName();
    private static final String CREATED_BY = "system";
    private static final String DEFAULT_STATUS = "new";

    /**
     * Batch-inserts consolidated rows into the target table.
     * Obtains a JDBC connection from Joget's {@code setupDataSource}.
     *
     * @param rows              consolidated rows (each row is Map of column name to value)
     * @param statementId       the parent statement record ID
     * @param statementRefPrefix prefix for statement reference (e.g., "STMT2024")
     * @param insertSql         the INSERT SQL statement
     * @param columnOrder       list of column names in INSERT order
     * @param targetTable       target table name for logging
     * @return number of rows inserted
     * @throws RuntimeException if the batch insert fails
     */
    public static int persist(List<Map<String, String>> rows,
                               String statementId,
                               String statementRefPrefix,
                               String insertSql,
                               List<String> columnOrder,
                               String targetTable) {
        if (rows == null || rows.isEmpty()) {
            return 0;
        }
        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");
        try (Connection con = ds.getConnection()) {
            return persist(rows, statementId, statementRefPrefix, insertSql, columnOrder, targetTable, con);
        } catch (SQLException e) {
            LogUtil.error(CLASS_NAME, e, "Batch insert failed for table: " + targetTable);
            throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
        }
    }

    /**
     * Overload for testability and internal use — accepts any JDBC Connection
     * (including in-memory H2 for unit tests).
     *
     * @param rows              consolidated rows
     * @param statementId       the parent statement record ID
     * @param statementRefPrefix prefix for statement reference (e.g., "STMT2024")
     * @param insertSql         the INSERT SQL statement
     * @param columnOrder       list of column names in INSERT order
     * @param targetTable       target table name for logging
     * @param con               JDBC connection to use
     * @return number of rows inserted
     * @throws SQLException if a database error occurs
     */
    public static int persist(List<Map<String, String>> rows,
                       String statementId,
                       String statementRefPrefix,
                       String insertSql,
                       List<String> columnOrder,
                       String targetTable,
                       Connection con) throws SQLException {
        if (rows == null || rows.isEmpty()) {
            return 0;
        }

        LogUtil.info(CLASS_NAME, "Batch insert: " + rows.size() + " rows into " + targetTable);

        String now = getCurrentTimestamp();
        int seqId = 0;
        int totalInserted = 0;

        try (PreparedStatement stmt = con.prepareStatement(insertSql)) {
            for (Map<String, String> row : rows) {
                seqId++;
                String statementRef = formatStatementReference(statementRefPrefix, seqId);

                int paramIndex = 1;
                for (String column : columnOrder) {
                    String value = getColumnValue(row, column, statementId, statementRef, now, seqId);
                    stmt.setString(paramIndex++, value);
                }

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
                + " rows inserted into " + targetTable);

        return totalInserted;
    }

    /**
     * Gets the value for a column from the row data or generates system values.
     */
    private static String getColumnValue(Map<String, String> row,
                                         String column,
                                         String statementId,
                                         String statementRef,
                                         String now,
                                         int seqId) {
        switch (column) {
            case "id":
                return UUID.randomUUID().toString();
            case "c_statement_id":
                return statementId;
            case "c_statement_reference":
                return statementRef;
            case "c_status":
                return DEFAULT_STATUS;
            case "dateCreated":
                return now;
            case "createdBy":
                return CREATED_BY;
            default:
                // Look up in the row data
                String value = row.get(column);
                return value != null ? value : "";
        }
    }

    /**
     * Formats a statement reference as STMT{YYYY}.{SEQ}.
     * Example: "STMT2024" + 1 → "STMT2024.001"
     */
    private static String formatStatementReference(String prefix, int seqId) {
        return String.format("%s.%03d", prefix, seqId);
    }

    /**
     * Returns the current timestamp in the format Joget uses for audit fields.
     */
    private static String getCurrentTimestamp() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }
}
