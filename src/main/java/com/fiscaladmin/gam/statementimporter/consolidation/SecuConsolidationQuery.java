package com.fiscaladmin.gam.statementimporter.consolidation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL queries and column definitions for securities transaction consolidation.
 * <p>
 * Consolidates raw securities transactions (app_fd_sec_account_trx) into summary rows
 * (app_fd_sec_total_trx) using GROUP BY aggregation.
 * <p>
 * This handles scenarios like LHV1T partial fills, where multiple raw rows
 * (e.g., qty=257 + qty=500) merge into a single consolidated row (qty=757).
 * <p>
 * Grouping dimensions (6 identifying fields):
 * <ul>
 *   <li>c_value_date</li>
 *   <li>c_transaction_date</li>
 *   <li>c_type (ost, müük, split+, split-, etc.)</li>
 *   <li>c_ticker</li>
 *   <li>c_description</li>
 *   <li>c_currency</li>
 * </ul>
 * <p>
 * Aggregations:
 * <ul>
 *   <li>c_quantity: SUM</li>
 *   <li>c_amount: SUM</li>
 *   <li>c_fee: SUM</li>
 *   <li>c_total_amount: SUM</li>
 *   <li>c_price: AVG (weighted average would be ideal, but simple avg for now)</li>
 *   <li>c_reference: GROUP_CONCAT</li>
 * </ul>
 */
public final class SecuConsolidationQuery {

    /** Target table for consolidated securities transactions */
    public static final String TARGET_TABLE = "app_fd_secu_total_trx";

    /** Source table for raw securities transactions */
    public static final String SOURCE_TABLE = "app_fd_sec_account_trx";

    /**
     * GROUP BY query that aggregates raw securities transactions.
     * <p>
     * Returns one row per unique combination of (value_date, transaction_date, type, ticker, description, currency).
     * Quantities, amounts, fees are summed. Prices are averaged. References are concatenated.
     */
    public static final String GROUP_BY_SQL =
        "SELECT " +
        "  c_value_date, " +
        "  c_transaction_date, " +
        "  c_type, " +
        "  c_ticker, " +
        "  c_description, " +
        "  c_currency, " +
        "  SUM(CAST(c_quantity AS DECIMAL(15,6))) AS total_quantity, " +
        "  AVG(CAST(c_price AS DECIMAL(15,8))) AS avg_price, " +
        "  SUM(CAST(c_amount AS DECIMAL(15,2))) AS total_amount, " +
        "  SUM(CAST(c_fee AS DECIMAL(15,2))) AS total_fee, " +
        "  SUM(CAST(c_total_amount AS DECIMAL(15,2))) AS total_total_amount, " +
        "  GROUP_CONCAT(c_reference ORDER BY c_transaction_id SEPARATOR ',') AS `references` " +
        "FROM " + SOURCE_TABLE + " " +
        "WHERE c_statement_id = ? " +
        "GROUP BY c_value_date, c_transaction_date, c_type, c_ticker, c_description, c_currency " +
        "ORDER BY c_value_date, c_type, c_ticker";

    /**
     * Column names in INSERT order for the target table.
     * Must match the order of placeholders in INSERT_SQL.
     */
    public static final List<String> INSERT_COLUMNS = Arrays.asList(
        "id",
        "c_statement_id",
        "c_statement_reference",
        "c_value_date",
        "c_transaction_date",
        "c_type",
        "c_ticker",
        "c_description",
        "c_currency",
        "c_quantity",
        "c_price",
        "c_amount",
        "c_fee",
        "c_total_amount",
        "c_reference",
        "c_status",
        "dateCreated",
        "createdBy"
    );

    /**
     * INSERT statement for consolidated rows.
     */
    public static final String INSERT_SQL =
        "INSERT INTO " + TARGET_TABLE + " (" +
        "id, " +
        "c_statement_id, " +
        "c_statement_reference, " +
        "c_value_date, " +
        "c_transaction_date, " +
        "c_type, " +
        "c_ticker, " +
        "c_description, " +
        "c_currency, " +
        "c_quantity, " +
        "c_price, " +
        "c_amount, " +
        "c_fee, " +
        "c_total_amount, " +
        "c_reference, " +
        "c_status, " +
        "dateCreated, " +
        "createdBy" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /**
     * DELETE statement for idempotency — removes existing consolidated rows
     * before re-consolidating.
     */
    public static final String DELETE_SQL =
        "DELETE FROM " + TARGET_TABLE + " WHERE c_statement_id = ?";

    /**
     * Reads a result row from the GROUP BY query into a Map.
     *
     * @param rs the ResultSet positioned at a row
     * @return Map with column names as keys
     * @throws SQLException if a database error occurs
     */
    public static Map<String, String> readRow(ResultSet rs) throws SQLException {
        Map<String, String> row = new HashMap<>();
        row.put("c_value_date", rs.getString("c_value_date"));
        row.put("c_transaction_date", rs.getString("c_transaction_date"));
        row.put("c_type", rs.getString("c_type"));
        row.put("c_ticker", rs.getString("c_ticker"));
        row.put("c_currency", rs.getString("c_currency"));
        row.put("c_description", rs.getString("c_description"));
        row.put("c_quantity", rs.getString("total_quantity"));
        row.put("c_price", rs.getString("avg_price"));
        row.put("c_amount", rs.getString("total_amount"));
        row.put("c_fee", rs.getString("total_fee"));
        row.put("c_total_amount", rs.getString("total_total_amount"));
        row.put("c_reference", rs.getString("references"));
        return row;
    }

    private SecuConsolidationQuery() {
        // utility class — no instantiation
    }
}
