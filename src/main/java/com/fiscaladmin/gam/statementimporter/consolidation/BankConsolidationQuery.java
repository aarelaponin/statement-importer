package com.fiscaladmin.gam.statementimporter.consolidation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL queries and column definitions for bank transaction consolidation.
 * <p>
 * Consolidates raw bank transactions (app_fd_bank_account_trx) into summary rows
 * (app_fd_bank_total_trx) using GROUP BY aggregation.
 * <p>
 * Grouping dimensions (11 identifying fields):
 * <ul>
 *   <li>c_account_number</li>
 *   <li>c_document_nr</li>
 *   <li>c_payment_date</li>
 *   <li>c_other_side_account</li>
 *   <li>c_other_side_name</li>
 *   <li>c_other_side_bank</li>
 *   <li>c_d_c (Debit/Credit)</li>
 *   <li>c_payment_description</li>
 *   <li>c_currency</li>
 *   <li>c_customer_id</li>
 *   <li>c_other_side_bic</li>
 * </ul>
 * <p>
 * Aggregations:
 * <ul>
 *   <li>c_payment_amount: SUM</li>
 *   <li>c_transaction_fee: SUM</li>
 *   <li>c_provider_reference: GROUP_CONCAT</li>
 * </ul>
 */
public final class BankConsolidationQuery {

    /** Target table for consolidated bank transactions */
    public static final String TARGET_TABLE = "app_fd_bank_total_trx";

    /** Source table for raw bank transactions */
    public static final String SOURCE_TABLE = "app_fd_bank_account_trx";

    /**
     * GROUP BY query that aggregates raw bank transactions.
     * <p>
     * Groups by all identifying fields to preserve transaction granularity.
     * Only truly duplicate rows (same document_nr, payment_date, counterparty, etc.) are merged.
     * Amounts and fees are summed. Provider references are concatenated.
     */
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
        "  GROUP_CONCAT(c_provider_reference ORDER BY c_transaction_id SEPARATOR ',') AS provider_references " +
        "FROM " + SOURCE_TABLE + " " +
        "WHERE c_statement_id = ? " +
        "GROUP BY c_account_number, c_document_nr, c_payment_date, c_other_side_account, " +
        "         c_other_side_name, c_other_side_bank, c_d_c, c_payment_description, " +
        "         c_currency, c_customer_id, c_other_side_bic " +
        "ORDER BY c_payment_date, c_d_c, c_other_side_name";

    /**
     * Column names in INSERT order for the target table.
     * Must match the order of placeholders in INSERT_SQL.
     */
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
        "c_status, " +
        "dateCreated, " +
        "createdBy" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
        // GROUP BY columns (11 identifying fields)
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
        return row;
    }

    private BankConsolidationQuery() {
        // utility class — no instantiation
    }
}
