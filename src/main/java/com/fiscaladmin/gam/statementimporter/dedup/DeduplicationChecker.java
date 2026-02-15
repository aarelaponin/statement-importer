package com.fiscaladmin.gam.statementimporter.dedup;

import org.joget.apps.app.service.AppUtil;
import org.joget.commons.util.LogUtil;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * Checks which parsed CSV rows already exist in the database
 * (from overlapping statement uploads).
 * <p>
 * De-duplication uses a hybrid key strategy:
 * <ul>
 *   <li><b>Bank:</b> Primary key: {@code provider_reference} (parsed index 17).
 *       Fallback (if empty): composite of {@code account_number + document_nr +
 *       payment_date + payment_amount + currency}.</li>
 *   <li><b>Securities:</b> Primary key: {@code reference} (parsed index 11).
 *       Fallback (if empty): composite of {@code value_date + transaction_date +
 *       type + ticker + amount + currency}.</li>
 * </ul>
 * <p>
 * The check is performed in two levels:
 * <ol>
 *   <li>Find overlapping statements by date range</li>
 *   <li>Collect dedup keys from raw rows of those overlapping statements</li>
 * </ol>
 * Rows whose dedup key matches an existing key are classified as duplicates
 * and excluded from the result.
 */
public class DeduplicationChecker {

    private static final String CLASS_NAME = DeduplicationChecker.class.getName();

    // --- Bank dedup indices (after field-6 stripping) ---
    static final int BANK_PRIMARY_KEY_INDEX = 17;  // provider_reference
    static final int[] BANK_COMPOSITE_INDICES = {0, 1, 2, 7, 12};
    // account_number, document_nr, payment_date, payment_amount, currency

    // --- Securities dedup indices ---
    static final int SECU_PRIMARY_KEY_INDEX = 11;  // reference
    static final int[] SECU_COMPOSITE_INDICES = {0, 1, 2, 3, 8, 7};
    // value_date, transaction_date, type, ticker, amount, currency

    /**
     * Checks which parsed CSV rows already exist in the database.
     * <p>
     * This is the public entry point called by {@code StatementImporter}.
     * It loads existing dedup keys from overlapping statements via JDBC,
     * then delegates to the testable overload.
     *
     * @param rows        all parsed CSV rows
     * @param statementId current statement ID (excluded from overlap search)
     * @param fromDate    statement period start (yyyy-MM-dd)
     * @param toDate      statement period end (yyyy-MM-dd)
     * @param accountType "bank" or "secu"
     * @return DeduplicationResult with non-duplicate rows and duplicate count
     */
    public static DeduplicationResult check(
            List<String[]> rows,
            String statementId,
            String fromDate,
            String toDate,
            String accountType) {

        LogUtil.info(CLASS_NAME, "De-duplication check: " + rows.size()
                + " rows, accountType=" + accountType + ", statementId=" + statementId);

        if (rows.isEmpty()) {
            LogUtil.info(CLASS_NAME, "No rows to check — returning empty result");
            return new DeduplicationResult(Collections.emptyList(), 0, 0);
        }

        Set<String> existingKeys = loadExistingKeys(statementId, fromDate, toDate, accountType);
        return check(rows, accountType, existingKeys);
    }

    /**
     * Checks parsed rows against a pre-loaded set of existing dedup keys.
     * <p>
     * Package-private for unit testing without a database.
     *
     * @param rows         all parsed CSV rows
     * @param accountType  "bank" or "secu"
     * @param existingKeys set of dedup keys already present in the database
     * @return DeduplicationResult with non-duplicate rows and duplicate count
     */
    static DeduplicationResult check(
            List<String[]> rows,
            String accountType,
            Set<String> existingKeys) {

        int primaryKeyIndex;
        int[] compositeKeyIndices;

        if ("bank".equals(accountType)) {
            primaryKeyIndex = BANK_PRIMARY_KEY_INDEX;
            compositeKeyIndices = BANK_COMPOSITE_INDICES;
        } else {
            primaryKeyIndex = SECU_PRIMARY_KEY_INDEX;
            compositeKeyIndices = SECU_COMPOSITE_INDICES;
        }

        List<String[]> nonDuplicateRows = new ArrayList<>();
        int duplicateCount = 0;

        for (String[] row : rows) {
            String key = extractKey(row, primaryKeyIndex, compositeKeyIndices);
            if (existingKeys.contains(key)) {
                duplicateCount++;
            } else {
                nonDuplicateRows.add(row);
            }
        }

        LogUtil.info(CLASS_NAME, "De-duplication result: " + duplicateCount + " duplicates, "
                + nonDuplicateRows.size() + " new rows out of " + rows.size() + " total");

        return new DeduplicationResult(nonDuplicateRows, duplicateCount, rows.size());
    }

    /**
     * Extracts the dedup key for a single parsed row.
     * <p>
     * Uses the primary key field if it is non-empty; otherwise falls back
     * to a composite key built from multiple fields separated by '|'.
     *
     * @param row                 parsed CSV row
     * @param primaryKeyIndex     index of the primary key field
     * @param compositeKeyIndices indices of the composite key fields
     * @return the dedup key string
     */
    static String extractKey(String[] row, int primaryKeyIndex, int[] compositeKeyIndices) {
        // Try primary key first
        if (primaryKeyIndex < row.length) {
            String primaryKey = row[primaryKeyIndex];
            if (primaryKey != null && !primaryKey.trim().isEmpty()) {
                return primaryKey.trim();
            }
        }

        // Fallback: composite key
        StringBuilder sb = new StringBuilder();
        for (int idx : compositeKeyIndices) {
            if (idx < row.length && row[idx] != null) {
                sb.append(row[idx].trim());
            }
            sb.append("|");
        }
        return sb.toString();
    }

    /**
     * Loads existing dedup keys from the database for overlapping statements.
     * <p>
     * Level 1: Find statement IDs with overlapping date ranges.<br>
     * Level 2: Collect primary and composite dedup keys from raw rows
     * belonging to those statements.
     *
     * @return set of existing dedup keys; empty set if no overlaps found
     */
    private static Set<String> loadExistingKeys(
            String statementId, String fromDate, String toDate, String accountType) {

        Set<String> existingKeys = new HashSet<>();

        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");

        try (Connection con = ds.getConnection()) {

            // Level 1 — Find overlapping statement IDs
            List<String> overlappingIds = findOverlappingStatements(
                    con, statementId, fromDate, toDate, accountType);

            if (overlappingIds.isEmpty()) {
                LogUtil.info(CLASS_NAME, "No overlapping statements found — all rows are new");
                return existingKeys;
            }

            LogUtil.info(CLASS_NAME, "Found " + overlappingIds.size() + " overlapping statements");

            // Level 2 — Collect dedup keys from overlapping statement rows
            if ("bank".equals(accountType)) {
                collectBankKeys(con, overlappingIds, existingKeys);
            } else {
                collectSecuKeys(con, overlappingIds, existingKeys);
            }

            LogUtil.info(CLASS_NAME, "Loaded " + existingKeys.size() + " existing dedup keys");

        } catch (SQLException e) {
            LogUtil.error(CLASS_NAME, e, "Database error during de-duplication key loading");
            throw new RuntimeException("De-duplication failed: " + e.getMessage(), e);
        }

        return existingKeys;
    }

    /**
     * Finds statement IDs that overlap with the given date range.
     */
    private static List<String> findOverlappingStatements(
            Connection con, String statementId, String fromDate, String toDate,
            String accountType) throws SQLException {

        String sql = "SELECT id FROM app_fd_bank_statement "
                + "WHERE c_account_type = ? "
                + "AND c_status NOT IN ('error', 'new') "
                + "AND c_from_date <= ? "
                + "AND c_to_date >= ? "
                + "AND id != ?";

        List<String> ids = new ArrayList<>();
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, accountType);
            ps.setString(2, toDate);
            ps.setString(3, fromDate);
            ps.setString(4, statementId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getString("id"));
                }
            }
        }
        return ids;
    }

    /**
     * Collects bank dedup keys: primary keys (provider_reference) and
     * composite keys (for rows where provider_reference is empty).
     */
    private static void collectBankKeys(
            Connection con, List<String> overlappingIds, Set<String> keys) throws SQLException {

        String inClause = buildInClause(overlappingIds.size());

        // Primary keys
        String sqlPrimary = "SELECT c_provider_reference FROM app_fd_bank_account_trx "
                + "WHERE c_statement_id IN (" + inClause + ") "
                + "AND c_provider_reference IS NOT NULL "
                + "AND c_provider_reference != ''";

        try (PreparedStatement ps = con.prepareStatement(sqlPrimary)) {
            setInParams(ps, overlappingIds, 1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String val = rs.getString(1);
                    if (val != null) {
                        keys.add(val.trim());
                    }
                }
            }
        }

        // Composite keys (for rows missing provider_reference)
        String sqlComposite = "SELECT CONCAT("
                + "IFNULL(c_account_number,''), '|', "
                + "IFNULL(c_document_nr,''), '|', "
                + "IFNULL(c_payment_date,''), '|', "
                + "IFNULL(c_payment_amount,''), '|', "
                + "IFNULL(c_currency,''), '|') "
                + "FROM app_fd_bank_account_trx "
                + "WHERE c_statement_id IN (" + inClause + ") "
                + "AND (c_provider_reference IS NULL OR c_provider_reference = '')";

        try (PreparedStatement ps = con.prepareStatement(sqlComposite)) {
            setInParams(ps, overlappingIds, 1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String val = rs.getString(1);
                    if (val != null) {
                        keys.add(val);
                    }
                }
            }
        }
    }

    /**
     * Collects securities dedup keys: primary keys (reference) and
     * composite keys (for rows where reference is empty).
     */
    private static void collectSecuKeys(
            Connection con, List<String> overlappingIds, Set<String> keys) throws SQLException {

        String inClause = buildInClause(overlappingIds.size());

        // Primary keys
        String sqlPrimary = "SELECT c_reference FROM app_fd_sec_account_trx "
                + "WHERE c_statement_id IN (" + inClause + ") "
                + "AND c_reference IS NOT NULL "
                + "AND c_reference != ''";

        try (PreparedStatement ps = con.prepareStatement(sqlPrimary)) {
            setInParams(ps, overlappingIds, 1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String val = rs.getString(1);
                    if (val != null) {
                        keys.add(val.trim());
                    }
                }
            }
        }

        // Composite keys (for rows missing reference)
        String sqlComposite = "SELECT CONCAT("
                + "IFNULL(c_value_date,''), '|', "
                + "IFNULL(c_transaction_date,''), '|', "
                + "IFNULL(c_type,''), '|', "
                + "IFNULL(c_ticker,''), '|', "
                + "IFNULL(c_amount,''), '|', "
                + "IFNULL(c_currency,''), '|') "
                + "FROM app_fd_sec_account_trx "
                + "WHERE c_statement_id IN (" + inClause + ") "
                + "AND (c_reference IS NULL OR c_reference = '')";

        try (PreparedStatement ps = con.prepareStatement(sqlComposite)) {
            setInParams(ps, overlappingIds, 1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String val = rs.getString(1);
                    if (val != null) {
                        keys.add(val);
                    }
                }
            }
        }
    }

    /**
     * Builds a SQL IN clause placeholder string: "?, ?, ?" for the given count.
     */
    private static String buildInClause(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        return sb.toString();
    }

    /**
     * Sets parameter values for an IN clause in a PreparedStatement.
     *
     * @param ps         the prepared statement
     * @param values     the values to bind
     * @param startIndex the 1-based parameter index to start at
     */
    private static void setInParams(PreparedStatement ps, List<String> values, int startIndex)
            throws SQLException {
        for (int i = 0; i < values.size(); i++) {
            ps.setString(startIndex + i, values.get(i));
        }
    }
}
