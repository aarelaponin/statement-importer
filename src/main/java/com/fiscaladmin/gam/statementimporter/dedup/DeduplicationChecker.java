package com.fiscaladmin.gam.statementimporter.dedup;

import java.util.List;

/**
 * Checks which parsed CSV rows already exist in the database
 * (from overlapping statement uploads).
 * <p>
 * De-duplication keys:
 * <ul>
 *   <li><b>Bank:</b> {@code provider_reference} (last field). If empty, hash of full row.</li>
 *   <li><b>Securities:</b> {@code reference} field.</li>
 * </ul>
 * Queries existing rows from overlapping statement periods and compares
 * dedup keys to identify duplicates.
 */
public class DeduplicationChecker {

    /**
     * Checks which parsed CSV rows already exist in the database.
     *
     * @param rows        all parsed CSV rows
     * @param statementId current statement ID
     * @param fromDate    statement period start
     * @param toDate      statement period end
     * @param accountType "bank" or "secu"
     * @return DeduplicationResult with non-duplicate rows and duplicate count
     */
    public static DeduplicationResult check(
            List<String[]> rows,
            String statementId,
            String fromDate,
            String toDate,
            String accountType) {
        // TODO: T2.5 — Implement deduplication logic
        throw new UnsupportedOperationException("Not yet implemented — see T2.5");
    }
}
