package com.fiscaladmin.gam.statementimporter.dedup;

import java.util.List;

/**
 * Immutable result of a de-duplication check.
 * <p>
 * Contains the list of non-duplicate rows that should be inserted,
 * plus counts for reporting and metadata updates.
 */
public class DeduplicationResult {

    private final List<String[]> nonDuplicateRows;
    private final int duplicateCount;
    private final int totalCount;

    public DeduplicationResult(List<String[]> nonDuplicateRows, int duplicateCount, int totalCount) {
        this.nonDuplicateRows = nonDuplicateRows;
        this.duplicateCount = duplicateCount;
        this.totalCount = totalCount;
    }

    /**
     * Returns the rows that are not duplicates and should be inserted.
     */
    public List<String[]> getNonDuplicateRows() {
        return nonDuplicateRows;
    }

    /**
     * Returns the number of rows identified as duplicates.
     */
    public int getDuplicateCount() {
        return duplicateCount;
    }

    /**
     * Returns the total number of rows checked (duplicates + non-duplicates).
     */
    public int getTotalCount() {
        return totalCount;
    }
}
