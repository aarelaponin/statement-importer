package com.fiscaladmin.gam.statementimporter.dedup;

import org.junit.Test;

import static org.junit.Assert.*;

public class DeduplicationCheckerTest {

    @Test
    public void emptyDatabaseReturnsAllRowsAsNew() {
        // TODO: T2.5 — All rows should be non-duplicates when DB is empty
    }

    @Test
    public void allDuplicatesReturnsEmptyList() {
        // TODO: T2.5 — All rows are duplicates, result should have 0 non-duplicate rows
    }

    @Test
    public void partialOverlapReturnCorrectSplit() {
        // TODO: T2.5 — Mix of new and duplicate rows, verify correct counts
    }
}
