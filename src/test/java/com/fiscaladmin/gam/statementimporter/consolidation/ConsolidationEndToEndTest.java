package com.fiscaladmin.gam.statementimporter.consolidation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * End-to-end integration tests for the consolidation pipeline.
 * <p>
 * Tests the full flow: parse CSV → persist raw → consolidate → verify results.
 */
public class ConsolidationEndToEndTest extends ConsolidationTestBase {

    @Before
    public void setUp() throws Exception {
        setUpDatabase();
    }

    @After
    public void tearDown() throws Exception {
        tearDownDatabase();
    }

    // -------------------------------------------------------------------------
    // Test 1: Full bank consolidation pipeline
    // -------------------------------------------------------------------------

    @Test
    public void bankFullPipeline() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: bankFullPipeline — bank_statement.csv not found");
            return;
        }

        String statementId = "E2E-BANK-001";

        // Step 1: Load and persist raw rows
        int rawCount = loadAndPersistBankCsv(statementId);
        assertEquals("Should load 161 raw rows", 161, rawCount);

        // Step 2: Execute GROUP BY
        List<Map<String, String>> consolidated = executeBankGroupBy(statementId);
        assertTrue("Should have consolidated rows", consolidated.size() > 0);
        assertTrue("Consolidated count should be less than raw", consolidated.size() < rawCount);

        // Step 3: Delete existing (idempotency prep)
        deleteConsolidatedBankRows(statementId);

        // Step 4: Insert consolidated rows
        int insertedCount = ConsolidatedRowPersister.persist(
            consolidated,
            statementId,
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        assertEquals("Insert count should match consolidated count", consolidated.size(), insertedCount);

        // Step 5: Verify database state
        assertEquals("DB should have correct row count",
            insertedCount, countRowsWithStatementId(BankConsolidationQuery.TARGET_TABLE, statementId));

        // Verify all rows have status = 'new'
        int nonNewCount = countNonNewStatusRows(BankConsolidationQuery.TARGET_TABLE, statementId);
        assertEquals("All rows should have status='new'", 0, nonNewCount);

        System.out.println("Bank E2E: " + rawCount + " raw → " + insertedCount + " consolidated");
    }

    // -------------------------------------------------------------------------
    // Test 2: Full securities consolidation pipeline
    // -------------------------------------------------------------------------

    @Test
    public void secuFullPipeline() throws Exception {
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: secuFullPipeline — secu_statement.csv not found");
            return;
        }

        String statementId = "E2E-SECU-001";

        // Step 1: Load and persist raw rows
        int rawCount = loadAndPersistSecuCsv(statementId);
        assertEquals("Should load 45 raw rows", 45, rawCount);

        // Step 2: Execute GROUP BY
        List<Map<String, String>> consolidated = executeSecuGroupBy(statementId);
        assertTrue("Should have consolidated rows", consolidated.size() > 0);
        assertTrue("Consolidated count should be less than raw (due to LHV1T merging)",
            consolidated.size() < rawCount);

        // Step 3: Delete existing (idempotency prep)
        deleteConsolidatedSecuRows(statementId);

        // Step 4: Insert consolidated rows
        int insertedCount = ConsolidatedRowPersister.persist(
            consolidated,
            statementId,
            "STMT2024",
            SecuConsolidationQuery.INSERT_SQL,
            SecuConsolidationQuery.INSERT_COLUMNS,
            SecuConsolidationQuery.TARGET_TABLE,
            con
        );

        assertEquals("Insert count should match consolidated count", consolidated.size(), insertedCount);

        // Step 5: Verify database state
        assertEquals("DB should have correct row count",
            insertedCount, countRowsWithStatementId(SecuConsolidationQuery.TARGET_TABLE, statementId));

        System.out.println("Securities E2E: " + rawCount + " raw → " + insertedCount + " consolidated");
    }

    // -------------------------------------------------------------------------
    // Test 3: Idempotency — run twice, same result
    // -------------------------------------------------------------------------

    @Test
    public void idempotencyBankRunTwice() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: idempotencyBankRunTwice — bank_statement.csv not found");
            return;
        }

        String statementId = "E2E-IDEMP-BANK";

        // Load raw data once
        loadAndPersistBankCsv(statementId);

        // First consolidation
        runBankConsolidation(statementId);
        int countAfterFirst = countRowsWithStatementId(BankConsolidationQuery.TARGET_TABLE, statementId);

        // Second consolidation (should delete and re-insert)
        runBankConsolidation(statementId);
        int countAfterSecond = countRowsWithStatementId(BankConsolidationQuery.TARGET_TABLE, statementId);

        assertEquals("Row count should be same after idempotent re-run", countAfterFirst, countAfterSecond);
    }

    @Test
    public void idempotencySecuRunTwice() throws Exception {
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: idempotencySecuRunTwice — secu_statement.csv not found");
            return;
        }

        String statementId = "E2E-IDEMP-SECU";

        // Load raw data once
        loadAndPersistSecuCsv(statementId);

        // First consolidation
        runSecuConsolidation(statementId);
        int countAfterFirst = countRowsWithStatementId(SecuConsolidationQuery.TARGET_TABLE, statementId);

        // Second consolidation (should delete and re-insert)
        runSecuConsolidation(statementId);
        int countAfterSecond = countRowsWithStatementId(SecuConsolidationQuery.TARGET_TABLE, statementId);

        assertEquals("Row count should be same after idempotent re-run", countAfterFirst, countAfterSecond);
    }

    // -------------------------------------------------------------------------
    // Test 4: Bank and securities isolation
    // -------------------------------------------------------------------------

    @Test
    public void bankAndSecuIsolation() throws Exception {
        File bankFile = new File(BANK_CSV);
        File secuFile = new File(SECU_CSV);
        if (!bankFile.exists() || !secuFile.exists()) {
            System.out.println("SKIP: bankAndSecuIsolation — CSV files not found");
            return;
        }

        // Load and consolidate both
        String bankStmtId = "ISOLATION-BANK";
        String secuStmtId = "ISOLATION-SECU";

        loadAndPersistBankCsv(bankStmtId);
        loadAndPersistSecuCsv(secuStmtId);

        runBankConsolidation(bankStmtId);
        runSecuConsolidation(secuStmtId);

        // Verify bank table has no secu rows
        assertEquals("Bank consolidated table should have no secu rows",
            0, countRowsWithStatementId(BankConsolidationQuery.TARGET_TABLE, secuStmtId));

        // Verify secu table has no bank rows
        assertEquals("Secu consolidated table should have no bank rows",
            0, countRowsWithStatementId(SecuConsolidationQuery.TARGET_TABLE, bankStmtId));

        // Verify each table has correct statement rows
        assertTrue("Bank consolidated table should have rows",
            countRowsWithStatementId(BankConsolidationQuery.TARGET_TABLE, bankStmtId) > 0);
        assertTrue("Secu consolidated table should have rows",
            countRowsWithStatementId(SecuConsolidationQuery.TARGET_TABLE, secuStmtId) > 0);
    }

    // -------------------------------------------------------------------------
    // Test 5: Statement reference uniqueness
    // -------------------------------------------------------------------------

    @Test
    public void statementReferencesUnique() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: statementReferencesUnique — bank_statement.csv not found");
            return;
        }

        String statementId = "E2E-UNIQUE-REF";

        loadAndPersistBankCsv(statementId);
        runBankConsolidation(statementId);

        // Get all statement references
        List<String> refs = getColumnValues(BankConsolidationQuery.TARGET_TABLE,
            "c_statement_reference", "c_statement_reference");

        // Check uniqueness
        long uniqueCount = refs.stream().distinct().count();
        assertEquals("All statement references should be unique", refs.size(), uniqueCount);
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    private List<Map<String, String>> executeBankGroupBy(String statementId) throws SQLException {
        List<Map<String, String>> results = new ArrayList<>();
        try (PreparedStatement ps = con.prepareStatement(BankConsolidationQuery.GROUP_BY_SQL)) {
            ps.setString(1, statementId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(BankConsolidationQuery.readRow(rs));
                }
            }
        }
        return results;
    }

    private List<Map<String, String>> executeSecuGroupBy(String statementId) throws SQLException {
        List<Map<String, String>> results = new ArrayList<>();
        try (PreparedStatement ps = con.prepareStatement(SecuConsolidationQuery.GROUP_BY_SQL)) {
            ps.setString(1, statementId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(SecuConsolidationQuery.readRow(rs));
                }
            }
        }
        return results;
    }

    private void deleteConsolidatedBankRows(String statementId) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(BankConsolidationQuery.DELETE_SQL)) {
            ps.setString(1, statementId);
            ps.executeUpdate();
        }
    }

    private void deleteConsolidatedSecuRows(String statementId) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(SecuConsolidationQuery.DELETE_SQL)) {
            ps.setString(1, statementId);
            ps.executeUpdate();
        }
    }

    private void runBankConsolidation(String statementId) throws SQLException {
        deleteConsolidatedBankRows(statementId);
        List<Map<String, String>> consolidated = executeBankGroupBy(statementId);
        ConsolidatedRowPersister.persist(
            consolidated,
            statementId,
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );
    }

    private void runSecuConsolidation(String statementId) throws SQLException {
        deleteConsolidatedSecuRows(statementId);
        List<Map<String, String>> consolidated = executeSecuGroupBy(statementId);
        ConsolidatedRowPersister.persist(
            consolidated,
            statementId,
            "STMT2024",
            SecuConsolidationQuery.INSERT_SQL,
            SecuConsolidationQuery.INSERT_COLUMNS,
            SecuConsolidationQuery.TARGET_TABLE,
            con
        );
    }

    private int countNonNewStatusRows(String table, String statementId) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COUNT(*) FROM " + table + " WHERE c_statement_id = ? AND c_status != 'new'")) {
            ps.setString(1, statementId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}
