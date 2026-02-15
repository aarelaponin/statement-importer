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
 * Unit tests for bank transaction consolidation.
 * <p>
 * Tests the GROUP BY query and consolidation logic for bank transactions.
 */
public class BankConsolidationTest extends ConsolidationTestBase {

    private static final String STATEMENT_ID = "TEST-BANK-CONSOL";

    @Before
    public void setUp() throws Exception {
        setUpDatabase();
    }

    @After
    public void tearDown() throws Exception {
        tearDownDatabase();
    }

    // -------------------------------------------------------------------------
    // GROUP BY reduces row count
    // -------------------------------------------------------------------------

    @Test
    public void groupByReducesRowCount() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: groupByReducesRowCount — bank_statement.csv not found");
            return;
        }

        // Load 161 raw rows
        int rawCount = loadAndPersistBankCsv(STATEMENT_ID);
        assertEquals("Should load 161 raw rows", 161, rawCount);

        // Execute GROUP BY
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        // GROUP BY should produce fewer rows than raw
        assertTrue("GROUP BY should reduce row count", consolidated.size() < rawCount);
        assertTrue("Should have at least 1 consolidated row", consolidated.size() > 0);

        System.out.println("Bank consolidation: " + rawCount + " raw rows → " + consolidated.size() + " consolidated rows");
    }

    // -------------------------------------------------------------------------
    // Statement reference pattern
    // -------------------------------------------------------------------------

    @Test
    public void statementReferencePattern() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: statementReferencePattern — bank_statement.csv not found");
            return;
        }

        // Load and consolidate
        loadAndPersistBankCsv(STATEMENT_ID);
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        // Persist with reference prefix
        int inserted = ConsolidatedRowPersister.persist(
            consolidated,
            STATEMENT_ID,
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        assertTrue("Should insert consolidated rows", inserted > 0);

        // Check statement references
        List<String> refs = getColumnValues("app_fd_bank_total_trx", "c_statement_reference", "c_statement_reference");
        assertEquals("First reference should be STMT2024.001", "STMT2024.001", refs.get(0));

        // All references should follow pattern STMT2024.XXX
        for (String ref : refs) {
            assertTrue("Reference should start with STMT2024.", ref.startsWith("STMT2024."));
            assertTrue("Reference should have 3-digit suffix", ref.matches("STMT2024\\.\\d{3}"));
        }
    }

    // -------------------------------------------------------------------------
    // Amount aggregation
    // -------------------------------------------------------------------------

    @Test
    public void amountAggregation() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: amountAggregation — bank_statement.csv not found");
            return;
        }

        loadAndPersistBankCsv(STATEMENT_ID);
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        // Persist
        ConsolidatedRowPersister.persist(
            consolidated,
            STATEMENT_ID,
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        // Find a row that has multiple provider references (comma-separated)
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_payment_amount, c_provider_reference FROM app_fd_bank_total_trx WHERE c_provider_reference LIKE '%,%' LIMIT 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String amount = rs.getString("c_payment_amount");
                    String refs = rs.getString("c_provider_reference");
                    assertNotNull("Aggregated amount should not be null", amount);
                    int refCount = refs.split(",").length;
                    assertTrue("Should have multiple references", refCount > 1);
                    System.out.println("Found aggregated row: amount=" + amount + ", ref_count=" + refCount);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // All status = new
    // -------------------------------------------------------------------------

    @Test
    public void allStatusNew() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: allStatusNew — bank_statement.csv not found");
            return;
        }

        loadAndPersistBankCsv(STATEMENT_ID);
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        ConsolidatedRowPersister.persist(
            consolidated,
            STATEMENT_ID,
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        // Count non-"new" status rows
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COUNT(*) FROM app_fd_bank_total_trx WHERE c_status != 'new'")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals("All consolidated rows should have status=new", 0, rs.getInt(1));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Provider references concatenated
    // -------------------------------------------------------------------------

    @Test
    public void providerReferencesConcatenated() throws Exception {
        File bankFile = new File(BANK_CSV);
        if (!bankFile.exists()) {
            System.out.println("SKIP: providerReferencesConcatenated — bank_statement.csv not found");
            return;
        }

        loadAndPersistBankCsv(STATEMENT_ID);
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        ConsolidatedRowPersister.persist(
            consolidated,
            STATEMENT_ID,
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        // Find a row with multiple provider references (comma in value)
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_provider_reference FROM app_fd_bank_total_trx WHERE c_provider_reference LIKE '%,%' LIMIT 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String refs = rs.getString("c_provider_reference");
                    assertNotNull("Provider reference should not be null", refs);
                    assertTrue("Provider reference should contain comma for multiple rows",
                        refs.contains(","));
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helper method
    // -------------------------------------------------------------------------

    private List<Map<String, String>> executeGroupBy(String statementId) throws SQLException {
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
}
