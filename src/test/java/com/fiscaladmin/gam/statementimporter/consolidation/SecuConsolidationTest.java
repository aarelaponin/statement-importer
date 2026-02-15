package com.fiscaladmin.gam.statementimporter.consolidation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for securities transaction consolidation.
 * <p>
 * Tests the GROUP BY query and consolidation logic for securities transactions,
 * including LHV1T partial fill merging.
 */
public class SecuConsolidationTest extends ConsolidationTestBase {

    private static final String STATEMENT_ID = "TEST-SECU-CONSOL";

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
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: groupByReducesRowCount — secu_statement.csv not found");
            return;
        }

        // Load 45 raw rows
        int rawCount = loadAndPersistSecuCsv(STATEMENT_ID);
        assertEquals("Should load 45 raw rows", 45, rawCount);

        // Execute GROUP BY
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        // GROUP BY should produce fewer rows than raw (due to LHV1T merging)
        assertTrue("GROUP BY should reduce row count", consolidated.size() < rawCount);
        assertTrue("Should have at least 1 consolidated row", consolidated.size() > 0);

        System.out.println("Securities consolidation: " + rawCount + " raw rows → " + consolidated.size() + " consolidated rows");
    }

    // -------------------------------------------------------------------------
    // LHV1T fragments merged
    // -------------------------------------------------------------------------

    @Test
    public void lhv1tFragmentsMerged() throws Exception {
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: lhv1tFragmentsMerged — secu_statement.csv not found");
            return;
        }

        loadAndPersistSecuCsv(STATEMENT_ID);
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        ConsolidatedRowPersister.persist(
            consolidated,
            STATEMENT_ID,
            "STMT2024",
            SecuConsolidationQuery.INSERT_SQL,
            SecuConsolidationQuery.INSERT_COLUMNS,
            SecuConsolidationQuery.TARGET_TABLE,
            con
        );

        // Find LHV1T buy (ost) row — should be merged from 6 fragments:
        // 257 + 500 + 1000 + 10 + 20 + 513 = 2300
        // Amounts: -899.50 + -1750.00 + -3500.00 + -35.00 + -70.00 + -1795.50 = -8050.00
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_quantity, c_amount, c_reference " +
                "FROM app_fd_secu_total_trx WHERE c_ticker = 'LHV1T' AND c_type = 'ost'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("Should have LHV1T buy row", rs.next());

                BigDecimal qty = new BigDecimal(rs.getString("c_quantity"));
                assertEquals("LHV1T quantity should be 2300 (257+500+1000+10+20+513)",
                    0, qty.compareTo(new BigDecimal("2300")));

                BigDecimal amount = new BigDecimal(rs.getString("c_amount"));
                assertEquals("LHV1T amount should be -8050.00",
                    0, amount.compareTo(new BigDecimal("-8050.00")));

                String refs = rs.getString("c_reference");
                assertTrue("References should contain comma (6 fragments merged)",
                    refs.contains(","));
                assertTrue("References should include 903867200", refs.contains("903867200"));
                assertTrue("References should include 903867343", refs.contains("903867343"));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Price averaged
    // -------------------------------------------------------------------------

    @Test
    public void priceAveraged() throws Exception {
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: priceAveraged — secu_statement.csv not found");
            return;
        }

        loadAndPersistSecuCsv(STATEMENT_ID);
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        ConsolidatedRowPersister.persist(
            consolidated,
            STATEMENT_ID,
            "STMT2024",
            SecuConsolidationQuery.INSERT_SQL,
            SecuConsolidationQuery.INSERT_COLUMNS,
            SecuConsolidationQuery.TARGET_TABLE,
            con
        );

        // Find LHV1T buy row — price should be average of 3.50000000 (both fragments have same price)
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c_price FROM app_fd_secu_total_trx WHERE c_ticker = 'LHV1T' AND c_type = 'ost'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("Should have LHV1T buy row", rs.next());
                BigDecimal price = new BigDecimal(rs.getString("c_price"));
                // Price should be 3.5 (average of identical prices)
                assertEquals("LHV1T price should be 3.5",
                    0, price.compareTo(new BigDecimal("3.5")));
            }
        }
    }

    // -------------------------------------------------------------------------
    // All status = new
    // -------------------------------------------------------------------------

    @Test
    public void allStatusNew() throws Exception {
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: allStatusNew — secu_statement.csv not found");
            return;
        }

        loadAndPersistSecuCsv(STATEMENT_ID);
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        ConsolidatedRowPersister.persist(
            consolidated,
            STATEMENT_ID,
            "STMT2024",
            SecuConsolidationQuery.INSERT_SQL,
            SecuConsolidationQuery.INSERT_COLUMNS,
            SecuConsolidationQuery.TARGET_TABLE,
            con
        );

        // Count non-"new" status rows
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COUNT(*) FROM app_fd_secu_total_trx WHERE c_status != 'new'")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals("All consolidated rows should have status=new", 0, rs.getInt(1));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Statement reference pattern
    // -------------------------------------------------------------------------

    @Test
    public void statementReferencePattern() throws Exception {
        File secuFile = new File(SECU_CSV);
        if (!secuFile.exists()) {
            System.out.println("SKIP: statementReferencePattern — secu_statement.csv not found");
            return;
        }

        loadAndPersistSecuCsv(STATEMENT_ID);
        List<Map<String, String>> consolidated = executeGroupBy(STATEMENT_ID);

        ConsolidatedRowPersister.persist(
            consolidated,
            STATEMENT_ID,
            "STMT2024",
            SecuConsolidationQuery.INSERT_SQL,
            SecuConsolidationQuery.INSERT_COLUMNS,
            SecuConsolidationQuery.TARGET_TABLE,
            con
        );

        // Check statement references
        List<String> refs = getColumnValues("app_fd_secu_total_trx", "c_statement_reference", "c_statement_reference");
        assertTrue("Should have consolidated rows", refs.size() > 0);
        assertEquals("First reference should be STMT2024.001", "STMT2024.001", refs.get(0));

        // All references should follow pattern STMT2024.XXX
        for (String ref : refs) {
            assertTrue("Reference should start with STMT2024.", ref.startsWith("STMT2024."));
            assertTrue("Reference should have 3-digit suffix", ref.matches("STMT2024\\.\\d{3}"));
        }
    }

    // -------------------------------------------------------------------------
    // Helper method
    // -------------------------------------------------------------------------

    private List<Map<String, String>> executeGroupBy(String statementId) throws SQLException {
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
}
