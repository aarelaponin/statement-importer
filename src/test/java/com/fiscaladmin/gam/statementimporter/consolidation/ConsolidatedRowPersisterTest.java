package com.fiscaladmin.gam.statementimporter.consolidation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ConsolidatedRowPersister}.
 * <p>
 * Tests the batch insert logic for consolidated transaction rows.
 */
public class ConsolidatedRowPersisterTest extends ConsolidationTestBase {

    @Before
    public void setUp() throws Exception {
        setUpDatabase();
    }

    @After
    public void tearDown() throws Exception {
        tearDownDatabase();
    }

    // -------------------------------------------------------------------------
    // Bank INSERT columns match schema
    // -------------------------------------------------------------------------

    @Test
    public void bankInsertColumnsMatchSchema() throws SQLException {
        Map<String, String> row = new HashMap<>();
        row.put("c_account_number", "EE123456789012345678");
        row.put("c_document_nr", "DOC001");
        row.put("c_payment_date", "2024-06-01");
        row.put("c_other_side_account", "EE987654321098765432");
        row.put("c_other_side_name", "Test Recipient");
        row.put("c_other_side_bank", "LHVBEE22");
        row.put("c_d_c", "C");
        row.put("c_payment_description", "Test payment");
        row.put("c_currency", "EUR");
        row.put("c_customer_id", "CUST001");
        row.put("c_other_side_bic", "HABAEE2X");
        row.put("c_payment_amount", "100.00");
        row.put("c_transaction_fee", "0.50");
        row.put("c_provider_reference", "REF001,REF002");

        List<Map<String, String>> rows = Collections.singletonList(row);

        int inserted = ConsolidatedRowPersister.persist(
            rows,
            "STMT-SCHEMA-BANK",
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        assertEquals("Should insert 1 row", 1, inserted);
        assertEquals(1, countRowsWithStatementId("app_fd_bank_total_trx", "STMT-SCHEMA-BANK"));
    }

    // -------------------------------------------------------------------------
    // Securities INSERT columns match schema
    // -------------------------------------------------------------------------

    @Test
    public void secuInsertColumnsMatchSchema() throws SQLException {
        Map<String, String> row = new HashMap<>();
        row.put("c_value_date", "2024-06-01");
        row.put("c_transaction_date", "2024-06-01");
        row.put("c_type", "ost");
        row.put("c_ticker", "AAPL");
        row.put("c_description", "Buy Apple");
        row.put("c_currency", "USD");
        row.put("c_quantity", "100");
        row.put("c_price", "185.50");
        row.put("c_amount", "-18550.00");
        row.put("c_fee", "-25.00");
        row.put("c_total_amount", "-18575.00");
        row.put("c_reference", "REF001");

        List<Map<String, String>> rows = Collections.singletonList(row);

        int inserted = ConsolidatedRowPersister.persist(
            rows,
            "STMT-SCHEMA-SECU",
            "STMT2024",
            SecuConsolidationQuery.INSERT_SQL,
            SecuConsolidationQuery.INSERT_COLUMNS,
            SecuConsolidationQuery.TARGET_TABLE,
            con
        );

        assertEquals("Should insert 1 row", 1, inserted);
        assertEquals(1, countRowsWithStatementId("app_fd_secu_total_trx", "STMT-SCHEMA-SECU"));
    }

    // -------------------------------------------------------------------------
    // Batch insert multiple rows
    // -------------------------------------------------------------------------

    @Test
    public void batchInsertMultipleRows() throws SQLException {
        List<Map<String, String>> rows = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Map<String, String> row = new HashMap<>();
            row.put("c_account_number", "EE123456789012345678");
            row.put("c_document_nr", "DOC00" + i);
            row.put("c_payment_date", "2024-06-0" + (i + 1));
            row.put("c_other_side_account", "EE987654321098765432");
            row.put("c_other_side_name", "Recipient " + i);
            row.put("c_other_side_bank", "LHVBEE22");
            row.put("c_d_c", i % 2 == 0 ? "C" : "D");
            row.put("c_payment_description", "Payment " + i);
            row.put("c_currency", "EUR");
            row.put("c_customer_id", "CUST001");
            row.put("c_other_side_bic", "HABAEE2X");
            row.put("c_payment_amount", String.valueOf(100 + i * 10));
            row.put("c_transaction_fee", "0.00");
            row.put("c_provider_reference", "REF" + i);
            rows.add(row);
        }

        int inserted = ConsolidatedRowPersister.persist(
            rows,
            "STMT-BATCH",
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        assertEquals("Should insert 5 rows", 5, inserted);
        assertEquals(5, countRowsWithStatementId("app_fd_bank_total_trx", "STMT-BATCH"));
    }

    // -------------------------------------------------------------------------
    // Statement reference sequence
    // -------------------------------------------------------------------------

    @Test
    public void statementReferenceSequence() throws SQLException {
        List<Map<String, String>> rows = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            Map<String, String> row = new HashMap<>();
            row.put("c_account_number", "EE123456789012345678");
            row.put("c_document_nr", "DOC00" + i);
            row.put("c_payment_date", "2024-06-01");
            row.put("c_other_side_account", "EE987654321098765432");
            row.put("c_other_side_name", "Recipient");
            row.put("c_other_side_bank", "LHVBEE22");
            row.put("c_d_c", "C");
            row.put("c_payment_description", "Payment");
            row.put("c_currency", "EUR");
            row.put("c_customer_id", "CUST001");
            row.put("c_other_side_bic", "HABAEE2X");
            row.put("c_payment_amount", "100.00");
            row.put("c_transaction_fee", "0.00");
            row.put("c_provider_reference", "REF" + i);
            rows.add(row);
        }

        ConsolidatedRowPersister.persist(
            rows,
            "STMT-SEQ",
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        List<String> refs = getColumnValues("app_fd_bank_total_trx", "c_statement_reference", "c_statement_reference");
        assertEquals(3, refs.size());
        assertEquals("STMT2024.001", refs.get(0));
        assertEquals("STMT2024.002", refs.get(1));
        assertEquals("STMT2024.003", refs.get(2));
    }

    // -------------------------------------------------------------------------
    // UUID generation
    // -------------------------------------------------------------------------

    @Test
    public void uuidGeneration() throws SQLException {
        Map<String, String> row = new HashMap<>();
        row.put("c_account_number", "EE123456789012345678");
        row.put("c_document_nr", "DOC001");
        row.put("c_payment_date", "2024-06-01");
        row.put("c_other_side_account", "EE987654321098765432");
        row.put("c_other_side_name", "Test");
        row.put("c_other_side_bank", "LHVBEE22");
        row.put("c_d_c", "C");
        row.put("c_payment_description", "Test payment");
        row.put("c_currency", "EUR");
        row.put("c_customer_id", "CUST001");
        row.put("c_other_side_bic", "HABAEE2X");
        row.put("c_payment_amount", "100.00");
        row.put("c_transaction_fee", "0.00");
        row.put("c_provider_reference", "REF001");

        ConsolidatedRowPersister.persist(
            Collections.singletonList(row),
            "STMT-UUID",
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        String id = getColumnValue("app_fd_bank_total_trx", "id", "c_statement_id = 'STMT-UUID'");
        assertNotNull("ID should not be null", id);
        assertEquals("ID should be UUID format (36 chars)", 36, id.length());
        assertTrue("ID should contain dashes", id.contains("-"));
    }

    // -------------------------------------------------------------------------
    // Audit fields
    // -------------------------------------------------------------------------

    @Test
    public void auditFields() throws SQLException {
        Map<String, String> row = new HashMap<>();
        row.put("c_account_number", "EE123456789012345678");
        row.put("c_document_nr", "DOC001");
        row.put("c_payment_date", "2024-06-01");
        row.put("c_other_side_account", "EE987654321098765432");
        row.put("c_other_side_name", "Test");
        row.put("c_other_side_bank", "LHVBEE22");
        row.put("c_d_c", "C");
        row.put("c_payment_description", "Test payment");
        row.put("c_currency", "EUR");
        row.put("c_customer_id", "CUST001");
        row.put("c_other_side_bic", "HABAEE2X");
        row.put("c_payment_amount", "100.00");
        row.put("c_transaction_fee", "0.00");
        row.put("c_provider_reference", "REF001");

        ConsolidatedRowPersister.persist(
            Collections.singletonList(row),
            "STMT-AUDIT",
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        String dateCreated = getColumnValue("app_fd_bank_total_trx", "dateCreated", "c_statement_id = 'STMT-AUDIT'");
        String createdBy = getColumnValue("app_fd_bank_total_trx", "createdBy", "c_statement_id = 'STMT-AUDIT'");

        assertNotNull("dateCreated should not be null", dateCreated);
        assertTrue("dateCreated should match yyyy-MM-dd HH:mm:ss",
            dateCreated.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));
        assertEquals("createdBy should be 'system'", "system", createdBy);
    }

    // -------------------------------------------------------------------------
    // Status field
    // -------------------------------------------------------------------------

    @Test
    public void statusFieldIsNew() throws SQLException {
        Map<String, String> row = new HashMap<>();
        row.put("c_account_number", "EE123456789012345678");
        row.put("c_document_nr", "DOC001");
        row.put("c_payment_date", "2024-06-01");
        row.put("c_other_side_account", "EE987654321098765432");
        row.put("c_other_side_name", "Test");
        row.put("c_other_side_bank", "LHVBEE22");
        row.put("c_d_c", "C");
        row.put("c_payment_description", "Test payment");
        row.put("c_currency", "EUR");
        row.put("c_customer_id", "CUST001");
        row.put("c_other_side_bic", "HABAEE2X");
        row.put("c_payment_amount", "100.00");
        row.put("c_transaction_fee", "0.00");
        row.put("c_provider_reference", "REF001");

        ConsolidatedRowPersister.persist(
            Collections.singletonList(row),
            "STMT-STATUS",
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        String status = getColumnValue("app_fd_bank_total_trx", "c_status", "c_statement_id = 'STMT-STATUS'");
        assertEquals("Status should be 'new'", "new", status);
    }

    // -------------------------------------------------------------------------
    // Empty/null list handling
    // -------------------------------------------------------------------------

    @Test
    public void emptyListReturnsZero() throws SQLException {
        int inserted = ConsolidatedRowPersister.persist(
            Collections.emptyList(),
            "STMT-EMPTY",
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        assertEquals(0, inserted);
    }

    @Test
    public void nullListReturnsZero() throws SQLException {
        int inserted = ConsolidatedRowPersister.persist(
            null,
            "STMT-NULL",
            "STMT2024",
            BankConsolidationQuery.INSERT_SQL,
            BankConsolidationQuery.INSERT_COLUMNS,
            BankConsolidationQuery.TARGET_TABLE,
            con
        );

        assertEquals(0, inserted);
    }
}
