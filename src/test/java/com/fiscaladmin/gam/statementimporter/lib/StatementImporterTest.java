package com.fiscaladmin.gam.statementimporter.lib;

import com.fiscaladmin.gam.statementimporter.parser.Format;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Lightweight unit tests for {@link StatementImporter}.
 * <p>
 * These tests verify plugin metadata, error handling paths, and helper
 * methods without requiring a running Joget instance. Full integration
 * testing (with FormDataDao, FileUtil, StatusManager) is deferred to
 * T2.10 and Track 3.
 */
public class StatementImporterTest {

    // ─────────────────────────────────────────────────────────────────
    //  Plugin metadata
    // ─────────────────────────────────────────────────────────────────

    @Test
    public void pluginMetadataIsCorrect() {
        StatementImporter plugin = new StatementImporter();

        assertEquals("Statement Importer", plugin.getName());
        assertEquals("8.1-SNAPSHOT", plugin.getVersion());
        assertNotNull(plugin.getDescription());
        assertFalse(plugin.getDescription().isEmpty());
        assertEquals("Statement Importer", plugin.getLabel());
        assertTrue(plugin.getClassName().contains("StatementImporter"));
    }

    // ─────────────────────────────────────────────────────────────────
    //  Error handling paths — execute() with missing inputs
    // ─────────────────────────────────────────────────────────────────

    @Test
    public void executeWithNullRecordIdReturnsNullGracefully() {
        StatementImporter plugin = new StatementImporter();
        Map<String, Object> properties = new HashMap<>();
        // No recordId set — should trigger IllegalArgumentException
        // which is caught by the error handler. Since recordId is null,
        // the error handler skips StatusManager calls and just sets
        // the workflow variable (which silently does nothing without
        // a workflowAssignment).

        Object result = plugin.execute(properties);
        assertNull(result);
    }

    @Test
    public void executeWithEmptyRecordIdReturnsNullGracefully() {
        StatementImporter plugin = new StatementImporter();
        Map<String, Object> properties = new HashMap<>();
        properties.put("recordId", "");

        Object result = plugin.execute(properties);
        assertNull(result);
    }

    @Test
    public void executeWithNullAccountTypeHandledGracefully() {
        StatementImporter plugin = new StatementImporter();
        Map<String, Object> properties = new HashMap<>();
        properties.put("recordId", "test-id");
        // No account_type — should trigger IllegalArgumentException
        // Error handler will try StatusManager (which will fail without
        // Spring context) and then fall through gracefully.

        Object result = plugin.execute(properties);
        assertNull(result);
    }

    // ─────────────────────────────────────────────────────────────────
    //  truncateErrorMessage helper
    // ─────────────────────────────────────────────────────────────────

    @Test
    public void truncateErrorMessageShortMessageUnchanged() {
        StatementImporter plugin = new StatementImporter();
        String shortMsg = "Something went wrong";
        assertEquals(shortMsg, plugin.truncateErrorMessage(shortMsg));
    }

    @Test
    public void truncateErrorMessageExactly1000Unchanged() {
        StatementImporter plugin = new StatementImporter();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("x");
        }
        String msg = sb.toString();
        assertEquals(1000, plugin.truncateErrorMessage(msg).length());
        assertEquals(msg, plugin.truncateErrorMessage(msg));
    }

    @Test
    public void truncateErrorMessageLongMessageTruncatedTo1000() {
        StatementImporter plugin = new StatementImporter();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1500; i++) {
            sb.append("y");
        }
        String result = plugin.truncateErrorMessage(sb.toString());
        assertEquals(1000, result.length());
    }

    @Test
    public void truncateErrorMessageNullReturnsDefault() {
        StatementImporter plugin = new StatementImporter();
        assertEquals("Unknown error", plugin.truncateErrorMessage(null));
    }

    // ─────────────────────────────────────────────────────────────────
    //  Format account type validation contract
    // ─────────────────────────────────────────────────────────────────

    @Test
    public void formatAccountTypeMismatchDetected() {
        // Documents the validation contract: LHV_BANK has accountType "bank"
        assertEquals("bank", Format.LHV_BANK.getAccountType());
        assertEquals("bank", Format.SWEDBANK.getAccountType());
        assertEquals("secu", Format.SECURITIES.getAccountType());

        // If user selects "secu" but uploads a bank file, the mismatch check triggers
        String detectedType = Format.LHV_BANK.getAccountType();
        String declaredType = "secu";
        assertNotEquals(detectedType, declaredType);

        // Matching case — no error
        String matchedType = "bank";
        assertEquals(detectedType, matchedType);
    }

    @Test
    public void formatTargetTablesAreCorrect() {
        assertEquals("bank_account_trx", Format.LHV_BANK.getTargetTable());
        assertEquals("bank_account_trx", Format.SWEDBANK.getTargetTable());
        assertEquals("sec_account_trx", Format.SECURITIES.getTargetTable());
    }
}
