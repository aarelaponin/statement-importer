package com.fiscaladmin.gam.statementimporter.lib;

import com.fiscaladmin.gam.framework.status.EntityType;
import com.fiscaladmin.gam.framework.status.Status;
import com.fiscaladmin.gam.framework.status.StatusManager;
import com.fiscaladmin.gam.statementimporter.dedup.DeduplicationChecker;
import com.fiscaladmin.gam.statementimporter.dedup.DeduplicationResult;
import com.fiscaladmin.gam.statementimporter.mapping.MappingConfig;
import com.fiscaladmin.gam.statementimporter.mapping.MappingConfigurations;
import com.fiscaladmin.gam.statementimporter.parser.CsvFormatDetector;
import com.fiscaladmin.gam.statementimporter.parser.Format;
import com.fiscaladmin.gam.statementimporter.parser.StatementParser;
import com.fiscaladmin.gam.statementimporter.persister.RawTransactionPersister;
import org.joget.apps.app.service.AppUtil;
import org.joget.apps.form.dao.FormDataDao;
import org.joget.apps.form.model.FormRow;
import org.joget.apps.form.model.FormRowSet;
import org.joget.apps.form.service.FileUtil;
import org.joget.commons.util.LogUtil;
import org.joget.plugin.base.DefaultApplicationPlugin;
import org.joget.workflow.model.WorkflowAssignment;
import org.joget.workflow.model.service.WorkflowManager;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Statement Importer — Joget Process Tool plugin.
 * <p>
 * Parses uploaded CSV bank/securities statement files, detects the format,
 * performs de-duplication, and batch-inserts raw transaction rows into
 * staging tables ({@code bank_account_trx} or {@code sec_account_trx}).
 * <p>
 * Configured as a Tool in BPMN workflow Activity 1 of the
 * {@code gam_statement_submission} process.
 * <p>
 * Orchestration flow:
 * <ol>
 *   <li>Read workflow variables from properties map</li>
 *   <li>Validate required inputs</li>
 *   <li>Load statement record via FormDataDao</li>
 *   <li>Resolve physical CSV file via FileUtil.getFile()</li>
 *   <li>Idempotency — delete any existing raw rows for this statement</li>
 *   <li>Transition status: NEW → IMPORTING</li>
 *   <li>Detect CSV format</li>
 *   <li>Parse CSV rows</li>
 *   <li>De-duplication check</li>
 *   <li>Batch-insert non-duplicate rows</li>
 *   <li>Update statement metadata</li>
 *   <li>Transition status: IMPORTING → IMPORTED</li>
 *   <li>Set workflow variable statementStatus = "imported"</li>
 * </ol>
 */
public class StatementImporter extends DefaultApplicationPlugin {

    private static final String CLASS_NAME = StatementImporter.class.getName();

    @Override
    public String getName() {
        return "Statement Importer";
    }

    @Override
    public String getDescription() {
        return "Parses uploaded CSV statement files, detects format, performs de-duplication, and batch-inserts raw transaction rows.";
    }

    @Override
    public String getVersion() {
        return "8.1-SNAPSHOT";
    }

    @Override
    public String getLabel() {
        return "Statement Importer";
    }

    @Override
    public String getClassName() {
        return getClass().getName();
    }

    @Override
    public String getPropertyOptions() {
        return AppUtil.readPluginResource(getClass().getName(), "/properties/app/StatementImporter.json", null, true, null);
    }

    @Override
    public Object execute(Map properties) {
        String recordId = null;
        StatusManager statusManager = new StatusManager();
        FormDataDao dao = null;

        try {
            // Step 1: Get record ID - try multiple approaches
            WorkflowAssignment assignment = (WorkflowAssignment) properties.get("workflowAssignment");
            WorkflowManager workflowManager = (WorkflowManager)
                AppUtil.getApplicationContext().getBean("workflowManager");

            // Approach 1: Try workflow variable 'id' (requires variable to be defined in process)
            if (assignment != null) {
                recordId = workflowManager.getProcessVariable(assignment.getProcessId(), "id");
                if (recordId != null && !recordId.isEmpty()) {
                    LogUtil.info(CLASS_NAME, "Got recordId from workflow variable 'id': " + recordId);
                }
            }

            // Approach 2: Try workflow variable 'recordId'
            if ((recordId == null || recordId.isEmpty()) && assignment != null) {
                recordId = workflowManager.getProcessVariable(assignment.getProcessId(), "recordId");
                if (recordId != null && !recordId.isEmpty()) {
                    LogUtil.info(CLASS_NAME, "Got recordId from workflow variable 'recordId': " + recordId);
                }
            }

            // Approach 3: Try to get from plugin properties
            if (recordId == null || recordId.isEmpty()) {
                Object propValue = properties.get("recordId");
                if (propValue != null) {
                    recordId = propValue.toString();
                    LogUtil.info(CLASS_NAME, "Got recordId from plugin properties: " + recordId);
                }
            }

            // Approach 4: Try 'id' from plugin properties
            if (recordId == null || recordId.isEmpty()) {
                Object propValue = properties.get("id");
                if (propValue != null) {
                    recordId = propValue.toString();
                    LogUtil.info(CLASS_NAME, "Got recordId from plugin 'id' property: " + recordId);
                }
            }

            LogUtil.info(CLASS_NAME, "Starting Statement Import for record: " + recordId);

            // Step 2: Validate record ID
            if (recordId == null || recordId.isEmpty()) {
                throw new IllegalArgumentException("recordId is required - could not extract from process instance or properties");
            }

            dao = StatusManager.getFormDataDao();

            // Step 3: Load statement record and read ALL fields from form
            FormRow statementRow = dao.load("bank_statement", "bank_statement", recordId);
            if (statementRow == null) {
                throw new IllegalStateException("Statement record not found: " + recordId);
            }

            // The operator supplies only account_type + the uploaded file. All statement
            // metadata — period (from/to dates) and account number — is derived from the
            // file itself after parsing (see deriveFileMetadata()), so the operator never
            // hand-keys data that is already present in the statement.
            String accountType = statementRow.getProperty("account_type");
            String fileName = statementRow.getProperty("statement_file");

            LogUtil.info(CLASS_NAME, "Loaded form data - accountType: " + accountType
                + ", file: " + fileName);

            // Validate required fields
            if (accountType == null || accountType.isEmpty()) {
                throw new IllegalArgumentException("account_type is required in the form");
            }
            if (fileName == null || fileName.isEmpty()) {
                throw new IllegalStateException("No file attached to statement: " + recordId);
            }

            // Step 4: Resolve physical CSV file
            File csvFile = FileUtil.getFile(fileName, "bank_statement", recordId);
            if (csvFile == null || !csvFile.exists()) {
                throw new IllegalStateException("CSV file not found on disk: " + fileName
                    + " (table=bank_statement, recordId=" + recordId + ")");
            }

            // Step 5: Idempotency — delete previous raw rows
            deleteExistingRawRows(recordId, accountType);

            // Step 6: Status NEW → IMPORTING
            statusManager.transition(dao, EntityType.STATEMENT, recordId,
                Status.IMPORTING, "statement-importer", "Starting CSV import");

            // Step 7: Detect CSV format
            Format format = CsvFormatDetector.detect(csvFile);
            LogUtil.info(CLASS_NAME, "Detected CSV format: " + format
                + " (accountType=" + format.getAccountType() + ")");

            // Validate format matches the declared account type
            if (!format.getAccountType().equals(accountType)) {
                throw new IllegalStateException("Format mismatch: detected "
                    + format.getAccountType() + " but statement has account_type=" + accountType);
            }

            // Step 8: Parse CSV rows
            List<String[]> allRows = StatementParser.parse(csvFile, format);
            LogUtil.info(CLASS_NAME, "Parsed " + allRows.size() + " rows from CSV");

            // Step 8b: Derive statement metadata from the file itself (operator no longer
            // enters these). Bank rows carry the account IBAN at index 0 and a payment date
            // at index 2; securities rows carry the value date at index 0 and no account.
            FileMetadata meta = deriveFileMetadata(allRows, accountType);
            String fromDate = meta.fromDate;
            String toDate = meta.toDate;
            LogUtil.info(CLASS_NAME, "Derived from file - period: " + fromDate + ".." + toDate
                + ", account: " + meta.accountNumber);

            // Step 9: De-duplication check (scoped by the derived statement period)
            DeduplicationResult dedupResult = DeduplicationChecker.check(
                allRows, recordId, fromDate, toDate, accountType);
            List<String[]> newRows = dedupResult.getNonDuplicateRows();
            int duplicateCount = dedupResult.getDuplicateCount();

            LogUtil.info(CLASS_NAME, "De-duplication: " + newRows.size() + " new rows, "
                + duplicateCount + " duplicates out of " + allRows.size() + " total");

            // Step 10: Batch-insert non-duplicate rows
            MappingConfig mappingConfig = MappingConfigurations.getConfig(format);
            int insertedCount = RawTransactionPersister.persist(newRows, recordId, mappingConfig);
            LogUtil.info(CLASS_NAME, "Inserted " + insertedCount + " rows into "
                + mappingConfig.getTargetTable());

            // Step 11: Update statement metadata (row counts + file-derived period/account
            // + the statement bank BIC derived from the detected format, which enrichment
            // uses to resolve the counterparty).
            updateStatementMetadata(dao, recordId, allRows.size(), duplicateCount, meta,
                    format.getStatementBankBic());

            // Step 12: Status IMPORTING → IMPORTED
            statusManager.transition(dao, EntityType.STATEMENT, recordId,
                Status.IMPORTED, "statement-importer",
                "Import completed: " + allRows.size() + " rows, " + duplicateCount + " duplicates");

            // Step 13: Set workflow variable for gateway
            setWorkflowVariable(properties, "statementStatus", "imported");

            LogUtil.info(CLASS_NAME, "Statement Import completed successfully for record: " + recordId);

        } catch (Exception e) {
            LogUtil.error(CLASS_NAME, e, "Statement Import failed for record: " + recordId);

            // 1. Try to set status to ERROR
            if (recordId != null) {
                try {
                    FormDataDao errorDao = StatusManager.getFormDataDao();
                    StatusManager errorStatusMgr = new StatusManager();
                    errorStatusMgr.transition(errorDao, EntityType.STATEMENT, recordId,
                        Status.ERROR, "statement-importer", "Import failed: " + e.getMessage());
                } catch (Exception statusEx) {
                    // StatusManager transition may fail if we're in an unexpected state.
                    // Fall back to direct SQL update.
                    LogUtil.error(CLASS_NAME, statusEx,
                        "Failed to transition status to ERROR via StatusManager");
                    try {
                        setStatusDirectly(recordId, "error", e.getMessage());
                    } catch (Exception directEx) {
                        LogUtil.error(CLASS_NAME, directEx, "Failed to set error status directly");
                    }
                }

                // 2. Populate error_message on the statement
                try {
                    FormDataDao errorDao = StatusManager.getFormDataDao();
                    FormRow errorRow = new FormRow();
                    errorRow.setId(recordId);
                    errorRow.setProperty("error_message", truncateErrorMessage(e.getMessage()));
                    FormRowSet errorRowSet = new FormRowSet();
                    errorRowSet.add(errorRow);
                    errorDao.saveOrUpdate("bank_statement", "bank_statement", errorRowSet);
                } catch (Exception metaEx) {
                    LogUtil.error(CLASS_NAME, metaEx, "Failed to update error_message");
                }
            }

            // 3. Set workflow variable for error routing at the gateway
            setWorkflowVariable(properties, "statementStatus", "error");
        }

        return null;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Private helpers
    // ─────────────────────────────────────────────────────────────────

    /**
     * Deletes any existing raw transaction rows for this statement ID.
     * Provides idempotency for safe re-processing after error recovery.
     */
    private void deleteExistingRawRows(String statementId, String accountType) {
        String rawTable;
        if ("bank".equals(accountType)) {
            rawTable = "app_fd_bank_account_trx";
        } else {
            rawTable = "app_fd_sec_account_trx";
        }

        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");
        try (Connection con = ds.getConnection();
             PreparedStatement ps = con.prepareStatement(
                 "DELETE FROM " + rawTable + " WHERE c_statement_id = ?")) {
            ps.setString(1, statementId);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LogUtil.info(CLASS_NAME, "Idempotency: deleted " + deleted
                    + " existing raw rows from " + rawTable + " for statement " + statementId);
            }
        } catch (SQLException e) {
            LogUtil.warn(CLASS_NAME, "Warning: could not delete existing raw rows: " + e.getMessage());
            // Non-fatal — proceed with import
        }
    }

    /**
     * Updates the statement record with processing results metadata, including the
     * file-derived statement period (from/to dates) and account number. These were
     * previously hand-entered by the operator; they are now read straight from the file.
     */
    private void updateStatementMetadata(FormDataDao dao, String recordId,
                                          int rowCount, int duplicateCount,
                                          FileMetadata meta, String bankBic) {
        FormRow row = new FormRow();
        row.setId(recordId);
        row.setProperty("row_count", String.valueOf(rowCount));
        row.setProperty("duplicate_count", String.valueOf(duplicateCount));
        row.setProperty("processing_timestamp",
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        // The statement bank BIC is derived from the file format (operator no longer picks a
        // bank). Enrichment's CounterpartyDeterminationStep reads this `bank` field to resolve
        // the counterparty; leaving it blank sends every row to manual review.
        if (bankBic != null && !bankBic.isEmpty()) {
            row.setProperty("bank", bankBic);
        }
        // Only write derived values when present, so a sparse file never blanks them out.
        if (meta != null) {
            if (meta.fromDate != null && !meta.fromDate.isEmpty()) {
                row.setProperty("from_date", meta.fromDate);
            }
            if (meta.toDate != null && !meta.toDate.isEmpty()) {
                row.setProperty("to_date", meta.toDate);
            }
            if (meta.accountNumber != null && !meta.accountNumber.isEmpty()) {
                row.setProperty("account_number", meta.accountNumber);
            }
        }

        FormRowSet rowSet = new FormRowSet();
        rowSet.add(row);
        dao.saveOrUpdate("bank_statement", "bank_statement", rowSet);
    }

    /**
     * Lightweight holder for the statement metadata derived from the parsed file.
     */
    static final class FileMetadata {
        final String fromDate;       // earliest date in the file (yyyy-MM-dd), or ""
        final String toDate;         // latest date in the file (yyyy-MM-dd), or ""
        final String accountNumber;  // account IBAN (bank statements only), or ""

        FileMetadata(String fromDate, String toDate, String accountNumber) {
            this.fromDate = fromDate;
            this.toDate = toDate;
            this.accountNumber = accountNumber;
        }
    }

    /**
     * Derives the statement period and account number from the parsed CSV rows so the
     * operator never has to type data that is already in the file.
     * <p>
     * Column positions follow {@code MappingConfigurations}:
     * <ul>
     *   <li><b>bank</b>: index 0 = account number (IBAN), index 2 = payment date</li>
     *   <li><b>securities</b>: index 0 = value date, no account number</li>
     * </ul>
     * Dates are stored in ISO {@code yyyy-MM-dd} form, so a plain string min/max yields
     * the correct chronological range without parsing.
     *
     * @param rows        parsed CSV rows (each a String[] in source-column order)
     * @param accountType "bank" or "secu"
     * @return derived metadata; fields are "" when the file does not carry them
     */
    static FileMetadata deriveFileMetadata(List<String[]> rows, String accountType) {
        boolean isBank = "bank".equals(accountType);
        int dateIdx = isBank ? 2 : 0;       // bank: payment date; secu: value date
        int accountIdx = isBank ? 0 : -1;   // bank: IBAN; secu: none

        String minDate = null;
        String maxDate = null;
        String accountNumber = "";

        if (rows != null) {
            for (String[] r : rows) {
                if (r == null) {
                    continue;
                }
                if (dateIdx < r.length) {
                    String d = trimOrEmpty(r[dateIdx]);
                    if (!d.isEmpty()) {
                        if (minDate == null || d.compareTo(minDate) < 0) {
                            minDate = d;
                        }
                        if (maxDate == null || d.compareTo(maxDate) > 0) {
                            maxDate = d;
                        }
                    }
                }
                if (accountNumber.isEmpty() && accountIdx >= 0 && accountIdx < r.length) {
                    accountNumber = trimOrEmpty(r[accountIdx]);
                }
            }
        }

        return new FileMetadata(
            minDate == null ? "" : minDate,
            maxDate == null ? "" : maxDate,
            accountNumber);
    }

    private static String trimOrEmpty(String s) {
        return s == null ? "" : s.trim();
    }

    /**
     * Direct SQL fallback for setting status when StatusManager cannot transition.
     * Handles edge cases where the current state doesn't allow a transition to ERROR.
     */
    private void setStatusDirectly(String recordId, String status, String errorMessage) {
        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");
        try (Connection con = ds.getConnection();
             PreparedStatement ps = con.prepareStatement(
                 "UPDATE app_fd_bank_statement SET c_status = ?, c_error_message = ? WHERE id = ?")) {
            ps.setString(1, status);
            ps.setString(2, truncateErrorMessage(errorMessage));
            ps.setString(3, recordId);
            ps.executeUpdate();
        } catch (SQLException ex) {
            LogUtil.error(CLASS_NAME, ex, "Direct status update failed");
        }
    }

    /**
     * Truncates an error message to avoid MySQL column overflow.
     * Package-private for testing.
     */
    String truncateErrorMessage(String message) {
        if (message == null) return "Unknown error";
        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }

    /**
     * Sets a workflow variable on the current activity.
     *
     * @param properties   the plugin properties map (contains workflowAssignment)
     * @param variableName the workflow variable name
     * @param value        the value to set
     */
    private void setWorkflowVariable(Map properties, String variableName, String value) {
        WorkflowAssignment assignment = (WorkflowAssignment) properties.get("workflowAssignment");
        if (assignment != null) {
            WorkflowManager workflowManager = (WorkflowManager)
                AppUtil.getApplicationContext().getBean("workflowManager");
            workflowManager.activityVariable(
                assignment.getActivityId(), variableName, value);
        }
    }
}
