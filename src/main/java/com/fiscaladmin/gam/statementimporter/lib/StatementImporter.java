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
import org.joget.commons.util.LogUtil;
import org.joget.plugin.base.DefaultApplicationPlugin;
import org.joget.workflow.model.WorkflowAssignment;
import org.joget.workflow.model.service.WorkflowManager;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Statement Importer — Joget Process Tool plugin.
 * <p>
 * Parses uploaded CSV bank/securities statement files, detects the format,
 * performs de-duplication, and batch-inserts raw transaction rows into
 * staging tables ({@code bank_account_trx} or {@code sec_account_trx}).
 * <p>
 * Configured as a Tool in BPMN workflow Activity 1.
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
        return AppUtil.readPluginResource(getClass().getName(), "", null, true, null);
    }

    @Override
    public Object execute(Map properties) {
        String recordId = null;
        try {
            // Step 1: Read workflow variables
            recordId = (String) properties.get("recordId");
            String accountType = (String) properties.get("account_type");
            String bank = (String) properties.get("bank");
            String fromDate = (String) properties.get("from_date");
            String toDate = (String) properties.get("to_date");

            LogUtil.info(CLASS_NAME, "Starting Statement Import for record: " + recordId
                + ", accountType: " + accountType + ", bank: " + bank);

            // Validate required inputs
            if (recordId == null || recordId.isEmpty()) {
                throw new IllegalArgumentException("recordId is required");
            }

            FormDataDao dao = StatusManager.getFormDataDao();

            // Step 2: Load statement record
            // TODO: T2.7 — Load statement from bank_statement via FormDataDao

            // Step 3: Resolve physical CSV file
            // TODO: T2.7 — Use FileUtil.getFile(fileName, "bank_statement", recordId)

            // Step 4: Transition status NEW → IMPORTING
            // TODO: T2.7 — statusManager.transition(dao, EntityType.STATEMENT, recordId, Status.IMPORTING, ...)

            // Step 5: Detect CSV format
            // TODO: T2.2 — CsvFormatDetector.detect(file)

            // Step 6: Parse CSV rows
            // TODO: T2.4 — StatementParser.parse(file, format)

            // Step 7: De-duplication check
            // TODO: T2.5 — DeduplicationChecker.check(rows, statementId, fromDate, toDate, accountType)

            // Step 8: Batch-insert non-duplicate rows
            // TODO: T2.6 — RawTransactionPersister.persist(rows, statementId, accountType, mappingConfig)

            // Step 9: Update statement metadata (row_count, duplicate_count, processing_timestamp)
            // TODO: T2.7

            // Step 10: Transition status IMPORTING → IMPORTED
            // TODO: T2.7 — statusManager.transition(dao, EntityType.STATEMENT, recordId, Status.IMPORTED, ...)

            // Step 11: Set workflow variable for gateway
            // TODO: T2.7 — setWorkflowVariable(properties, "statementStatus", "imported")

            LogUtil.info(CLASS_NAME, "Statement Import completed successfully for record: " + recordId);

        } catch (Exception e) {
            LogUtil.error(CLASS_NAME, e, "Statement Import failed for record: " + recordId);
            // TODO: T2.7 — Set status=error, populate error_message, set statementStatus="error"
        }
        return null;
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
