package com.fiscaladmin.gam.statementimporter.lib;

import com.fiscaladmin.gam.framework.status.EntityType;
import com.fiscaladmin.gam.framework.status.Status;
import com.fiscaladmin.gam.framework.status.StatusManager;
import com.fiscaladmin.gam.statementimporter.consolidation.BankConsolidationQuery;
import com.fiscaladmin.gam.statementimporter.consolidation.ConsolidatedRowPersister;
import com.fiscaladmin.gam.statementimporter.consolidation.SecuConsolidationQuery;
import org.joget.apps.app.service.AppUtil;
import org.joget.apps.form.dao.FormDataDao;
import org.joget.apps.form.model.FormRow;
import org.joget.apps.form.model.FormRowSet;
import org.joget.commons.util.LogUtil;
import org.joget.plugin.base.DefaultApplicationPlugin;
import org.joget.workflow.model.WorkflowAssignment;
import org.joget.workflow.model.service.WorkflowManager;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Statement Consolidator — Joget Process Tool plugin.
 * <p>
 * Consolidates raw imported transaction rows into summary rows via GROUP BY
 * aggregation. This is Activity 2 in the {@code gam_statement_submission}
 * BPMN process (runs after StatementImporter).
 * <p>
 * Status flow: IMPORTED → CONSOLIDATING → CONSOLIDATED
 * <p>
 * Orchestration flow:
 * <ol>
 *   <li>Get record ID from workflow variable {@code id}</li>
 *   <li>Load statement record via FormDataDao</li>
 *   <li>Read account_type from form to determine bank vs securities</li>
 *   <li>Validate current status is IMPORTED</li>
 *   <li>Transition status: IMPORTED → CONSOLIDATING</li>
 *   <li>Delete existing consolidated rows (idempotency)</li>
 *   <li>Execute GROUP BY query on raw transactions</li>
 *   <li>Generate statement references (STMT{YYYY}.{SEQ})</li>
 *   <li>Batch-insert consolidated rows</li>
 *   <li>Update statement metadata (total_count)</li>
 *   <li>Transition status: CONSOLIDATING → CONSOLIDATED</li>
 *   <li>Set workflow variable statementStatus = "consolidated"</li>
 * </ol>
 */
public class StatementConsolidator extends DefaultApplicationPlugin {

    private static final String CLASS_NAME = StatementConsolidator.class.getName();

    @Override
    public String getName() {
        return "Statement Consolidator";
    }

    @Override
    public String getDescription() {
        return "Consolidates raw imported transaction rows into summary rows via GROUP BY aggregation.";
    }

    @Override
    public String getVersion() {
        return "8.1-SNAPSHOT";
    }

    @Override
    public String getLabel() {
        return "Statement Consolidator";
    }

    @Override
    public String getClassName() {
        return getClass().getName();
    }

    @Override
    public String getPropertyOptions() {
        return AppUtil.readPluginResource(getClass().getName(), "/properties/app/StatementConsolidator.json", null, true, null);
    }

    @Override
    public Object execute(Map properties) {
        String recordId = null;
        StatusManager statusManager = new StatusManager();

        try {
            // Step 1: Get record ID - try multiple approaches (same as StatementImporter)
            WorkflowAssignment assignment = (WorkflowAssignment) properties.get("workflowAssignment");
            WorkflowManager workflowManager = (WorkflowManager)
                AppUtil.getApplicationContext().getBean("workflowManager");

            // Approach 1: Try workflow variable 'id'
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

            LogUtil.info(CLASS_NAME, "Starting Statement Consolidation for record: " + recordId);

            // Step 2: Validate record ID
            if (recordId == null || recordId.isEmpty()) {
                throw new IllegalArgumentException("recordId is required - could not extract from process instance or properties");
            }

            FormDataDao dao = StatusManager.getFormDataDao();

            // Step 3: Load statement record
            FormRow statementRow = dao.load("bank_statement", "bank_statement", recordId);
            if (statementRow == null) {
                throw new IllegalStateException("Statement record not found: " + recordId);
            }

            // Read fields from form
            String accountType = statementRow.getProperty("account_type");
            String fromDate = statementRow.getProperty("from_date");

            LogUtil.info(CLASS_NAME, "Loaded form data - accountType: " + accountType + ", fromDate: " + fromDate);

            // Validate required fields
            if (accountType == null || accountType.isEmpty()) {
                throw new IllegalArgumentException("account_type is required in the form");
            }

            // Step 4: Transition status IMPORTED → CONSOLIDATING
            statusManager.transition(dao, EntityType.STATEMENT, recordId,
                Status.CONSOLIDATING, "statement-consolidator", "Starting consolidation");

            // Step 5: Delete existing consolidated rows (idempotency)
            int deletedRows = deleteExistingConsolidatedRows(recordId, accountType);
            if (deletedRows > 0) {
                LogUtil.info(CLASS_NAME, "Idempotency: deleted " + deletedRows + " existing consolidated rows");
            }

            // Step 6: Execute consolidation
            String statementRefPrefix = generateStatementRefPrefix(fromDate);
            int consolidatedCount = executeConsolidation(recordId, accountType, statementRefPrefix);

            LogUtil.info(CLASS_NAME, "Consolidated " + consolidatedCount + " summary rows");

            // Step 7: Update statement metadata
            updateStatementMetadata(dao, recordId, consolidatedCount);

            // Step 8: Transition status CONSOLIDATING → CONSOLIDATED
            statusManager.transition(dao, EntityType.STATEMENT, recordId,
                Status.CONSOLIDATED, "statement-consolidator",
                "Consolidation completed: " + consolidatedCount + " summary rows");

            // Step 9: Set workflow variable for gateway
            setWorkflowVariable(properties, "statementStatus", "consolidated");

            LogUtil.info(CLASS_NAME, "Statement Consolidation completed successfully for record: " + recordId);

        } catch (Exception e) {
            LogUtil.error(CLASS_NAME, e, "Statement Consolidation failed for record: " + recordId);

            // Try to set status to ERROR
            if (recordId != null) {
                try {
                    FormDataDao errorDao = StatusManager.getFormDataDao();
                    StatusManager errorStatusMgr = new StatusManager();
                    errorStatusMgr.transition(errorDao, EntityType.STATEMENT, recordId,
                        Status.ERROR, "statement-consolidator", "Consolidation failed: " + e.getMessage());
                } catch (Exception statusEx) {
                    LogUtil.error(CLASS_NAME, statusEx,
                        "Failed to transition status to ERROR via StatusManager");
                    try {
                        setStatusDirectly(recordId, "error", e.getMessage());
                    } catch (Exception directEx) {
                        LogUtil.error(CLASS_NAME, directEx, "Failed to set error status directly");
                    }
                }

                // Populate error_message on the statement
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

            // Set workflow variable for error routing
            setWorkflowVariable(properties, "statementStatus", "error");
        }

        return null;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Private helpers
    // ─────────────────────────────────────────────────────────────────

    /**
     * Deletes any existing consolidated rows for this statement ID.
     * Provides idempotency for safe re-processing.
     */
    private int deleteExistingConsolidatedRows(String statementId, String accountType) {
        String deleteSql;
        if ("bank".equals(accountType)) {
            deleteSql = BankConsolidationQuery.DELETE_SQL;
        } else {
            deleteSql = SecuConsolidationQuery.DELETE_SQL;
        }

        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");
        try (Connection con = ds.getConnection();
             PreparedStatement ps = con.prepareStatement(deleteSql)) {
            ps.setString(1, statementId);
            return ps.executeUpdate();
        } catch (SQLException e) {
            LogUtil.warn(CLASS_NAME, "Warning: could not delete existing consolidated rows: " + e.getMessage());
            return 0;
        }
    }

    /**
     * Executes the consolidation: GROUP BY query followed by batch insert.
     */
    private int executeConsolidation(String statementId, String accountType, String statementRefPrefix) {
        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");

        String groupBySql;
        String insertSql;
        List<String> insertColumns;
        String targetTable;

        if ("bank".equals(accountType)) {
            groupBySql = BankConsolidationQuery.GROUP_BY_SQL;
            insertSql = BankConsolidationQuery.INSERT_SQL;
            insertColumns = BankConsolidationQuery.INSERT_COLUMNS;
            targetTable = BankConsolidationQuery.TARGET_TABLE;
        } else {
            groupBySql = SecuConsolidationQuery.GROUP_BY_SQL;
            insertSql = SecuConsolidationQuery.INSERT_SQL;
            insertColumns = SecuConsolidationQuery.INSERT_COLUMNS;
            targetTable = SecuConsolidationQuery.TARGET_TABLE;
        }

        try (Connection con = ds.getConnection()) {
            // Execute GROUP BY query
            List<Map<String, String>> consolidatedRows = new ArrayList<>();

            try (PreparedStatement ps = con.prepareStatement(groupBySql)) {
                ps.setString(1, statementId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, String> row;
                        if ("bank".equals(accountType)) {
                            row = BankConsolidationQuery.readRow(rs);
                        } else {
                            row = SecuConsolidationQuery.readRow(rs);
                        }
                        consolidatedRows.add(row);
                    }
                }
            }

            LogUtil.info(CLASS_NAME, "GROUP BY returned " + consolidatedRows.size() + " aggregated rows");

            if (consolidatedRows.isEmpty()) {
                return 0;
            }

            // Batch insert consolidated rows
            return ConsolidatedRowPersister.persist(
                consolidatedRows,
                statementId,
                statementRefPrefix,
                insertSql,
                insertColumns,
                targetTable,
                con
            );

        } catch (SQLException e) {
            throw new RuntimeException("Consolidation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Generates the statement reference prefix from the from_date.
     * Example: "2024-06-01" → "STMT2024"
     */
    private String generateStatementRefPrefix(String fromDate) {
        if (fromDate == null || fromDate.length() < 4) {
            // Fall back to current year
            return "STMT" + new SimpleDateFormat("yyyy").format(new Date());
        }
        String year = fromDate.substring(0, 4);
        return "STMT" + year;
    }

    /**
     * Updates the statement record with consolidation results metadata.
     */
    private void updateStatementMetadata(FormDataDao dao, String recordId, int totalCount) {
        FormRow row = new FormRow();
        row.setId(recordId);
        row.setProperty("total_count", String.valueOf(totalCount));
        row.setProperty("consolidation_timestamp",
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        FormRowSet rowSet = new FormRowSet();
        rowSet.add(row);
        dao.saveOrUpdate("bank_statement", "bank_statement", rowSet);
    }

    /**
     * Direct SQL fallback for setting status when StatusManager cannot transition.
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
     */
    String truncateErrorMessage(String message) {
        if (message == null) return "Unknown error";
        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }

    /**
     * Sets a workflow variable on the current activity.
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
