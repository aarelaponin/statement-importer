package com.fiscaladmin.gam.statementimporter.persister;

import com.fiscaladmin.gam.statementimporter.mapping.MappingConfig;

import java.util.List;

/**
 * Batch-inserts parsed CSV rows into the target raw transaction table
 * using JDBC batch insert for performance.
 * <p>
 * Key behaviours:
 * <ul>
 *   <li>Uses JDBC (not FormDataDao) for bulk insert performance</li>
 *   <li>Generates Joget-compatible UUIDs for the {@code id} column</li>
 *   <li>Sets {@code c_statement_id} FK on every row</li>
 *   <li>Sets {@code c_transaction_id} as sequential row number</li>
 *   <li>Sets {@code dateCreated} and {@code createdBy} for Joget audit fields</li>
 *   <li>Target table from MappingConfig (e.g., {@code app_fd_bank_account_trx})</li>
 * </ul>
 */
public class RawTransactionPersister {

    /**
     * Batch-inserts parsed CSV rows into the target raw transaction table.
     *
     * @param rows        parsed CSV rows (each row is String[] of field values)
     * @param statementId the parent statement record ID
     * @param config      the mapping configuration for the CSV format
     * @return number of rows inserted
     */
    public static int persist(List<String[]> rows, String statementId, MappingConfig config) {
        // TODO: T2.6 — Implement JDBC batch insert
        throw new UnsupportedOperationException("Not yet implemented — see T2.6");
    }
}
