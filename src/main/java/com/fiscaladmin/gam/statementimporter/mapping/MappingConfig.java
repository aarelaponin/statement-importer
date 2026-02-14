package com.fiscaladmin.gam.statementimporter.mapping;

import java.util.List;

/**
 * Configuration container for one CSV format's complete column mapping.
 * <p>
 * Holds the MySQL target table name (with {@code app_fd_} prefix) and the
 * ordered list of field mappings from CSV column indices to database columns.
 */
public class MappingConfig {

    private final String targetTable;
    private final List<FieldMapping> fieldMappings;

    /**
     * @param targetTable   MySQL table name WITH app_fd_ prefix, e.g., "app_fd_bank_account_trx"
     * @param fieldMappings ordered list of CSV index → DB column mappings
     */
    public MappingConfig(String targetTable, List<FieldMapping> fieldMappings) {
        this.targetTable = targetTable;
        this.fieldMappings = fieldMappings;
    }

    /**
     * Returns the MySQL table name with {@code app_fd_} prefix.
     */
    public String getTargetTable() {
        return targetTable;
    }

    /**
     * Returns the ordered list of field mappings.
     */
    public List<FieldMapping> getFieldMappings() {
        return fieldMappings;
    }
}
