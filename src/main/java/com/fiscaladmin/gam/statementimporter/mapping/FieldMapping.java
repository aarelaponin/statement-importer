package com.fiscaladmin.gam.statementimporter.mapping;

/**
 * An immutable mapping from a CSV column index to a database column name.
 * <p>
 * The {@code dbColumnName} uses the <b>SQL-level</b> column name with the {@code c_} prefix
 * (e.g., {@code c_payment_date}), because this mapping is used by
 * {@link com.fiscaladmin.gam.statementimporter.persister.RawTransactionPersister}
 * which does JDBC batch inserts directly against MySQL, not through {@code FormDataDao}.
 */
public class FieldMapping {

    private final int csvIndex;
    private final String dbColumnName;

    public FieldMapping(int csvIndex, String dbColumnName) {
        this.csvIndex = csvIndex;
        this.dbColumnName = dbColumnName;
    }

    /**
     * Returns the zero-based index of this field in the CSV row.
     */
    public int getCsvIndex() {
        return csvIndex;
    }

    /**
     * Returns the SQL column name with {@code c_} prefix (e.g., {@code c_payment_date}).
     */
    public String getDbColumnName() {
        return dbColumnName;
    }
}
