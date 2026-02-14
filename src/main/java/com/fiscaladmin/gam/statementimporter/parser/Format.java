package com.fiscaladmin.gam.statementimporter.parser;

/**
 * Supported CSV statement formats.
 * <p>
 * Each format defines:
 * <ul>
 *   <li>{@code accountType} — "bank" or "secu", determines which processing branch</li>
 *   <li>{@code separator} — the CSV delimiter character</li>
 *   <li>{@code targetTable} — the bare Joget table name for raw row storage</li>
 * </ul>
 */
public enum Format {

    LHV_BANK("bank", ',', "bank_account_trx"),
    SWEDBANK("bank", ';', "bank_account_trx"),
    SECURITIES("secu", ',', "sec_account_trx");

    private final String accountType;
    private final char separator;
    private final String targetTable;

    Format(String accountType, char separator, String targetTable) {
        this.accountType = accountType;
        this.separator = separator;
        this.targetTable = targetTable;
    }

    /**
     * Returns the account type: "bank" or "secu".
     */
    public String getAccountType() {
        return accountType;
    }

    /**
     * Returns the CSV field separator character.
     */
    public char getSeparator() {
        return separator;
    }

    /**
     * Returns the bare Joget table name (without app_fd_ prefix) for raw row storage.
     */
    public String getTargetTable() {
        return targetTable;
    }
}
