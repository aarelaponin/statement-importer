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

    // The statementBankBic is the SWIFT/BIC of the institution that issued the file.
    // It is intrinsic to the format (an LHV CSV is, by definition, an LHV statement) and is
    // written to the statement's `bank` field so downstream enrichment can resolve the
    // counterparty — CounterpartyDeterminationStep matches this BIC against counterparty_master.
    // GAM's securities custodian is also LHV, so the securities format carries LHV's BIC.
    LHV_BANK("bank", ',', "bank_account_trx", "LHVBEE22"),
    SWEDBANK("bank", ';', "bank_account_trx", "HABAEE2X"),
    SECURITIES("secu", ',', "sec_account_trx", "LHVBEE22");

    private final String accountType;
    private final char separator;
    private final String targetTable;
    private final String statementBankBic;

    Format(String accountType, char separator, String targetTable, String statementBankBic) {
        this.accountType = accountType;
        this.separator = separator;
        this.targetTable = targetTable;
        this.statementBankBic = statementBankBic;
    }

    /**
     * Returns the SWIFT/BIC of the bank that issued statements in this format.
     * Written to the statement's {@code bank} field so enrichment can resolve the
     * counterparty without the operator selecting a bank.
     */
    public String getStatementBankBic() {
        return statementBankBic;
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
