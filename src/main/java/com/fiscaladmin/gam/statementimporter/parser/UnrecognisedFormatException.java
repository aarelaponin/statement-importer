package com.fiscaladmin.gam.statementimporter.parser;

/**
 * Thrown when the CSV header does not match any known statement format.
 * Carries the actual header line for diagnostic purposes.
 */
public class UnrecognisedFormatException extends Exception {

    private final String headerLine;

    public UnrecognisedFormatException(String headerLine) {
        super("Unrecognised CSV format. Header: " + headerLine);
        this.headerLine = headerLine;
    }

    /**
     * Returns the header line that could not be matched to any known format.
     */
    public String getHeaderLine() {
        return headerLine;
    }
}
