package com.fiscaladmin.gam.statementimporter.parser;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Parses a CSV file using the detected format and returns all data rows.
 * <p>
 * Uses Apache Commons CSV internally. Key behaviours:
 * <ul>
 *   <li>Uses the format's separator character</li>
 *   <li>Skips the header row</li>
 *   <li>For {@link Format#LHV_BANK}: strips the empty 6th field (index 6) from each row</li>
 *   <li>Returns {@code List<String[]>} — each array is one row of raw field values</li>
 * </ul>
 */
public class StatementParser {

    /**
     * Parses a CSV file using the detected format and returns all data rows.
     *
     * @param file   the CSV file to parse
     * @param format the detected CSV format
     * @return list of parsed rows (each row is a String array of field values)
     * @throws IOException if file cannot be read
     */
    public static List<String[]> parse(File file, Format format) throws IOException {
        // TODO: T2.4 — Implement CSV parsing
        throw new UnsupportedOperationException("Not yet implemented — see T2.4");
    }
}
