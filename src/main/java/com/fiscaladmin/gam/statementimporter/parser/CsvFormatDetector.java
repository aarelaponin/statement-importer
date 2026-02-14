package com.fiscaladmin.gam.statementimporter.parser;

import java.io.File;
import java.io.IOException;

/**
 * Detects the CSV format by inspecting the first line (header) of a file.
 * <p>
 * Detection logic:
 * <ul>
 *   <li>Contains "vääruspäev" AND "tehingupäev" → {@link Format#SECURITIES}</li>
 *   <li>Contains "dokumendi number" → {@link Format#LHV_BANK}</li>
 *   <li>Contains "dok nr" → {@link Format#SWEDBANK}</li>
 *   <li>Otherwise → throws {@link UnrecognisedFormatException}</li>
 * </ul>
 * Strips UTF-8 BOM (bytes EF BB BF) if present before inspection.
 */
public class CsvFormatDetector {

    /**
     * Detects the CSV format by inspecting the first line (header) of the file.
     *
     * @param file the CSV file to inspect
     * @return the detected Format
     * @throws UnrecognisedFormatException if format cannot be determined
     * @throws IOException if file cannot be read
     */
    public static Format detect(File file) throws IOException, UnrecognisedFormatException {
        // TODO: T2.2 — Implement format detection
        throw new UnsupportedOperationException("Not yet implemented — see T2.2");
    }
}
