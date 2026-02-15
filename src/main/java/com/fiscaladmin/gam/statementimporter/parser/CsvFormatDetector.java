package com.fiscaladmin.gam.statementimporter.parser;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Detects the CSV format by inspecting the first line (header) of a file.
 * <p>
 * Detection logic (applied after BOM stripping, quote removal, and lowercasing):
 * <ul>
 *   <li>Contains "väärtuspäev" AND "tehingupäev" → {@link Format#SECURITIES}</li>
 *   <li>Contains "dokumendi number" → {@link Format#LHV_BANK}</li>
 *   <li>Contains "dok nr" → {@link Format#SWEDBANK}</li>
 *   <li>Otherwise → throws {@link UnrecognisedFormatException}</li>
 * </ul>
 * Strips UTF-8 BOM (bytes EF BB BF) if present before inspection.
 */
public class CsvFormatDetector {

    private CsvFormatDetector() {
        // utility class
    }

    /**
     * Detects the CSV format by inspecting the first line (header) of the file.
     *
     * @param file the CSV file to inspect
     * @return the detected Format
     * @throws UnrecognisedFormatException if format cannot be determined
     * @throws IOException                 if file cannot be read
     */
    public static Format detect(File file) throws IOException, UnrecognisedFormatException {
        String headerLine;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            headerLine = reader.readLine();
        }

        if (headerLine == null) {
            throw new UnrecognisedFormatException("(empty file)");
        }

        // Strip UTF-8 BOM if present
        if (headerLine.startsWith("\uFEFF")) {
            headerLine = headerLine.substring(1);
        }

        // Keep the raw header (after BOM strip) for diagnostics
        String rawHeader = headerLine;

        // Normalise: remove quotes, lowercase
        String normalised = headerLine.replaceAll("[\"']", "").toLowerCase();

        // Match in priority order: Securities → LHV → Swedbank
        if (normalised.contains("väärtuspäev") && normalised.contains("tehingupäev")) {
            return Format.SECURITIES;
        }
        if (normalised.contains("dokumendi number")) {
            return Format.LHV_BANK;
        }
        if (normalised.contains("dok nr")) {
            return Format.SWEDBANK;
        }

        throw new UnrecognisedFormatException(rawHeader);
    }
}
