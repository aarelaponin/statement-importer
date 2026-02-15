package com.fiscaladmin.gam.statementimporter.parser;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a CSV file using the detected format and returns all data rows.
 * <p>
 * Uses Apache Commons CSV internally. Key behaviours:
 * <ul>
 *   <li>Uses the format's separator character</li>
 *   <li>Skips the header row</li>
 *   <li>Handles UTF-8 BOM ({@code EF BB BF}) transparently</li>
 *   <li>For {@link Format#LHV_BANK}: strips the empty 6th field (index 6, "Tühi") from each row,
 *       so the returned arrays have 18 elements aligned with
 *       {@code MappingConfigurations.LHV_BANK_CONFIG}</li>
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
        // 1. Build CSVFormat using the 1.10.0 builder pattern
        CSVFormat csvFormat = CSVFormat.RFC4180.builder()
                .setDelimiter(format.getSeparator())
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreEmptyLines(true)
                .setTrim(true)
                .setQuote('"')
                .build();

        // 2. Open reader with UTF-8 encoding and BOM handling
        Reader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
        reader.mark(1);
        int firstChar = reader.read();
        if (firstChar != '\uFEFF' && firstChar != -1) {
            reader.reset(); // not a BOM — put the character back
        }

        // 3. Parse all rows
        List<String[]> rows = new ArrayList<>();
        try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
            for (CSVRecord record : csvParser) {
                String[] fields = new String[record.size()];
                for (int i = 0; i < record.size(); i++) {
                    String value = record.get(i);
                    fields[i] = (value != null) ? value : "";
                }
                rows.add(fields);
            }
        }

        // 4. Strip the empty field at index 6 for LHV Bank
        if (format == Format.LHV_BANK) {
            for (int i = 0; i < rows.size(); i++) {
                rows.set(i, removeElement(rows.get(i), 6));
            }
        }

        // 5. Return parsed rows
        return rows;
    }

    /**
     * Removes the element at the given index from a String array,
     * returning a new array with length {@code array.length - 1}.
     */
    private static String[] removeElement(String[] array, int indexToRemove) {
        String[] result = new String[array.length - 1];
        for (int i = 0, j = 0; i < array.length; i++) {
            if (i != indexToRemove) {
                result[j++] = array[i];
            }
        }
        return result;
    }
}
