package com.fiscaladmin.gam.statementimporter.dedup;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link DeduplicationChecker}.
 * <p>
 * All tests use the package-private {@code check(rows, accountType, existingKeys)}
 * overload, which accepts a pre-built set of existing keys — no database required.
 */
public class DeduplicationCheckerTest {

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    /**
     * Creates an 18-element array matching the LHV_BANK_CONFIG layout
     * (after field-6 stripping).
     */
    private String[] bankRow(String accountNumber, String docNr, String paymentDate,
                             String paymentAmount, String currency, String providerReference) {
        String[] row = new String[18];
        Arrays.fill(row, "");
        row[0] = accountNumber;       // c_account_number
        row[1] = docNr;               // c_document_nr
        row[2] = paymentDate;         // c_payment_date
        row[7] = paymentAmount;       // c_payment_amount
        row[12] = currency;           // c_currency
        row[17] = providerReference;  // c_provider_reference
        return row;
    }

    /**
     * Creates a 13-element array matching the SECU_CONFIG layout.
     */
    private String[] secuRow(String valueDate, String trxDate, String type,
                             String ticker, String amount, String currency, String reference) {
        String[] row = new String[13];
        Arrays.fill(row, "");
        row[0] = valueDate;       // c_value_date
        row[1] = trxDate;         // c_transaction_date
        row[2] = type;            // c_type
        row[3] = ticker;          // c_ticker
        row[7] = currency;        // c_currency
        row[8] = amount;          // c_amount
        row[11] = reference;      // c_reference
        return row;
    }

    // -------------------------------------------------------------------------
    // Bank tests — primary key (provider_reference)
    // -------------------------------------------------------------------------

    @Test
    public void emptyDatabaseReturnsAllRowsAsNew() {
        List<String[]> rows = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF002"),
                bankRow("EE1234", "003", "2024-06-03", "300.00", "EUR", "REF003")
        );

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", Collections.emptySet());

        assertEquals(3, result.getNonDuplicateRows().size());
        assertEquals(0, result.getDuplicateCount());
        assertEquals(3, result.getTotalCount());
    }

    @Test
    public void allDuplicatesReturnsEmptyList() {
        List<String[]> rows = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF002"),
                bankRow("EE1234", "003", "2024-06-03", "300.00", "EUR", "REF003")
        );

        Set<String> existingKeys = new HashSet<>(Arrays.asList("REF001", "REF002", "REF003"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(0, result.getNonDuplicateRows().size());
        assertEquals(3, result.getDuplicateCount());
        assertEquals(3, result.getTotalCount());
    }

    @Test
    public void partialOverlapReturnCorrectSplit() {
        List<String[]> rows = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF002"),
                bankRow("EE1234", "003", "2024-06-03", "300.00", "EUR", "REF003"),
                bankRow("EE1234", "004", "2024-06-04", "400.00", "EUR", "REF004"),
                bankRow("EE1234", "005", "2024-06-05", "500.00", "EUR", "REF005")
        );

        // REF002 and REF004 already exist
        Set<String> existingKeys = new HashSet<>(Arrays.asList("REF002", "REF004"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(3, result.getNonDuplicateRows().size());
        assertEquals(2, result.getDuplicateCount());
        assertEquals(5, result.getTotalCount());

        // Verify the correct rows survived
        List<String[]> survivors = result.getNonDuplicateRows();
        assertEquals("REF001", survivors.get(0)[17]);
        assertEquals("REF003", survivors.get(1)[17]);
        assertEquals("REF005", survivors.get(2)[17]);
    }

    @Test
    public void bankUsesProviderReferenceAsPrimaryKey() {
        // Two rows with SAME composite key fields but DIFFERENT provider_references
        String[] row1 = bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF_A");
        String[] row2 = bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF_B");

        List<String[]> rows = Arrays.asList(row1, row2);

        // Only REF_A exists in DB
        Set<String> existingKeys = new HashSet<>(Collections.singletonList("REF_A"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(1, result.getDuplicateCount());
        assertEquals(1, result.getNonDuplicateRows().size());
        // The surviving row should be the one with REF_B
        assertEquals("REF_B", result.getNonDuplicateRows().get(0)[17]);
    }

    @Test
    public void bankFallsBackToCompositeKeyWhenProviderRefEmpty() {
        // Two rows with empty provider_reference — different composite keys
        String[] row1 = bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "");
        String[] row2 = bankRow("EE5678", "002", "2024-06-02", "200.00", "USD", "");

        List<String[]> rows = Arrays.asList(row1, row2);

        // Composite key for row1: "EE1234|001|2024-06-01|100.00|EUR|"
        Set<String> existingKeys = new HashSet<>(Collections.singletonList(
                "EE1234|001|2024-06-01|100.00|EUR|"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(1, result.getDuplicateCount());
        assertEquals(1, result.getNonDuplicateRows().size());
        // The surviving row should be row2
        assertEquals("EE5678", result.getNonDuplicateRows().get(0)[0]);
    }

    // -------------------------------------------------------------------------
    // Securities tests — primary key (reference)
    // -------------------------------------------------------------------------

    @Test
    public void secuUsesReferenceAsPrimaryKey() {
        // Two secu rows with same composite fields but different references
        String[] row1 = secuRow("2024-06-01", "2024-06-01", "BUY", "AAPL", "1000.00", "USD", "SREF_A");
        String[] row2 = secuRow("2024-06-01", "2024-06-01", "BUY", "AAPL", "1000.00", "USD", "SREF_B");

        List<String[]> rows = Arrays.asList(row1, row2);

        Set<String> existingKeys = new HashSet<>(Collections.singletonList("SREF_A"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "secu", existingKeys);

        assertEquals(1, result.getDuplicateCount());
        assertEquals(1, result.getNonDuplicateRows().size());
        assertEquals("SREF_B", result.getNonDuplicateRows().get(0)[11]);
    }

    @Test
    public void secuFallsBackToCompositeKeyWhenReferenceEmpty() {
        // Two secu rows with empty reference — different composite keys
        String[] row1 = secuRow("2024-06-01", "2024-06-01", "BUY", "AAPL", "1000.00", "USD", "");
        String[] row2 = secuRow("2024-06-02", "2024-06-02", "SELL", "MSFT", "500.00", "EUR", "");

        List<String[]> rows = Arrays.asList(row1, row2);

        // Composite key for row1: "2024-06-01|2024-06-01|BUY|AAPL|1000.00|USD|"
        Set<String> existingKeys = new HashSet<>(Collections.singletonList(
                "2024-06-01|2024-06-01|BUY|AAPL|1000.00|USD|"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "secu", existingKeys);

        assertEquals(1, result.getDuplicateCount());
        assertEquals(1, result.getNonDuplicateRows().size());
        assertEquals("MSFT", result.getNonDuplicateRows().get(0)[3]);
    }

    // -------------------------------------------------------------------------
    // Edge cases
    // -------------------------------------------------------------------------

    @Test
    public void emptyRowsListReturnsEmptyResult() {
        List<String[]> rows = Collections.emptyList();

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", Collections.emptySet());

        assertTrue(result.getNonDuplicateRows().isEmpty());
        assertEquals(0, result.getDuplicateCount());
        assertEquals(0, result.getTotalCount());
    }

    @Test
    public void preservesRowOrder() {
        // 5 rows; positions 1 and 3 are duplicates
        List<String[]> rows = Arrays.asList(
                bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF_0"),
                bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "REF_1"),  // dup
                bankRow("EE1234", "003", "2024-06-03", "300.00", "EUR", "REF_2"),
                bankRow("EE1234", "004", "2024-06-04", "400.00", "EUR", "REF_3"),  // dup
                bankRow("EE1234", "005", "2024-06-05", "500.00", "EUR", "REF_4")
        );

        Set<String> existingKeys = new HashSet<>(Arrays.asList("REF_1", "REF_3"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        List<String[]> survivors = result.getNonDuplicateRows();
        assertEquals(3, survivors.size());
        assertEquals("REF_0", survivors.get(0)[17]);
        assertEquals("REF_2", survivors.get(1)[17]);
        assertEquals("REF_4", survivors.get(2)[17]);
    }

    @Test
    public void whitespaceInKeyIsTrimmed() {
        String[] row = bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", " ABC123 ");

        List<String[]> rows = Collections.singletonList(row);

        // Key in DB was stored trimmed
        Set<String> existingKeys = new HashSet<>(Collections.singletonList("ABC123"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(1, result.getDuplicateCount());
        assertEquals(0, result.getNonDuplicateRows().size());
    }

    @Test
    public void nullProviderReferenceFallsBackToComposite() {
        // Row with null provider_reference (not just empty string)
        String[] row = new String[18];
        Arrays.fill(row, "");
        row[0] = "EE1234";       // account_number
        row[1] = "001";          // document_nr
        row[2] = "2024-06-01";   // payment_date
        row[7] = "100.00";       // payment_amount
        row[12] = "EUR";         // currency
        row[17] = null;          // provider_reference is null

        List<String[]> rows = Collections.singletonList(row);

        Set<String> existingKeys = new HashSet<>(Collections.singletonList(
                "EE1234|001|2024-06-01|100.00|EUR|"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(1, result.getDuplicateCount());
        assertEquals(0, result.getNonDuplicateRows().size());
    }

    @Test
    public void shortRowFallsBackToCompositeGracefully() {
        // Row shorter than expected (e.g., Swedbank with 14 fields — no provider_reference at index 17)
        String[] row = new String[14];
        Arrays.fill(row, "");
        row[0] = "EE9999";       // account_number
        row[1] = "010";          // document_nr
        row[2] = "2024-07-15";   // payment_date
        row[7] = "50.00";        // payment_amount
        row[12] = "EUR";         // currency
        // No index 17 at all

        List<String[]> rows = Collections.singletonList(row);

        Set<String> existingKeys = new HashSet<>(Collections.singletonList(
                "EE9999|010|2024-07-15|50.00|EUR|"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(1, result.getDuplicateCount());
        assertEquals(0, result.getNonDuplicateRows().size());
    }

    @Test
    public void compositeKeyFieldsAreTrimmed() {
        // Row with whitespace in composite key fields, empty provider_reference
        String[] row = bankRow(" EE1234 ", " 001 ", " 2024-06-01 ", " 100.00 ", " EUR ", "");

        List<String[]> rows = Collections.singletonList(row);

        // Existing key was built from trimmed DB values
        Set<String> existingKeys = new HashSet<>(Collections.singletonList(
                "EE1234|001|2024-06-01|100.00|EUR|"));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(1, result.getDuplicateCount());
        assertEquals(0, result.getNonDuplicateRows().size());
    }

    @Test
    public void mixOfPrimaryAndCompositeKeyRows() {
        // Some rows have provider_reference, some don't — mixed dedup strategies
        String[] row1 = bankRow("EE1234", "001", "2024-06-01", "100.00", "EUR", "REF001");  // primary
        String[] row2 = bankRow("EE1234", "002", "2024-06-02", "200.00", "EUR", "");         // composite
        String[] row3 = bankRow("EE1234", "003", "2024-06-03", "300.00", "EUR", "REF003");  // primary

        List<String[]> rows = Arrays.asList(row1, row2, row3);

        // REF001 (primary) and row2's composite key both exist
        Set<String> existingKeys = new HashSet<>(Arrays.asList(
                "REF001",
                "EE1234|002|2024-06-02|200.00|EUR|"
        ));

        DeduplicationResult result = DeduplicationChecker.check(rows, "bank", existingKeys);

        assertEquals(2, result.getDuplicateCount());
        assertEquals(1, result.getNonDuplicateRows().size());
        assertEquals("REF003", result.getNonDuplicateRows().get(0)[17]);
    }
}
