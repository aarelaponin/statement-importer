package com.fiscaladmin.gam.statementimporter.parser;

import com.fiscaladmin.gam.statementimporter.mapping.MappingConfigurations;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

public class StatementParserTest {

    // ── Helpers ──────────────────────────────────────────────────────────

    private File createTempCsv(String content) throws IOException {
        File tempFile = File.createTempFile("test-csv-", ".csv");
        tempFile.deleteOnExit();
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8))) {
            writer.write(content);
        }
        return tempFile;
    }

    private File createTempCsvWithBom(String content) throws IOException {
        File tempFile = File.createTempFile("test-csv-bom-", ".csv");
        tempFile.deleteOnExit();
        try (OutputStream os = new FileOutputStream(tempFile)) {
            os.write(0xEF);
            os.write(0xBB);
            os.write(0xBF);
            os.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return tempFile;
    }

    // ── Header constants ────────────────────────────────────────────────

    private static final String LHV_HEADER =
            "\"Kliendi konto\",\"Dokumendi number\",\"Kuupäev\",\"Saaja/maksja konto\","
                    + "\"Saaja/maksja nimi\",\"Saaja panga kood\",\"Tühi\",\"Deebet/Kreedit (D/C)\","
                    + "\"Summa\",\"Viitenumber\",\"Arhiveerimistunnus\",\"Selgitus\","
                    + "\"Teenustasu\",\"Valuuta\",\"Isikukood või registrikood\","
                    + "\"Saaja/maksja panga BIC\",\"Makse algataja nimi\","
                    + "\"Kande viide\",\"Konto teenusepakkuja viide\"";

    private static final String SECU_HEADER =
            "\"VÄÄRTUSPÄEV\",\"TEHINGUPÄEV\",\"TEHING\",\"SÜMBOL\",\"VÄÄRTPABER\","
                    + "\"KOGUS\",\"HIND\",\"VALUUTA\",\"NETOSUMMA\",\"TEENUSTASU\","
                    + "\"KOKKU\",\"VIIDE\",\"KOMMENTAAR\"";

    private static final String SWEDBANK_HEADER =
            "\"Kliendi konto\";\"Dok nr\";\"Makse kuupäev\";\"Saaja/maksja konto\";"
                    + "\"Saaja/maksja nimi\";\"Saaja/maksja pank\";\"D/C\";\"Summa\";"
                    + "\"Viitenumber\";\"Arhiveerimistunnus\";\"Selgitus\";\"Teenustasu\";"
                    + "\"Valuuta\";\"Isikukood/registrikood\"";

    // ── Synthetic unit tests ────────────────────────────────────────────

    @Test
    public void parseLhvBankSyntheticStripsField6() throws Exception {
        String content = LHV_HEADER + "\r\n"
                + "\"EE123\",\"DOC001\",\"2024-06-01\",\"EE456\",\"John Doe\",\"BANK\",\"\","
                + "\"C\",\"100.00\",\"REF1\",\"ARCH1\",\"Test payment\","
                + "\"0.50\",\"EUR\",\"12345678\",\"SWIFT1\",\"Init1\",\"TX001\",\"PROV001\"\r\n"
                + "\"EE123\",\"DOC002\",\"2024-06-02\",\"EE789\",\"Jane Doe\",\"BANK2\",\"\","
                + "\"D\",\"-50.00\",\"REF2\",\"ARCH2\",\"Another payment\","
                + "\"0.25\",\"USD\",\"87654321\",\"SWIFT2\",\"Init2\",\"TX002\",\"PROV002\"\r\n";

        File file = createTempCsv(content);
        List<String[]> rows = StatementParser.parse(file, Format.LHV_BANK);

        assertEquals(2, rows.size());
        assertEquals(18, rows.get(0).length);   // 19 - 1 stripped
        assertEquals(18, rows.get(1).length);

        // Row 0 — verify field positions after stripping index 6
        assertEquals("EE123", rows.get(0)[0]);      // account_number
        assertEquals("DOC001", rows.get(0)[1]);      // document_nr
        assertEquals("2024-06-01", rows.get(0)[2]);  // payment_date
        assertEquals("EE456", rows.get(0)[3]);       // other_side_account
        assertEquals("John Doe", rows.get(0)[4]);    // other_side_name
        assertEquals("BANK", rows.get(0)[5]);        // other_side_bank
        assertEquals("C", rows.get(0)[6]);           // d_c (was index 7)
        assertEquals("100.00", rows.get(0)[7]);      // payment_amount (was index 8)
        assertEquals("PROV001", rows.get(0)[17]);    // provider_reference (was 18)

        // Row 1
        assertEquals("D", rows.get(1)[6]);
        assertEquals("-50.00", rows.get(1)[7]);
        assertEquals("PROV002", rows.get(1)[17]);
    }

    @Test
    public void parseSecuritiesSyntheticNoStripping() throws Exception {
        String content = SECU_HEADER + "\r\n"
                + "2024-06-07,2024-06-05,\"ost\",\"AAPL\",\"Apple Inc\",10,150.50,"
                + "\"USD\",-1505.00,-5.00,-1510.00,\"REF001\",\"\"\r\n"
                + "2024-06-08,2024-06-06,\"müük\",\"MSFT\",\"Microsoft Corp\",5,300.00,"
                + "\"EUR\",1500.00,-3.00,1497.00,\"REF002\",\"test comment\"\r\n";

        File file = createTempCsv(content);
        List<String[]> rows = StatementParser.parse(file, Format.SECURITIES);

        assertEquals(2, rows.size());
        assertEquals(13, rows.get(0).length);
        assertEquals(13, rows.get(1).length);

        assertEquals("2024-06-07", rows.get(0)[0]);  // value_date
        assertEquals("ost", rows.get(0)[2]);          // type
        assertEquals("AAPL", rows.get(0)[3]);         // ticker
        assertEquals("USD", rows.get(0)[7]);          // currency
        assertEquals("REF001", rows.get(0)[11]);      // reference
        assertEquals("", rows.get(0)[12]);            // comment (empty)

        assertEquals("müük", rows.get(1)[2]);
        assertEquals("MSFT", rows.get(1)[3]);
        assertEquals("test comment", rows.get(1)[12]);
    }

    @Test
    public void parseSwedbankSyntheticSemicolonSeparated() throws Exception {
        String content = SWEDBANK_HEADER + "\r\n"
                + "\"EE999\";\"SW001\";\"2024-06-01\";\"EE888\";\"Test Person\";"
                + "\"Swedbank\";\"C\";\"200.00\";\"REF1\";\"ARCH1\";"
                + "\"Swedbank payment\";\"1.00\";\"EUR\";\"11223344\"\r\n";

        File file = createTempCsv(content);
        List<String[]> rows = StatementParser.parse(file, Format.SWEDBANK);

        assertEquals(1, rows.size());
        assertEquals(14, rows.get(0).length);
        assertEquals("EE999", rows.get(0)[0]);
        assertEquals("SW001", rows.get(0)[1]);
        assertEquals("C", rows.get(0)[6]);
        assertEquals("200.00", rows.get(0)[7]);
        assertEquals("EUR", rows.get(0)[12]);
        assertEquals("11223344", rows.get(0)[13]);
    }

    @Test
    public void parseEmptyFileReturnsEmptyList() throws Exception {
        // Header only, no data rows
        File file = createTempCsv(SECU_HEADER + "\r\n");
        List<String[]> rows = StatementParser.parse(file, Format.SECURITIES);
        assertTrue(rows.isEmpty());
    }

    @Test
    public void parseHandlesBomCorrectly() throws Exception {
        String content = SECU_HEADER + "\r\n"
                + "2024-06-07,2024-06-05,\"ost\",\"AAPL\",\"Apple Inc\",10,150.50,"
                + "\"USD\",-1505.00,-5.00,-1510.00,\"REF001\",\"\"\r\n";

        File file = createTempCsvWithBom(content);
        List<String[]> rows = StatementParser.parse(file, Format.SECURITIES);

        assertEquals(1, rows.size());
        assertEquals(13, rows.get(0).length);
        assertEquals("2024-06-07", rows.get(0)[0]);
        assertEquals("AAPL", rows.get(0)[3]);
    }

    @Test
    public void parsePreservesEmptyFieldsAsEmptyStrings() throws Exception {
        String content = LHV_HEADER + "\r\n"
                + "\"EE123\",\"\",\"2024-06-01\",\"\",\"\",\"\",\"\","
                + "\"C\",\"0.00\",\"\",\"\",\"Interest payment\","
                + "\"0.00\",\"EUR\",\"\",\"\",\"\",\"\",\"\"\r\n";

        File file = createTempCsv(content);
        List<String[]> rows = StatementParser.parse(file, Format.LHV_BANK);

        assertEquals(1, rows.size());
        assertEquals(18, rows.get(0).length);

        // Verify empty fields are empty strings, not null
        for (String field : rows.get(0)) {
            assertNotNull("All fields must be non-null", field);
        }
        assertEquals("", rows.get(0)[1]);   // document_nr (empty)
        assertEquals("", rows.get(0)[3]);   // other_side_account (empty)
        assertEquals("EE123", rows.get(0)[0]);
        assertEquals("C", rows.get(0)[6]);  // d_c after stripping
        assertEquals("Interest payment", rows.get(0)[10]); // payment_description
    }

    // ── Integration tests (real CSV files) ──────────────────────────────

    @Test
    public void realBankFileParsing() throws Exception {
        File realFile = new File("test-data/bank_statement.csv");
        if (!realFile.exists()) {
            return; // skip in environments without sample data
        }

        List<String[]> rows = StatementParser.parse(realFile, Format.LHV_BANK);
        assertEquals(161, rows.size());
        assertEquals(18, rows.get(0).length);

        // First row assertions
        assertEquals("EE117700771003603322", rows.get(0)[0]);
        assertEquals("2024-06-05", rows.get(0)[2]);
        assertEquals("C", rows.get(0)[6]);
        assertEquals("0.31", rows.get(0)[7]);
        assertEquals("EUR", rows.get(0)[12]);
        assertEquals("17401979DC22EF11B45B001DD8D11D14", rows.get(0)[17]);

        // Last row assertions
        String[] lastRow = rows.get(rows.size() - 1);
        assertEquals("EE117700771003603322", lastRow[0]);
        assertEquals("2024-07-30", lastRow[2]);
        assertEquals("D", lastRow[6]);
        assertEquals("-12.84", lastRow[7]);
        assertEquals("USD", lastRow[12]);
        assertEquals("C5D38254B04DEF11B45B001DD8D11D14", lastRow[17]);
    }

    @Test
    public void realSecuFileParsing() throws Exception {
        File realFile = new File("test-data/secu_statement.csv");
        if (!realFile.exists()) {
            return; // skip in environments without sample data
        }

        List<String[]> rows = StatementParser.parse(realFile, Format.SECURITIES);
        assertEquals(45, rows.size());
        assertEquals(13, rows.get(0).length);

        // First row
        assertEquals("2024-06-07", rows.get(0)[0]);
        assertEquals("2024-06-05", rows.get(0)[1]);
        assertEquals("ost", rows.get(0)[2]);
        assertEquals("HLMBK095034FA", rows.get(0)[3]);
        assertEquals("EUR", rows.get(0)[7]);
        assertEquals("903745878", rows.get(0)[11]);

        // Last row
        String[] lastRow = rows.get(rows.size() - 1);
        assertEquals("2024-08-01", lastRow[0]);
        assertEquals("ost", lastRow[2]);
        assertEquals("SMCI", lastRow[3]);
        assertEquals("USD", lastRow[7]);
        assertEquals("940524330", lastRow[11]);
    }

    @Test
    public void fieldCountsAlignWithMappingConfigurations() throws Exception {
        File bankFile = new File("test-data/bank_statement.csv");
        File secuFile = new File("test-data/secu_statement.csv");
        if (!bankFile.exists() || !secuFile.exists()) {
            return;
        }

        List<String[]> bankRows = StatementParser.parse(bankFile, Format.LHV_BANK);
        int expectedBankFields = MappingConfigurations.LHV_BANK_CONFIG.getFieldMappings().size();
        assertEquals(expectedBankFields, bankRows.get(0).length);

        List<String[]> secuRows = StatementParser.parse(secuFile, Format.SECURITIES);
        int expectedSecuFields = MappingConfigurations.SECU_CONFIG.getFieldMappings().size();
        assertEquals(expectedSecuFields, secuRows.get(0).length);
    }
}
