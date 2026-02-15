package com.fiscaladmin.gam.statementimporter.parser;

import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class CsvFormatDetectorTest {

    // ── Helpers ──────────────────────────────────────────────────────────

    private File createTempCsvWithHeader(String headerLine) throws IOException {
        File tempFile = File.createTempFile("test-csv-", ".csv");
        tempFile.deleteOnExit();
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8))) {
            writer.write(headerLine);
            writer.newLine();
        }
        return tempFile;
    }

    private File createTempCsvWithBomAndHeader(String headerLine) throws IOException {
        File tempFile = File.createTempFile("test-csv-bom-", ".csv");
        tempFile.deleteOnExit();
        try (OutputStream os = new FileOutputStream(tempFile)) {
            os.write(0xEF);
            os.write(0xBB);
            os.write(0xBF);
            os.write(headerLine.getBytes(StandardCharsets.UTF_8));
            os.write('\n');
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

    private static final String SWEDBANK_HEADER =
            "\"Kliendi konto\";\"Dok nr\";\"Makse kuupäev\";\"Saaja/maksja konto\";"
                    + "\"Saaja/maksja nimi\";\"Saaja/maksja pank\";\"D/C\";\"Summa\";"
                    + "\"Viitenumber\";\"Arhiveerimistunnus\";\"Selgitus\";\"Teenustasu\";"
                    + "\"Valuuta\";\"Isikukood/registrikood\"";

    private static final String SECU_HEADER =
            "\"VÄÄRTUSPÄEV\",\"TEHINGUPÄEV\",\"TEHING\",\"SÜMBOL\",\"VÄÄRTPABER\","
                    + "\"KOGUS\",\"HIND\",\"VALUUTA\",\"NETOSUMMA\",\"TEENUSTASU\","
                    + "\"KOKKU\",\"VIIDE\",\"KOMMENTAAR\"";

    // ── Unit tests ──────────────────────────────────────────────────────

    @Test
    public void detectLhvBankFormat() throws Exception {
        File file = createTempCsvWithHeader(LHV_HEADER);
        assertEquals(Format.LHV_BANK, CsvFormatDetector.detect(file));
    }

    @Test
    public void detectSwedBankFormat() throws Exception {
        File file = createTempCsvWithHeader(SWEDBANK_HEADER);
        assertEquals(Format.SWEDBANK, CsvFormatDetector.detect(file));
    }

    @Test
    public void detectSecuritiesFormat() throws Exception {
        File file = createTempCsvWithHeader(SECU_HEADER);
        assertEquals(Format.SECURITIES, CsvFormatDetector.detect(file));
    }

    @Test
    public void detectWithBomPresent() throws Exception {
        File file = createTempCsvWithBomAndHeader(LHV_HEADER);
        assertEquals(Format.LHV_BANK, CsvFormatDetector.detect(file));
    }

    @Test
    public void detectSecuritiesWithBomPresent() throws Exception {
        File file = createTempCsvWithBomAndHeader(SECU_HEADER);
        assertEquals(Format.SECURITIES, CsvFormatDetector.detect(file));
    }

    @Test(expected = UnrecognisedFormatException.class)
    public void unknownFormatThrowsException() throws Exception {
        File file = createTempCsvWithHeader("\"Name\",\"Age\",\"City\"");
        CsvFormatDetector.detect(file);
    }

    @Test(expected = UnrecognisedFormatException.class)
    public void emptyFileThrowsException() throws Exception {
        File tempFile = File.createTempFile("test-csv-empty-", ".csv");
        tempFile.deleteOnExit();
        // Write nothing — 0 bytes
        CsvFormatDetector.detect(tempFile);
    }

    @Test
    public void caseInsensitiveDetection() throws Exception {
        // Lowercase, no quotes
        String lowerHeader = "kliendi konto,dokumendi number,kuupäev,saaja/maksja konto,"
                + "saaja/maksja nimi,saaja panga kood,tühi,deebet/kreedit (d/c),"
                + "summa,viitenumber,arhiveerimistunnus,selgitus,"
                + "teenustasu,valuuta,isikukood või registrikood,"
                + "saaja/maksja panga bic,makse algataja nimi,"
                + "kande viide,konto teenusepakkuja viide";
        File file = createTempCsvWithHeader(lowerHeader);
        assertEquals(Format.LHV_BANK, CsvFormatDetector.detect(file));
    }

    // ── Integration tests (real sample files) ───────────────────────────

    @Test
    public void realBankFileDetection() throws Exception {
        File realFile = new File("test-data/bank_statement.csv");
        if (!realFile.exists()) {
            return; // skip in environments without sample data
        }
        assertEquals(Format.LHV_BANK, CsvFormatDetector.detect(realFile));
    }

    @Test
    public void realSecuFileDetection() throws Exception {
        File realFile = new File("test-data/secu_statement.csv");
        if (!realFile.exists()) {
            return; // skip in environments without sample data
        }
        assertEquals(Format.SECURITIES, CsvFormatDetector.detect(realFile));
    }
}
