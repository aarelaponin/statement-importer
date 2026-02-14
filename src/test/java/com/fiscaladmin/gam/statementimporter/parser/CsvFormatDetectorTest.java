package com.fiscaladmin.gam.statementimporter.parser;

import org.junit.Test;

import static org.junit.Assert.*;

public class CsvFormatDetectorTest {

    @Test
    public void detectLhvBankFormat() {
        // TODO: T2.2 — Test with LHV header line containing "dokumendi number"
    }

    @Test
    public void detectSwedBankFormat() {
        // TODO: T2.2 — Test with Swedbank header line containing "dok nr"
    }

    @Test
    public void detectSecuritiesFormat() {
        // TODO: T2.2 — Test with Securities header line containing "vääruspäev" and "tehingupäev"
    }

    @Test
    public void detectWithBomPresent() {
        // TODO: T2.2 — Test BOM stripping (bytes EF BB BF at start of file)
    }

    @Test
    public void unknownFormatThrowsException() throws Exception {
        // TODO: T2.2 — Test with unrecognised header, add @Test(expected = UnrecognisedFormatException.class)
    }
}
