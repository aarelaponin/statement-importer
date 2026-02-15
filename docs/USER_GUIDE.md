# Statement Importer User Guide

This guide explains how to use the Statement Importer plugin to import bank and securities account statements into the GAM platform.

## Table of Contents

- [Introduction](#introduction)
- [Supported Statement Types](#supported-statement-types)
- [Preparing Your CSV File](#preparing-your-csv-file)
- [Uploading a Statement](#uploading-a-statement)
- [Understanding the Import Process](#understanding-the-import-process)
- [Viewing Import Results](#viewing-import-results)
- [Handling Duplicates](#handling-duplicates)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)

---

## Introduction

The Statement Importer automatically processes CSV statement files from your bank or securities account. It:

- Detects the statement format automatically
- Extracts all transactions from the file
- Identifies and skips duplicate transactions
- Stores the transactions for further processing

No manual data entry is required - just upload your CSV file and the system handles the rest.

---

## Supported Statement Types

### Bank Statements

| Bank | Format | Supported |
|------|--------|-----------|
| LHV Bank | CSV (comma-separated) | Yes |
| Swedbank | CSV (semicolon-separated) | Yes |

### Securities Statements

| Provider | Format | Supported |
|----------|--------|-----------|
| LHV Securities | CSV (comma-separated) | Yes |

---

## Preparing Your CSV File

### Downloading from LHV Bank

1. Log in to your LHV Internet Bank
2. Go to **Account** > **Account Statement**
3. Select the date range
4. Choose **CSV** as the export format
5. Click **Download**

### Downloading from Swedbank

1. Log in to Swedbank Internet Bank
2. Navigate to **Account Statement**
3. Set the date range
4. Select **Export to CSV**
5. Download the file

### Downloading Securities Statements

1. Log in to your securities account portal
2. Go to **Transaction History**
3. Select the period
4. Export as **CSV**

### File Requirements

| Requirement | Details |
|-------------|---------|
| Format | CSV (Comma or Semicolon separated) |
| Encoding | UTF-8 (BOM optional) |
| Header Row | Must include original bank headers |
| Maximum Size | 10 MB recommended |

**Important**: Do not modify the CSV file after downloading. The system uses the header row to detect the format.

---

## Uploading a Statement

### Step 1: Create New Statement Record

1. Navigate to **Statements** in the main menu
2. Click **New Statement**
3. Fill in the required fields:

| Field | Description |
|-------|-------------|
| Account Type | Select "Bank" or "Securities" |
| Bank | Select your bank from the dropdown |
| From Date | Start date of the statement period |
| To Date | End date of the statement period |
| Statement File | Upload your CSV file |

4. Click **Submit**

### Step 2: Automatic Processing

After submission:

1. The statement status changes to **Importing**
2. The system processes your file automatically
3. When complete, status changes to **Imported**

### Step 3: Review Results

The system will display:

- Total rows in the file
- Number of new transactions imported
- Number of duplicate transactions skipped

---

## Understanding the Import Process

### Processing Pipeline

```
Upload File
    │
    ▼
Detect Format ─────────────► (LHV Bank / Swedbank / Securities)
    │
    ▼
Parse Transactions ────────► Extract all rows from CSV
    │
    ▼
Check Duplicates ──────────► Compare with existing transactions
    │
    ▼
Import New Rows ───────────► Store non-duplicate transactions
    │
    ▼
Update Statistics ─────────► Record counts and timestamp
    │
    ▼
Complete
```

### Status Values

| Status | Meaning |
|--------|---------|
| New | Statement created, not yet processed |
| Importing | Processing in progress |
| Imported | Successfully processed |
| Error | Processing failed (see error message) |

---

## Viewing Import Results

### Statement Details

After import, view the statement record to see:

- **Row Count**: Total transactions in the file
- **Duplicate Count**: Transactions that already existed
- **Processing Timestamp**: When the import completed

### Transaction List

To view imported transactions:

1. Open the Statement record
2. Click **View Transactions**
3. Browse or search the transaction list

### Export Options

You can export the imported transactions:

1. Go to the Transaction list
2. Use filters to select transactions
3. Click **Export** to download as CSV or Excel

---

## Handling Duplicates

### What Are Duplicates?

Duplicates occur when you upload overlapping statements. For example:

- January statement: Jan 1 - Jan 31
- February statement: Jan 25 - Feb 28

The February statement contains transactions from Jan 25-31 that were already imported.

### How Duplicates Are Detected

#### Bank Transactions

1. **Primary**: Provider Reference number (unique transaction ID from the bank)
2. **Fallback**: Combination of Account + Document Number + Date + Amount + Currency

#### Securities Transactions

1. **Primary**: Reference number
2. **Fallback**: Combination of Value Date + Transaction Date + Type + Ticker + Amount + Currency

### Why Skip Duplicates?

- Prevents double-counting in reports
- Maintains data integrity
- Allows flexible statement overlaps

---

## Troubleshooting

### "Unrecognised CSV format"

**Problem**: The system cannot identify the statement format.

**Solutions**:
1. Verify you're uploading a supported bank's CSV
2. Don't modify the CSV header row
3. Check that the file isn't corrupted
4. Try downloading the statement again

### "Format mismatch"

**Problem**: The detected format doesn't match the account type you selected.

**Solutions**:
1. Check if you selected the correct Account Type (Bank vs Securities)
2. Ensure you're uploading the correct file

### "CSV file not found"

**Problem**: The uploaded file cannot be accessed.

**Solutions**:
1. Try uploading the file again
2. Check your network connection
3. Verify the file size is under 10 MB

### "Statement record not found"

**Problem**: System cannot find the statement record.

**Solutions**:
1. Wait a moment and try again
2. Contact support if the issue persists

### Import Stuck on "Importing"

**Problem**: The status doesn't change after several minutes.

**Solutions**:
1. Wait up to 5 minutes for large files
2. Check if there's a system notification
3. Contact support if stuck longer

### All Transactions Marked as Duplicates

**Problem**: 100% duplicate rate when you expect new transactions.

**Possible Causes**:
1. You already imported this exact statement
2. The date range overlaps completely with a previous import
3. The statement was re-processed after an error

**Solution**: Check your previous statement imports for overlapping date ranges.

---

## FAQ

### Q: Can I import the same statement twice?

**A**: Yes, but the second import will mark all transactions as duplicates. The system is designed for safe re-processing.

### Q: What happens if import fails?

**A**: The statement status becomes "Error" with a message. You can fix the issue and submit again - previous partial imports are automatically cleaned up.

### Q: How long does import take?

**A**: Typically a few seconds. Large statements (500+ rows) may take up to a minute.

### Q: Can I edit imported transactions?

**A**: Raw imported transactions are read-only. Edits happen in the processing stage after import.

### Q: What date format does the CSV use?

**A**: The system accepts standard formats (yyyy-MM-dd, dd.MM.yyyy) as provided by the banks.

### Q: Can I import multiple statements at once?

**A**: Currently, statements must be imported one at a time. Each upload creates a separate statement record.

### Q: How do I delete an imported statement?

**A**: Contact your administrator. Deleting a statement removes all associated transactions.

### Q: Why is there a date range requirement?

**A**: The date range is used for:
- Duplicate detection (finding overlapping statements)
- Report filtering
- Audit trail

### Q: What if my CSV has extra columns?

**A**: Extra columns are ignored. The system maps only the columns it expects based on the detected format.

### Q: Can I import non-CSV files (PDF, Excel)?

**A**: No, only CSV files are supported. Convert your statement to CSV format before uploading.

---

## Getting Help

If you encounter issues not covered in this guide:

1. Check the error message in the statement record
2. Review the Troubleshooting section above
3. Contact your system administrator
4. For technical issues, administrators can check server logs

---

*Document Version: 1.0*
*Last Updated: 2024*
