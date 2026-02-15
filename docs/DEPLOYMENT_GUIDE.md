# Statement Importer Plugin — Deployment Guide

This guide covers deploying the Statement Importer plugin to Joget DX 8.x with the wf-activator integration.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Build Dependencies](#2-build-dependencies)
3. [Build the Plugin](#3-build-the-plugin)
4. [Deploy Plugins to Joget](#4-deploy-plugins-to-joget)
5. [Form Requirements](#5-form-requirements)
6. [Create Workflow Process](#6-create-workflow-process)
7. [Configure Form Post-Processing](#7-configure-form-post-processing)
8. [Testing](#8-testing)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

| Component | Version | Notes |
|-----------|---------|-------|
| Joget DX | 8.x | Running at `http://localhost:8082/jw` |
| Java | 11+ | JDK for building |
| Maven | 3.6+ | For dependency resolution |
| MySQL/MariaDB | 5.7+ / 10.x | Joget's database |

### Verify Joget is Running

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8082/jw
# Should return: 200 or 302
```

---

## 2. Build Dependencies

### 2.1 Install gam-framework to Local Maven Repository

The `statement-importer` plugin depends on `gam-framework`. This dependency is **embedded** into the plugin JAR, but Maven needs it during compilation.

```bash
cd /path/to/gam-plugins/gam-framework
mvn clean install
```

### 2.2 Build wf-activator (if not already built)

```bash
cd /path/to/gs-plugins/wf-activator
mvn clean package
```

---

## 3. Build the Plugin

```bash
cd /path/to/gam-plugins/statement-importer
mvn clean package
```

**Output JAR:** `target/statement-importer-8.1-SNAPSHOT.jar`

The bundle includes:
- Statement Importer plugin code
- Embedded `gam-framework` classes
- Embedded `commons-csv` library

---

## 4. Deploy Plugins to Joget

### 4.1 Deploy wf-activator Plugin

1. Open: `http://localhost:8082/jw/web/console/setting/plugin`
2. Click **Upload Plugin**
3. Select: `wf-activator-8.1-SNAPSHOT.jar`
4. Verify "Workflow Activator" appears in plugin list

### 4.2 Deploy Statement Importer Plugin

1. Open: `http://localhost:8082/jw/web/console/setting/plugin`
2. Click **Upload Plugin**
3. Select: `statement-importer-8.1-SNAPSHOT.jar`
4. Verify "Statement Importer" appears in plugin list

---

## 5. Form Requirements

### 5.1 Statement Form (`bankStatement`)

**Table name:** `bank_statement`

| Field ID | Type | Required | Notes |
|----------|------|----------|-------|
| `id` | Hidden/Auto | Yes | Record ID (auto-generated) |
| `bank` | Select/Text | Yes | Bank code |
| `account_type` | Select | Yes | Values: `bank` or `secu` |
| `from_date` | Date Picker | No | Statement start date |
| `to_date` | Date Picker | No | Statement end date |
| `status` | Select/Text | Yes | Default: `new` |
| `statement_file` | File Upload | Yes | CSV file attachment |
| `row_count` | Text/Number | No | Populated by plugin |
| `duplicate_count` | Text/Number | No | Populated by plugin |
| `error_message` | Text Area | **Yes** | **Required for error handling** |
| `processing_timestamp` | Text | No | Populated by plugin |

> **Important:** The `error_message` field MUST exist or the plugin will fail when trying to log errors.

### 5.2 Bank Transaction Form (`bankAccountTransaction`)

**Table name:** `bank_account_trx`

| Field ID | Type |
|----------|------|
| `statement_id` | Hidden |
| `account_number` | Text |
| `document_nr` | Text |
| `payment_date` | Text |
| `other_side_account` | Text |
| `other_side_name` | Text |
| `other_side_bank` | Text |
| `d_c` | Text |
| `payment_amount` | Text |
| `reference_number` | Text |
| `archival_number` | Text |
| `payment_description` | Text |
| `transaction_fee` | Text |
| `currency` | Text |
| `customer_id` | Text |
| `other_side_bic` | Text |
| `initiator` | Text |
| `transaction_reference` | Text |
| `provider_reference` | Text |

### 5.3 Securities Transaction Form (`securityTransaction`)

**Table name:** `sec_account_trx`

| Field ID | Type |
|----------|------|
| `statement_id` | Hidden |
| `value_date` | Text |
| `transaction_date` | Text |
| `type` | Text |
| `ticker` | Text |
| `description` | Text |
| `quantity` | Text |
| `price` | Text |
| `currency` | Text |
| `amount` | Text |
| `fee` | Text |
| `total_amount` | Text |
| `reference` | Text |
| `comment` | Text |

---

## 6. Create Workflow Process

### 6.1 Process Name (Critical!)

Create a process with this **exact name**:

```
gam_statement_submission
```

The wf-activator uses convention: `{serviceId}_submission`, so with `serviceId = "gam_statement"` it looks for `gam_statement_submission`.

### 6.2 BPMN Flow

```
┌─────────┐     ┌─────────────────────┐     ┌─────────────┐
│  Start  │────▶│  Statement Importer │────▶│     End     │
│         │     │  (Tool Activity)    │     │             │
└─────────┘     └─────────────────────┘     └─────────────┘
```

Simple flow:
1. **Start Event**
2. **Tool Activity** — Statement Importer plugin
3. **End Event**

### 6.3 Define Workflow Variable (Required!)

**This is the critical step.** You must define ONE workflow variable:

1. In Process Builder, click on the **canvas** (not on any activity)
2. Find **Workflow Variables** section in properties panel
3. Add variable:
   - **Name:** `id`
   - **Type:** String
4. **Save** and **Deploy** the process

> **Why?** Joget requires workflow variables to be pre-defined before they can receive values. The wf-activator passes `id` (the form record ID), but without this variable defined, Joget rejects it.

### 6.4 Configure Tool Activity

1. Click on the Tool Activity
2. Go to **Mapping** tab → **Tools**
3. Select: **Statement Importer**
4. No additional configuration needed — the plugin reads all data from the form automatically

---

## 7. Configure Form Post-Processing

### 7.1 Add wf-activator to Statement Form

1. Open **Form Builder** for `bankStatement` form
2. Go to form **Properties** (gear icon)
3. Find **Post Processing Tool** section
4. Click **Add**
5. Select: **Workflow Activator**

### 7.2 Configure wf-activator

| Setting | Value |
|---------|-------|
| **Service ID** | `gam_statement` |
| **Use Naming Convention** | ✅ Checked |
| **Process Definition ID** | (leave empty) |
| **Process Name** | (leave empty) |
| **Run Mode** | Synchronous |
| **Pass Form Data** | ✅ Checked |
| **Custom Workflow Variables** | (leave empty) |
| **Participant Mapping** | `admin` or `#currentUser.username#` |
| **Enable Detailed Logging** | ✅ Checked (for debugging) |

6. Click **OK**
7. **Save** the form

---

## 8. Testing

### 8.1 Test Bank Statement Import

1. Open the statement form (via CRUD or UI menu)
2. Fill in:
   - **Bank:** Select a bank
   - **Account Type:** `bank`
   - **From Date / To Date:** Optional
   - **Statement File:** Upload `test-data/bank_statement.csv`
3. Click **Submit**
4. Verify results:

```sql
-- Check statement status
SELECT id, c_status, c_row_count, c_duplicate_count
FROM app_fd_bank_statement
ORDER BY dateCreated DESC LIMIT 1;

-- Expected: c_status='imported', c_row_count=161

-- Check transaction rows
SELECT COUNT(*) FROM app_fd_bank_account_trx
WHERE c_statement_id = 'YOUR_STATEMENT_ID';

-- Expected: 161
```

### 8.2 Test Securities Statement Import

1. Create new statement with:
   - **Account Type:** `secu`
   - **Statement File:** Upload `test-data/secu_statement.csv`
2. Submit
3. Verify: 45 rows in `app_fd_sec_account_trx`

### 8.3 Test Re-Import (Idempotency)

1. Open an existing imported statement
2. Submit again (re-import)
3. Verify: Same row count (old rows deleted, new rows inserted)

---

## 9. Troubleshooting

### 9.1 Enable Debug Logging

Add to Joget's `log4j.properties`:

```properties
log4j.logger.com.fiscaladmin.gam.statementimporter=DEBUG
log4j.logger.global.govstack.workflow.activator=DEBUG
```

### 9.2 Check Logs

```bash
tail -f /path/to/joget/apache-tomcat-*/logs/joget.log | grep -E "(StatementImporter|WorkflowActivator)"
```

### 9.3 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Statement record not found: null" | `id` variable not defined in process | Add `id` workflow variable to process |
| "Statement record not found: 12345" | Wrong ID extracted (process number, not record ID) | Ensure `id` variable is defined and process is redeployed |
| "Unknown column 'c_error_message'" | Missing `error_message` field in form | Add `error_message` field to statement form |
| "context attribute X does not exist" | Workflow variable not pre-defined | Define required variables in process |
| "Format mismatch" | CSV format doesn't match account_type | Verify account_type matches CSV format |
| "No file attached" | File upload failed | Check file upload configuration |

### 9.4 Successful Log Pattern

```
INFO  WorkflowActivator - ===== WorkflowActivator v2.0 STARTING =====
INFO  WorkflowActivator - Found recordId directly: 86375ba9-...
INFO  WorkflowActivator - Using convention-based process: gam_statement_submission
INFO  WorkflowActivator - Successfully started workflow process: ...
INFO  StatementImporter - Got recordId from workflow variable 'id': 86375ba9-...
INFO  StatementImporter - Starting Statement Import for record: 86375ba9-...
INFO  StatementImporter - Loaded form data - accountType: bank, bank: LHVBEE22, file: ...
INFO  StatementImporter - Detected CSV format: LHV_BANK
INFO  StatementImporter - Parsed 161 rows from CSV
INFO  StatementImporter - Inserted 161 rows into app_fd_bank_account_trx
INFO  StatementImporter - Statement Import completed successfully
```

---

## Quick Reference

| Item | Value |
|------|-------|
| Plugin JAR | `target/statement-importer-8.1-SNAPSHOT.jar` |
| wf-activator JAR | `wf-activator-8.1-SNAPSHOT.jar` |
| Process Name | `gam_statement_submission` |
| Service ID | `gam_statement` |
| Required Workflow Variable | `id` (String) |
| Bank Test CSV | `test-data/bank_statement.csv` (161 rows) |
| Securities Test CSV | `test-data/secu_statement.csv` (45 rows) |

---

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────────┐
│                    User Submits Form                            │
│                  (bankStatement form)                           │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                 wf-activator                                    │
│           (Post-Processing Tool)                                │
│  - serviceId: gam_statement                                     │
│  - Passes form data as workflow variables                       │
│  - Starts: gam_statement_submission process                     │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│              gam_statement_submission                           │
│                (Workflow Process)                               │
│  - Receives 'id' workflow variable                              │
│  - Executes Statement Importer tool                             │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│               Statement Importer                                │
│              (Process Tool Plugin)                              │
│  1. Gets record ID from 'id' workflow variable                  │
│  2. Loads form record directly from database                    │
│  3. Reads: account_type, bank, dates, file                      │
│  4. Detects CSV format                                          │
│  5. Parses CSV rows                                             │
│  6. Checks for duplicates                                       │
│  7. Batch inserts to bank_account_trx or sec_account_trx        │
│  8. Updates statement: status=imported, row_count, etc.         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 8.1-SNAPSHOT | 2026-02-15 | Initial deployment with wf-activator integration |
