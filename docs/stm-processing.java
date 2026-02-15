
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.UUID;
import java.util.Objects;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.text.DecimalFormat;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import org.joget.commons.util.LogUtil;

import org.joget.apps.app.service.AppUtil;
import org.joget.apps.form.service.FileUtil;
import org.joget.apps.form.model.Form;


// Constants

private static final String LHV_BIC = "LHVBEE22";
private static final String SWE_BIC = "HABAEE2X";

private static final String EUROPE_DATE_FMT = "dd-MM-yyyy";

private static final String ACC_POST_FORM_ID = "accPost";                   // form to store recognised transactions

private static final String SECU_PURCHASE_TRX_CODE = "PS";                  // share purchase
private static final String SECU_SELL_TRX_CODE = "SS";                      // share sell
private static final String SECU_SPLIT_TRX_CODE = "SL";                     // security split
private static final String BANK_CUSTCASHIN_TRX_CODE = "BI";                // Customer bank cash-in transaction

private static final String BANK_REF_FIELD_NAME = "c_bank_payment_ref";
private static final String FEE_REF_FIELD_NAME = "c_fee_payment_ref";
private static final String BANK_TOTAL_FIELD_NAME = "c_total_in_bank";
private static final String ACCOUNT_TYPE_FIELD_NAME = "c_account_type";

private static final String CUSTOMER_ID_FIELD_NAME = "c_customer_row_id";     // in form accPost
private static final String CUSTOMER_REF_FIELD_NAME = "c_customer_reference"; // in form accPost


// Utility Functions

class Utils {

    public static String getCurrentDateAsString() {
        return new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    }

    public static String generateUUID() {
        return java.util.UUID.randomUUID().toString();
    }

    public static String convertColumnNameToPropertyKey(String columnName) {
        // Drop the prefix 'c_' from the column name
        if (columnName.startsWith("c_")) {
            return columnName.substring(2);
        }
        return columnName;
    }

    public static double round(double value, int places) {

        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public static List extractColumnNamesFromSQL(String sql) {

        List columns = new ArrayList();

        // Extract the part of the SQL between SELECT and FROM
        String selectClauseRegex = "(?i)SELECT(.*?)FROM"; // (?i) for case insensitive
        Pattern pattern = Pattern.compile(selectClauseRegex);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            // Get the part between SELECT and FROM
            String selectClause = matcher.group(1).trim();

            // Split the select clause by commas to get individual columns
            String[] columnDefinitions = selectClause.split(",");

            for (String columnDefinition : columnDefinitions) {
                // Extract the column name or alias, which is after the last space in the definition
                String column = columnDefinition.trim();
                String[] parts = column.split("\\s+"); // Split by spaces
                String columnName = parts[parts.length - 1]; // Last part is the alias (or column name if no alias)

                // Remove any prefix before the column name (e.g., 'trx.id' -> 'id')
                if (columnName.contains(".")) {
                    columnName = columnName.substring(columnName.lastIndexOf('.') + 1);
                }

                columns.add(columnName);
            }
        }

        return columns;
    }

    public static String[] removeElementFromArray (String[] originalArray, int indexToRemove) {

        if (indexToRemove < 0 || indexToRemove >= originalArray.length) {
            System.out.println("Invalid index");
            return;
        }

        String[] newArray = new String[originalArray.length - 1];
        for (int i = 0, j = 0; i < originalArray.length; i++) {
            if (i == indexToRemove) {
                continue; // Skip the element at the index to remove
            }
            newArray[j++] = originalArray[i];
        }

        return newArray;
    }
    
    public static String[] listToArray(List columns) {
        String[] columnArray = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnArray[i] = (String) columns.get(i);
        }
        return columnArray;
    }

    public static String getAttr(String sql, String[] params, String returnAttrName) {
        
        Object returnAttribute = null;
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            con = getConnection();
            stmt = con.prepareStatement(sql);
            rs = executeQuery(stmt, params);
            
            if (rs.next()) {
                returnAttribute = rs.getObject(returnAttrName);
            }
            
        } catch (SQLException e) {
            LogUtil.error("getAssetTypeIdByTrxType", e, "Error IN getAssetTypeIdByTrxType");
        } finally {
            close(rs);
            close(stmt);
            close(con);
        }
        
        return returnAttribute;
    }

    public static Map getAttributes(String sql, String[] params, String[] returnAttrNames) {
        Map attributes = new HashMap();
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
    
        try {
            con = getConnection();
            stmt = con.prepareStatement(sql);
            rs = executeQuery(stmt, params);
    
            if (rs.next()) {
                for (String attrName : returnAttrNames) {
                    attributes.put(attrName, rs.getObject(attrName));
                }
            }
    
        } catch (SQLException e) {
            LogUtil.error("getAttributes", e, "Error in getAttributes");
        } finally {
            close(rs);
            close(stmt);
            close(con);
        }
    
        return attributes;
    }

    public static void close(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                
                LogUtil.error("Error closing resource");
            }
        }
    }

    public static PreparedStatement createPreparedStatement(PreparedStatement stm, String columns, Map rowFields) throws SQLException {
        
        String[] columnArray = columns.split(",\\s*");
        for (int i = 0; i < columnArray.length; i++) {
            String columnName = columnArray[i];
            Object value = rowFields.get(columnName);
            stm.setObject(i + 1, value);
        }

        return stm;
    }

    public static String save(Map rowFields, String sql, String columns) {
        Connection connection = null;
        PreparedStatement insertStatement = null;
        String pId = null;
        String assetType = null;
        
        try {
            connection = getConnection();
            insertStatement = connection.prepareStatement(sql);
            insertStatement = Utils.createPreparedStatement(insertStatement, columns, rowFields);
            int rowsInserted = insertStatement.executeUpdate();
            
        } catch (SQLException e) {
            // Handle SQL exceptions
            e.printStackTrace();
            return;
        } finally {
            closeResources(null, insertStatement, connection);
        }
        return pId;
    }
    
    public static Connection getConnection() throws SQLException {
        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");
        this.con = ds.getConnection();
        return this.con;
    }
    
    public static void setParameters(PreparedStatement stmt, Object[] params) {
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
    }

    public static ResultSet executeQuery(PreparedStatement stmt, String[] params) {
    
        try {
    
            if (params != null && params.length > 0) {
                setParameters(stmt, params);
            }
    
            this.rs = stmt.executeQuery();
            return this.rs;                  // Caller must handle closing resources
    
        } catch (SQLException e) {
            LogUtil.error("DatabaseUtils executeQuery", e, "Error executing query: " + sql);
            return null;
        }
    }

    public static void closeResources (ResultSet rs, PreparedStatement stmt, Connection con) {
        close(rs);
        close(stmt);
        close(con);
    }

    public static void executeUpdate(String sql, String[] params) throws SQLException {
        Connection con = null;
        PreparedStatement stmt = null;
    
        try {
            con = Utils.getConnection();
            stmt = con.prepareStatement(sql);
            Utils.setParameters(stmt, params);
            stmt.executeUpdate();
        } catch (SQLException e) {
            LogUtil.error("DatabaseUtils", e, "Error executing update: " + sql);
        } finally {
            closeResources(null, stmt, con);
        }
    }

}

class SqlUtils {
    
    private static Set extractColumns(String sqlQuery) {
        Set columns = new HashSet();
        String selectPart = sqlQuery.substring(sqlQuery.indexOf("SELECT") + 6, sqlQuery.indexOf("FROM")).trim();
        String[] columnArray = selectPart.split(",");
        
        for (String column : columnArray) {
            column = column.trim();
            
            // Handle alias for constants and regular columns
            if (column.contains(" as ")) {
                columns.add(column); // Keep the entire expression with alias
            } else {
                // Handle simple columns or constants without alias
                String cleanColumn = column.split(" ")[0].trim();
                columns.add(cleanColumn);
            }
        }

        return columns;
    }
    
    private static String transformColumnsToString(Set columns) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String column : columns) {
            joiner.add(column);
        }
        return joiner.toString();
    }
    
    private static String getColumnsForInsertStatement(String columns, Map rowFields) {
        
        String[] columnArray = columns.split(", ");
        
        Set uniqueColumns = new HashSet();
        StringJoiner columnsJoiner = new StringJoiner(", ");
        
        for (String column : columnArray) {
            String cleanColumn;
            
            // Handle aliases properly by extracting the alias if it exists
            if (column.contains(" as ")) {
                cleanColumn = column.split(" as ")[1].trim();
            } else {
                // Remove the prefix (like trx., stm., bnk., etc.) from the column name
                cleanColumn = column.trim().substring(column.indexOf('.') + 1).trim();
            }
            
            if (!"id".equals(cleanColumn) && !cleanColumn.startsWith("c_")) {
                cleanColumn = "c_" + cleanColumn;
            }
            
            if (uniqueColumns.add(cleanColumn)) {
                columnsJoiner.add(cleanColumn);
            }
        }
        
        for (String key : rowFields.keySet()) {
            String cleanKey = key;
            
            if (!"id".equals(cleanKey) && !cleanKey.startsWith("c_")) {
                cleanKey = "c_" + cleanKey;
            }
            
            if (uniqueColumns.add(key)) {
                columnsJoiner.add(key);
            }
        }
        
        return columnsJoiner.toString();
    }

    public static String getColumnsForSQL(Map rowFields, List processors) {
        
        Set allColumns = new HashSet();
        
        for (Object processor : processors) {
            allColumns.addAll(extractColumns(processor.getNotProcessedTransactionsSQL()));
        }
        
        return getColumnsForInsertStatement(transformColumnsToString(allColumns), rowFields);
    }
}

public class DatabaseInserter {

    // Method for inserting multiple rows using list of field values
    public static void insertRows(
        String tableName,
        List columns,
        List rowsData
    ) throws SQLException {
        String sql = createInsertSQL(tableName, columns);

        Connection con = null;
        PreparedStatement stmt = null;

        try {
            con = Utils.getConnection();
            stmt = con.prepareStatement(sql);

            for (List rowData : rowsData) {
                setPreparedStatementParameters(stmt, rowData);
                stmt.addBatch();
            }
            stmt.executeBatch();
        } finally {
            Utils.closeResources(null, stmt, con);
        }
    }

    // Method for inserting a single row using a Map of field values
    public static String insertRow(
        String tableName,
        List columns,
        Map rowFields
    ) throws SQLException {
        String sql = createInsertSQL(tableName, columns);
        Connection con = null;
        PreparedStatement stmt = null;
        String generatedId = null;

        try {
            con = Utils.getConnection();
            stmt = con.prepareStatement(sql);
            setPreparedStatementParameters(stmt, columns, rowFields);
            stmt.executeUpdate();
            generatedId = (String) rowFields.get("id"); // Assuming 'id' is the primary key
        } finally {
            Utils.closeResources(null, stmt, con);
        }
        return generatedId;
    }

    // Overloaded method for inserting a single row using a list of field values
    public static void insertRow(
        String tableName,
        List columns,
        List rowData
    ) throws SQLException {
        String sql = createInsertSQL(tableName, columns);

        Connection con = null;
        PreparedStatement stmt = null;

        try {
            con = Utils.getConnection();
            stmt = con.prepareStatement(sql);
            setPreparedStatementParameters(stmt, rowData);
            stmt.executeUpdate();
        } finally {
            Utils.closeResources(null, stmt, con);
        }
    }

    // Method to create INSERT SQL statement
    private static String createInsertSQL(String tableName, List columns) {
        
        String[] columnArray = listToArray(columns);
        return getInsertSQLString(tableName, columnArray);
    }

    // Method to set parameters using a Map
    private static void setPreparedStatementParameters(PreparedStatement stmt, List columns, Map rowFields) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            String columnName = (String) columns.get(i);
            Object value = rowFields.get(columnName);
            stmt.setObject(i + 1, value);
        }
    }

    // Method to set parameters using a list of values
    private static void setPreparedStatementParameters(PreparedStatement stmt, List rowData) throws SQLException {
        for (int i = 0; i < rowData.size(); i++) {
            stmt.setObject(i + 1, rowData.get(i));
        }
    }

    private static String[] listToArray(List columns) {
        String[] columnArray = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnArray[i] = (String) columns.get(i);
        }
        return columnArray;
    }

    private static String getInsertSQLString(String tableName, String[] columnArray) {
        
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columnArray.length; i++) {
            placeholders.append("?");
            if (i < columnArray.length - 1) {
                placeholders.append(", ");
            }
        }

        // String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders.toString() + ")";
        String sql = "INSERT INTO " + tableName + " (" + String.join(", ", columnArray) + ") VALUES (" + placeholders.toString() + ")";
        
        return sql;
    }    

}




// Business Functions:
//      1) Extraction data from statement file
//      2) saving statement records bankAccountTransaction and securityTransaction
//      3) creation of consolidated records from extracted records
//      4) recognition of transactions
//      5) saving of recognised transaction to form accPost


// 1. Import of statement lines from file to SECU and BANK tables (_account_trx and _total_trx)

class BankStatementRecordDTO {

    private String fileName;
    private String accountType;
    private String bankId;
    private String statementRef;
    private String bankCode;
    private File uploadedFile;

    public BankStatementRecordDTO(
            String fileName,
            String accountType,
            String bankId,
            String statementRef,
            String bankCode,
            File uploadedFile
        ) {
        this.fileName = fileName;
        this.accountType = accountType;
        this.bankId = bankId;
        this.statementRef = statementRef;
        this.bankCode = bankCode;
        this.uploadedFile = uploadedFile;
    }

    // Getters and Setters
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getAccountType() {
        return accountType;
    }

    public void setAccountType(String accountType) {
        this.accountType = accountType;
    }

    public String getBankId() {
        return bankId;
    }

    public void setBankId(String bankId) {
        this.bankId = bankId;
    }

    public String getStatementRef() {
        return statementRef;
    }

    public void setStatementRef(String statementRef) {
        this.statementRef = statementRef;
    }

    public String getBankCode() {
        return bankCode;
    }

    public void setBankCode(String bankCode) {
        this.bankCode = bankCode;
    }

    public File getUploadedFile() {
        return uploadedFile;
    }

    public void setUploadedFile(File uploadedFile) {
        this.uploadedFile = uploadedFile;
    }

    public String toString() {
        return "BankStatementRecordDTO{" +
                "fileName='" + fileName + '\'' +
                ", accountType='" + accountType + '\'' +
                ", bankId='" + bankId + '\'' +
                ", statementRef='" + statementRef + '\'' +
                ", bankCode='" + bankCode +
                " }";
    }
}

class TransactionDTO {
    private Map rowFields;

    // Default constructor
    public TransactionDTO() {
        this.rowFields = new HashMap();
    }

    public TransactionDTO(TransactionDTO template) {
        this.rowFields = new HashMap(template.getRowFields()); // Create a shallow copy of the template's rowFields
    }

    public String getColumnValue(String columnName) {
        return (String) rowFields.getOrDefault(columnName, "");
    }
    public void setColumnValue(String fieldName, String value) {
        rowFields.put(fieldName, value);
    }
    public Map getRowFields() {
        return rowFields;
    }
}

class CustomerDTO {
    private String customerId;     // customer's in table row ID
    private String customerRef;    // customer's business ID (company reg.nr or National ID)

    public CustomerDTO(Object customerId, Object customerRef) {
        this.customerId = customerId;
        this.customerRef = customerRef;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getCustomerRef() {
        return customerRef;
    }

    public String toString() {
        return "CustomerDTO{" +
               "customerId=" + customerId +
               ", customerRef=" + customerRef +
               '}';
    }
}

class StatementLineDTO {
    private String id;
    private String statementReference;
    
    public StatementLineDTO(String id, String statementReference) {
        this.id = id;
        this.statementReference = statementReference;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getStatementReference() {
        return statementReference;
    }

    public void setStatementReference(String statementReference) {
        this.statementReference = statementReference;
    }
}

class SecuAmountsDTO {

    private StatementLineDTO statementLineMain;
    private StatementLineDTO statementLineFee;
    private Map rowFields;
    private double difference;
    private double secuAmount = 0;
    private double secuFeeAmount = 0;

    public SecuAmountsDTO(StatementLineDTO statementLineMain, StatementLineDTO statementLineFee, Map rowFields) {
        this.statementLineMain = statementLineMain;
        this.statementLineFee = statementLineFee;
        this.rowFields = rowFields;
        
        if (this.statementLineMain != null && this.statementLineMain.getId() != null) {
            this.secuAmount = Double.parseDouble(rowFields.get("c_amount"));
        }
        if (this.statementLineFee != null && this.statementLineMain.getId() != null) {
            this.secuFeeAmount = Double.parseDouble(rowFields.get("c_fee"));
        }
    }
    
    public double getMainAmount() {
        return this.secuAmount;
    }

    public double getFeeAmount() {
        return this.secuFeeAmount;
    }

    public SecuAmountsDTO(StatementLineDTO statementLineMain, Map rowFields) {
        this.statementLineMain = statementLineMain;
        this.rowFields = rowFields;
    }

    public StatementLineDTO getStatementLineMain() {
        return statementLineMain;
    }

    public void setStatementLineMain(StatementLineDTO statementLineMain) {
        this.statementLineMain = statementLineMain;
    }

    public StatementLineDTO getStatementLineFee() {
        return statementLineFee;
    }

    public void setStatementLineFee(StatementLineDTO statementLineFee) {
        this.statementLineFee = statementLineFee;
    }

    public Map getRowFields() {
        return rowFields;
    }

    public void setRowFields(Map rowFields) {
        this.rowFields = rowFields;
    }

    public double getDifference() {
        calculateDifference();
        return this.difference;
    }

    private void calculateDifference() {
        
        double secuTotalAmount1 = 0;
        double secuTotalAmount2 = 0;
        
        secuTotalAmount1 = Utils.round(Double.parseDouble(rowFields.get("c_total_amount")), 2);
        secuTotalAmount2 = Utils.round((secuAmount + secuFeeAmount), 2);
        this.difference = Double.compare(secuTotalAmount1, secuTotalAmount2);
    }
}

class InternalTransactionTypeDTO {

    private String trxTypeId;
    private String trxTypeCode;

    // Default constructor
    public InternalTransactionTypeDTO() {
    }

    public InternalTransactionTypeDTO(String trxTypeId, String trxTypeCode) {
        this.trxTypeId = trxTypeId;
        this.trxTypeCode = trxTypeCode;
    }

    public String getTrxTypeId() {
        return trxTypeId;
    }

    public void setTrxTypeId(String trxTypeId) {
        this.trxTypeId = trxTypeId;
    }

    public String getTrxTypeCode() {
        return trxTypeCode;
    }

    public void setTrxTypeCode(String trxTypeCode) {
        this.trxTypeCode = trxTypeCode;
    }

    public String toString() {
        return "InternalTransactionTypeDTO{" +
                "trxTypeId='" + trxTypeId + '\'' +
                ", trxTypeCode='" + trxTypeCode + '\'' +
                '}';
    }
}

public class LedgerOpTypeDTO {
    private String id;
    private String code;
    private String name;
    private String basisTrxType;
    private String includedWords;
    private String excludedWords;

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getBasisTrxType() { return basisTrxType; }
    public void setBasisTrxType(String basisTrxType) { this.basisTrxType = basisTrxType; }

    public String getIncludedWords() { return includedWords; }
    public void setIncludedWords(String includedWords) { this.includedWords = includedWords; }

    public String getExcludedWords() { return excludedWords; }
    public void setExcludedWords(String excludedWords) { this.excludedWords = excludedWords; }
}

public class TransactionRepository {

    private String statementId;

    public TransactionRepository(String statementId) {
        this.statementId = statementId;
    }

    public String getStatementId() {
        return this.statementId;
    }
    
    
    public String getIdByCode(String tableName, String code) {
        String sql = "SELECT id FROM app_fd_" + tableName + " WHERE c_code = ?";
        String[] params = {code};
        return Utils.getAttr(sql, params, "id");
    }

    public String getStatementReference(String id) {
        String sql = "SELECT c_reference FROM app_fd_bank_statement WHERE id = ?";
        String[] params = {id};
        return Utils.getAttr(sql, params, "c_reference");
    }

    public String getAssetTypeIdByTrxType(String id) {
        String sql = "SELECT * FROM app_fd_trx_type WHERE id = ?";
        String[] params = {id};
        return Utils.getAttr(sql, params, "c_asset_type");
    }

    public String getBankCode(String id) {
        String sql = "SELECT * FROM app_fd_bank WHERE id = ?";
        String[] params = {id};
        return Utils.getAttr(sql, params, "c_swift_code_bic");
    }


    private InternalTransactionTypeDTO executeQueryForTransactionType(String sql, String[] params) {
        String[] returnAttrs = {"id", "c_code"};
        InternalTransactionTypeDTO trxTypeDTO = null;
        Map rs = Utils.getAttributes(sql, params, returnAttrs);

        if (!rs.isEmpty()) {
            trxTypeDTO = new InternalTransactionTypeDTO((String) rs.get("id"), (String) rs.get("c_code"));
        }

        return trxTypeDTO;
    }

    public InternalTransactionTypeDTO getInternalTransactionType(String statementType, String flowType, String assetType) {
        String sql = "select id, c_code from app_fd_trx_type where c_statement_type = ? and c_flow_type = ? and c_asset_type = ?";
        String[] params = {statementType, flowType, assetType};
        return executeQueryForTransactionType(sql, params);
    }

    public InternalTransactionTypeDTO getInternalTransactionType(String statementType, String flowType, String assetType, String isWithCustomerId) {
        String sql = "select id, c_code from app_fd_trx_type where c_statement_type = ? and c_flow_type = ? and c_asset_type = ? and c_is_customer = ?";
        String[] params = {statementType, flowType, assetType, isWithCustomerId};
        return executeQueryForTransactionType(sql, params);
    }

    public InternalTransactionTypeDTO getInternalTransactionType(String code) {
        String sql = "select id, c_code from app_fd_trx_type where c_code = ?";
        String[] params = {code};
        return executeQueryForTransactionType(sql, params);
    }


    public StatementLineDTO getBankTransaction(String bicCode, String paymentAmount, String currency) {

        String sql = "SELECT trx.id, trx.c_statement_reference " +
                     "FROM app_fd_bank bnk " +
                     "JOIN app_fd_bank_statement stm ON (bnk.id = stm.c_bank) " +
                     "JOIN app_fd_bank_total_trx trx ON (stm.id = trx.c_statement_id) " +
                     "WHERE trx.c_acc_post_id IS NULL " +
                     "AND trx.c_payment_amount = ? " +
                     "AND trx.c_currency = ? " +
                     "AND bnk.c_swift_code_bic = ?";

        String[] params = {paymentAmount, currency, bicCode};
        String[] returnAttrs = {"id", "c_statement_reference"};
        
        StatementLineDTO statementLine = null;
        Map rs = Utils.getAttributes(sql, params, returnAttrs);
        if (!rs.isEmpty()) {
            statementLine = new StatementLineDTO(rs.get("id"), rs.get("c_statement_reference"));
        }
        return statementLine;
    }
    
    public CustomerDTO getCustomerByBankRef(String customerReference) {
        String sql;
        String[] params = {customerReference};
        String[] returnAttrs = {"id", "c_customer_ref"};
    
        if (customerReference.length() == 8) {
            sql = "SELECT id, c_customer_ref FROM app_fd_customer WHERE c_registration_number = ?";  // legal entity
        } else {
            sql = "SELECT id, c_customer_ref FROM app_fd_customer WHERE c_national_id = ?";  // individual
        }
    
        Map rs = Utils.getAttributes(sql, params, returnAttrs);
        CustomerDTO cInfo = null;
    
        if (!rs.isEmpty()) {
            cInfo = new CustomerDTO((String) rs.get("id"), (String) rs.get("c_customer_ref"));
        }
    
        return cInfo;
    }    

    public CustomerDTO getCustomerByAccount(String bankAccount, String customerName) {
        String sql = "SELECT id, c_customer_ref FROM app_fd_customer WHERE c_org_name = ? AND c_account_number = ?";
        String[] params = {customerName, bankAccount};
        String[] returnAttrs = {"id", "c_customer_ref"};

        Map rs = Utils.getAttributes(sql, params, returnAttrs);
        CustomerDTO cInfo = null;
    
        if (!rs.isEmpty()) {
            cInfo = new CustomerDTO((String) rs.get("id"), (String) rs.get("c_customer_ref"));
        }
    
        return cInfo;
    }
    
    public CustomerDTO getCustomerByIndBusinessName(String bankAccount, String customerName) {
        String sql = "SELECT id, c_customer_ref FROM app_fd_customer WHERE c_ind_business_name = ? AND c_account_number = ?";
        String[] params = {customerName, bankAccount};
        String[] returnAttrs = {"id", "c_customer_ref"};

        Map rs = Utils.getAttributes(sql, params, returnAttrs);
        CustomerDTO cInfo = null;
    
        if (!rs.isEmpty()) {
            cInfo = new CustomerDTO((String) rs.get("id"), (String) rs.get("c_customer_ref"));
        }
    
        return cInfo;
    }
    
    // TODO: not yet properly cleaned
    public void cleanPostings(String accountType) {
        String sql = "DELETE FROM app_fd_acc_post WHERE c_statement_id = ? and c_account_type = ?";
        String[] params = {this.statementId, accountType};
        Utils.executeUpdate(sql, params);
    }

    public void cleanTableByStatementId(String tableName) {
        String sql = "DELETE FROM " + tableName + " WHERE c_statement_id = ?";
        String[] params = {this.statementId};
        Utils.executeUpdate(sql, params);
    }

    public List getLedgerOpTypeConfigurations() {
        List ledgerOpTypes = new ArrayList();
    
        String sql = "SELECT id, c_code, c_name, c_basis_trx_type, c_included_words, c_excluded_words FROM app_fd_ledger_op_type";

        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
    
        try {
            con = Utils.getConnection();
            stmt = con.prepareStatement(sql);
            rs = stmt.executeQuery();
    
            while (rs != null && rs.next()) {
                LedgerOpTypeDTO ledgerOpType = new LedgerOpTypeDTO();
                ledgerOpType.setId(rs.getString("id"));
                ledgerOpType.setCode(rs.getString("c_code"));
                ledgerOpType.setName(rs.getString("c_name"));
                ledgerOpType.setBasisTrxType(rs.getString("c_basis_trx_type"));
                ledgerOpType.setIncludedWords(rs.getString("c_included_words"));
                ledgerOpType.setExcludedWords(rs.getString("c_excluded_words"));
    
                ledgerOpTypes.add(ledgerOpType);
            }
        } catch (SQLException e) {
            LogUtil.error("getLedgerOpTypeConfigurations", e, "Error retrieving ledger operation types");
        } finally {
            Utils.closeResources(rs, stmt, con);
        }
    
        return ledgerOpTypes;
    }

}


// 1.1 Get the file

public class FileHandler {

    private String fileRowId;
    private BankStatementRecordDTO stmRecord;
    private TransactionRepository trxRepo;

    private File uploadedFile;

    public FileHandler(String fileRowId) {
        this.fileRowId = fileRowId;
        this.trxRepo = new TransactionRepository(fileRowId);
        this.stmRecord = retrieveUploadedFileRecord(fileRowId);
        this.uploadedFile = this.stmRecord.getUploadedFile();
    }

    public getFile() {
        return this.uploadedFile;
    }

    public getStatementRecord() {
        return this.stmRecord;
    }

    public getFileRowId() {
        return this.fileRowId;
    }
    
    private BankStatementRecordDTO retrieveUploadedFileRecord(String fileRowId) {
        String sql = "SELECT c_statement_file, c_account_type, c_bank, c_reference FROM app_fd_bank_statement WHERE id = ?";
        String[] params = {fileRowId};
        String[] returnAttrs = {"c_statement_file", "c_account_type", "c_bank", "c_reference"};
    
        Map rs = Utils.getAttributes(sql, params, returnAttrs);
        BankStatementRecordDTO recordDTO = null;
    
        if (!rs.isEmpty()) {
            String fileName = (String) rs.get("c_statement_file");
            String accountType = (String) rs.get("c_account_type");
            String bankId = (String) rs.get("c_bank");
            String statementRef = (String) rs.get("c_reference");
            String bankCode = trxRepo.getBankCode(bankId);
            File uploadedFile = FileUtil.getFile(fileName, "bank_statement", fileRowId);
            
            recordDTO = new BankStatementRecordDTO(fileName, accountType, bankId, statementRef, bankCode, uploadedFile);
        }
        return recordDTO;
    }    
}


// 1.2 Extract data from the file

public class StatementImportHelper {

    private List fields = new ArrayList();  // Output list containing processed CSV fields

    public void extractAndProcessRowsFromFile(File uploadedFile) {
        try {
            CSVFormat csvFormat = determineCsvFormat(uploadedFile);
            processCsvFile(uploadedFile, csvFormat);
        } catch (IOException e) {
            LogUtil.error("StatementImportHelper", e, "Error processing CSV file.");
        }
    }

    private CSVFormat determineCsvFormat(File uploadedFile) throws IOException {
        BufferedReader reader = null;
        String headerLine = null;
        try {
            reader = new BufferedReader(new FileReader(uploadedFile));
            headerLine = reader.readLine();
        } finally {
            Utils.close(reader);
        }

        if (headerLine != null) {
            // Remove quotes and convert to lowercase for easier matching
            String headerLineUnquoted = headerLine.replaceAll("\"", "").replaceAll("'", "").toLowerCase();

            if (headerLine.indexOf(';') >= 0) {
                // Swedbank format
                if (headerLineUnquoted.indexOf("kliendi konto") >= 0 &&
                    headerLineUnquoted.indexOf("dok nr") >= 0 &&
                    headerLineUnquoted.indexOf("makse kuupäev") >= 0) {

                    return CSVFormat.RFC4180
                            .withDelimiter(';')
                            .withFirstRecordAsHeader()
                            .withIgnoreEmptyLines()
                            .withTrim()
                            .withQuote('"')
                            .withQuoteMode(QuoteMode.MINIMAL);
                }
            } else if (headerLine.indexOf(',') >= 0) {
                // Secu format
                if (headerLineUnquoted.indexOf("väärtuspäev") >= 0 &&
                    headerLineUnquoted.indexOf("tehingupäev") >= 0 &&
                    headerLineUnquoted.indexOf("tehing") >= 0) {

                    return CSVFormat.RFC4180
                            .withDelimiter(',')
                            .withFirstRecordAsHeader()
                            .withIgnoreEmptyLines()
                            .withTrim()
                            .withEscape('\\')
                            .withQuote('"')
                            .withQuoteMode(QuoteMode.MINIMAL);
                }

                // LHV Bank format
                if (headerLineUnquoted.indexOf("kliendi konto") >= 0 &&
                    headerLineUnquoted.indexOf("dokumendi number") >= 0 &&
                    headerLineUnquoted.indexOf("kuupäev") >= 0) {

                    return CSVFormat.RFC4180
                            .withDelimiter(',')
                            .withFirstRecordAsHeader()
                            .withIgnoreEmptyLines()
                            .withTrim()
                            .withEscape('\\')
                            .withQuote('"')
                            .withQuoteMode(QuoteMode.MINIMAL);
                }
            }
        }

        // Default format
        return CSVFormat.RFC4180
                .withFirstRecordAsHeader()
                .withIgnoreEmptyLines()
                .withTrim();
    }

    private void processCsvFile(File uploadedFile, CSVFormat csvFormat) throws IOException {
        FileReader reader = null;
        CSVParser csvParser = null;
        try {
            reader = new FileReader(uploadedFile);
            csvParser = new CSVParser(reader, csvFormat);

            Iterator csvIterator = csvParser.iterator();
            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = (CSVRecord) csvIterator.next();
                fields.add(csvRecordToArray(csvRecord));
            }
        } catch (IOException e) {
            LogUtil.error("StatementImportHelper", e, "Error processing CSV file with format: " + csvFormat.toString());
            throw e; // Re-throw exception to be caught in the calling method
        } finally {
            Utils.close(csvParser);
            Utils.close(reader);
        }
    }

    private String[] csvRecordToArray(CSVRecord csvRecord) {
        int size = csvRecord.size();
        String[] array = new String[size];
        for (int i = 0; i < size; i++) {
            array[i] = csvRecord.get(i);
        }
        return array;
    }

    public List getFields() {
        return fields;
    }
}



// 1.3 Store the file to Secu or Bank accounts tables

public class FieldMapping {
    private int csvIndex;
    private String sqlColumnName;

    public FieldMapping(int csvIndex, String sqlColumnName) {
        this.csvIndex = csvIndex;
        this.sqlColumnName = sqlColumnName;
    }

    public int getCsvIndex() {
        return csvIndex;
    }

    public String getSqlColumnName() {
        return sqlColumnName;
    }
}

public class Formatter {

    public static String formatTransactionId(int transactionId) {
        String idString = String.valueOf(transactionId);
        while (idString.length() < 3) {
            idString = "0" + idString;
        }
        return idString;
    }
}

public class DataMappingConfig {

    private String tableName;  // Name of the target database table
    private List fieldMappings; // List of FieldMapping objects

    public DataMappingConfig(String tableName, List fieldMappings) {
        this.tableName = tableName;
        this.fieldMappings = fieldMappings;
    }

    public String getTableName() {
        return tableName;
    }

    public List getFieldMappings() {
        return fieldMappings;
    }

    public String prepareInsertSQL() {
        
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");

        // Adding SQL columns based on field mappings
        for (int i = 0; i < fieldMappings.size(); i++) {
            FieldMapping mapping = (FieldMapping) fieldMappings.get(i);
            sql.append(mapping.getSqlColumnName());
            if (i < fieldMappings.size() - 1) {
                sql.append(", ");
            }
        }

        // Add placeholders for the manually added fields
        sql.append(", id, c_transaction_id, c_statement_id)");

        sql.append(" VALUES (");
        for (int i = 0; i < fieldMappings.size(); i++) {
            sql.append("?");
            if (i < fieldMappings.size() - 1) {
                sql.append(", ");
            }
        }

        // Add placeholders for the manually added fields
        sql.append(", ?, ?, ?)");

        return sql.toString();
    }
}

public class DataMappingConfigurations {

    public static final DataMappingConfig SECU_CONFIG = new DataMappingConfig(
        "app_fd_sec_account_trx",
        Arrays.asList(new FieldMapping[]{
            new FieldMapping(0, "c_value_date"),
            new FieldMapping(1, "c_transaction_date"),
            new FieldMapping(2, "c_type"),
            new FieldMapping(3, "c_ticker"),
            new FieldMapping(4, "c_description"),
            new FieldMapping(5, "c_quantity"),
            new FieldMapping(6, "c_price"),
            new FieldMapping(7, "c_currency"),
            new FieldMapping(8, "c_amount"),
            new FieldMapping(9, "c_fee"),
            new FieldMapping(10, "c_total_amount"),
            new FieldMapping(11, "c_reference"),
            new FieldMapping(12, "c_comment")
        })
    );

    public static final DataMappingConfig LHV_BANK_CONFIG = new DataMappingConfig(
        "app_fd_bank_account_trx",
        Arrays.asList(new FieldMapping[]{
            new FieldMapping(0, "c_account_number"),
            new FieldMapping(1, "c_document_nr"),
            new FieldMapping(2, "c_payment_date"),
            new FieldMapping(3, "c_other_side_account"),
            new FieldMapping(4, "c_other_side_name"),
            new FieldMapping(5, "c_other_side_bank"),
            new FieldMapping(6, "c_d_c"),
            new FieldMapping(7, "c_payment_amount"),
            new FieldMapping(8, "c_reference_number"),
            new FieldMapping(9, "c_archival_number"),
            new FieldMapping(10, "c_payment_description"),
            new FieldMapping(11, "c_transaction_fee"),
            new FieldMapping(12, "c_currency"),
            new FieldMapping(13, "c_customer_id"),
            new FieldMapping(14, "c_other_side_bic"),
            new FieldMapping(15, "c_initiator"),
            new FieldMapping(16, "c_transaction_reference"),
            new FieldMapping(17, "c_provider_reference")
        })
    );

    public static final DataMappingConfig SWED_BANK_CONFIG = new DataMappingConfig(
        "app_fd_bank_account_trx",
        Arrays.asList(new FieldMapping[]{
            new FieldMapping(0, "c_account_number"),
            new FieldMapping(1, "c_document_nr"),
            new FieldMapping(2, "c_payment_date"),
            new FieldMapping(3, "c_other_side_account"),
            new FieldMapping(4, "c_other_side_name"),
            new FieldMapping(5, "c_other_side_bank"),
            new FieldMapping(6, "c_d_c"),
            new FieldMapping(7, "c_payment_amount"),
            new FieldMapping(8, "c_reference_number"),
            new FieldMapping(9, "c_archival_number"),
            new FieldMapping(10, "c_payment_description"),
            new FieldMapping(11, "c_transaction_fee"),
            new FieldMapping(12, "c_currency"),
            new FieldMapping(13, "c_customer_id")
        })
    );
}

public class DataProcessor {

    private List fields;  // Data extracted from CSV files to be saved

    public DataProcessor(List fields) {
        this.fields = fields;
    }

    public void saveData(String accountType, String fileRowId, DataMappingConfig config) {
        List columns = new ArrayList();
        List rowsData = new ArrayList();

        // Get columns from field mappings
        List fieldMappings = config.getFieldMappings();
        for (int i = 0; i < fieldMappings.size(); i++) {
            FieldMapping mapping = (FieldMapping) fieldMappings.get(i);
            columns.add(mapping.getSqlColumnName());
        }
        // Add additional columns
        columns.add("id");
        columns.add("c_transaction_id");
        columns.add("c_statement_id");

        int seqId = 0;

        for (int rowIndex = 0; rowIndex < fields.size(); rowIndex++) {
            String[] row = (String[]) fields.get(rowIndex);

            if ("bank".equals(accountType)) {
                row = Utils.removeElementFromArray(row, 6); // 6th field from the bank row is not used
            }

            seqId++;
            String pId = UUID.randomUUID().toString();
            String formattedSeqId = Formatter.formatTransactionId(seqId);

            List rowData = new ArrayList();

            // Add data from CSV row
            for (int i = 0; i < fieldMappings.size(); i++) {
                FieldMapping mapping = (FieldMapping) fieldMappings.get(i);
                int csvIndex = mapping.getCsvIndex();

                if (csvIndex >= 0 && csvIndex < row.length) {
                    rowData.add(row[csvIndex]);
                } else {
                    rowData.add(null); // Handle missing or invalid indices
                }
            }

            // Add additional fields per row
            rowData.add(pId);              // id
            rowData.add(formattedSeqId);   // c_transaction_id
            rowData.add(fileRowId);        // c_statement_id

            rowsData.add(rowData);
        }

        // Insert data using the shared utility
        try {
            DatabaseInserter.insertRows(config.getTableName(), columns, rowsData);
        } catch (SQLException e) {
            LogUtil.error("DataProcessor", e, "Error inserting data");
        }
    }
    
    
}




// 1.4 Create consolidated transactions

public class ConsolidationProcessor {

    private String fileRowId;
    private TransactionRepository trxRepo;

    public ConsolidationProcessor(TransactionRepository trxRepo) {
        this.trxRepo = trxRepo;
        this.fileRowId = trxRepo.getStatementId();
    }

    public void process(
        String fileRowId,
        DataMappingConfig config,
        String summaryTable,
        String groupByClause,
        String[] summaryExpressions,
        String[] summaryAliases,
        String[] insertColumns
    ) {
        List sumRows = new ArrayList();
        String sql = createSummarySQL(config.getTableName(), summaryExpressions, groupByClause);
        String[] params = {fileRowId};
        int rowLength = summaryAliases.length;

        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            con = Utils.getConnection();
            stmt = con.prepareStatement(sql);
            rs = Utils.executeQuery(stmt, params);

            while (rs != null && rs.next()) {
                String[] rowFields = new String[rowLength];

                for (int i = 0; i < rowLength; i++) {
                    rowFields[i] = String.valueOf(rs.getObject(summaryAliases[i]));
                }

                sumRows.add(rowFields);

                // Batch insert rows to prevent memory overflow on large datasets
                if (sumRows.size() % 1000 == 0) {
                    insertSummaryRows(fileRowId, sumRows, summaryTable, insertColumns);
                    sumRows.clear();
                }
            }

            // Insert remaining rows
            if (!sumRows.isEmpty()) {
                insertSummaryRows(fileRowId, sumRows, summaryTable, insertColumns);
            }
        } finally {
            Utils.closeResources(rs, stmt, con);
        }
    }

    private static String createSummarySQL(String tableName, String[] summaryColumns, String groupByClause) {
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < summaryColumns.length; i++) {
            sql.append(summaryColumns[i]);
            if (i < summaryColumns.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(" FROM ").append(tableName).append(" WHERE c_statement_id = ? GROUP BY ").append(groupByClause);
        return sql.toString();
    }

    private static String createInsertSQL(String tableName, String[] insertColumns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        for (int i = 0; i < insertColumns.length; i++) {
            sql.append(insertColumns[i]);
            if (i < insertColumns.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(", id, c_transaction_id, c_statement_id, c_status, c_statement_reference) VALUES (");
        for (int i = 0; i < insertColumns.length + 5; i++) {
            sql.append("?");
            if (i < insertColumns.length + 4) {
                sql.append(", ");
            }
        }
        sql.append(")");
        return sql.toString();
    }
    
    private void insertSummaryRows(String fileRowId, List sumRows, String summaryTable, String[] insertColumns) {
        List columns = new ArrayList(Arrays.asList(insertColumns));
        columns.add("id");
        columns.add("c_transaction_id");
        columns.add("c_statement_id");
        columns.add("c_status");
        columns.add("c_statement_reference");

        List rowsData = new ArrayList();
        int seqId = 0;
        String statementReference = trxRepo.getStatementReference(fileRowId);

        for (Object rowObj : sumRows) {
            String[] row = (String[]) rowObj;
            seqId++;
            String pId = UUID.randomUUID().toString();
            String formattedSeqId = Formatter.formatTransactionId(seqId);

            List rowData = new ArrayList();

            // Add data from summary row
            for (String value : row) {
                rowData.add(value);
            }

            // Add additional fields per row
            rowData.add(pId);                             // id
            rowData.add(String.valueOf(seqId));           // c_transaction_id
            rowData.add(fileRowId);                       // c_statement_id
            rowData.add("new");                           // c_status
            rowData.add(statementReference + "." + formattedSeqId); // c_statement_reference

            rowsData.add(rowData);
        }

        // Insert data using the shared utility
        try {
            DatabaseInserter.insertRows(summaryTable, columns, rowsData);
        } catch (SQLException e) {
            LogUtil.error("ConsolidationProcessor", e, "Error during insertion into " + summaryTable);
        }
    }



}



// 1.5 Orchestrate Extraction, Saving and Consolidation

public class StatementImportProcessor {

    private FileHandler fileHandler;
    private TransactionRepository trxRepo;
    private StatementImportHelper helper;
    private BankStatementRecordDTO recordDTO;
    private String fileRowId;
    private File uploadedFile;
    private String bankCode;
    private String accountType;
    private List fields;

    public StatementImportProcessor(FileHandler fileHandler, TransactionRepository trxRepo, StatementImportHelper helper) {
        this.fileHandler = fileHandler;
        this.trxRepo = trxRepo;
        this.helper = helper;
        this.recordDTO = fileHandler.getStatementRecord();
        this.fileRowId = fileHandler.getFileRowId();
        this.uploadedFile = this.recordDTO.getUploadedFile();
        this.bankCode = this.recordDTO.getBankCode();
        this.accountType = recordDTO.getAccountType();
        
        helper.extractAndProcessRowsFromFile(uploadedFile);
        this.fields = helper.getFields();
    }

    public void process() {
        deletePreviouslyDispatchedRows();
        storeFields();
        makeConsolidatedRows();
    }

    private void deletePreviouslyDispatchedRows() {
        Map tableMap = new HashMap();
        tableMap.put("bank", new String[]{"app_fd_bank_account_trx", "app_fd_bank_total_trx"});
        tableMap.put("secu", new String[]{"app_fd_sec_account_trx", "app_fd_secu_total_trx"});

        String[] tables = (String[]) tableMap.get(accountType);
        if (tables != null) {
            for (int i = 0; i < tables.length; i++) {
                trxRepo.cleanTableByStatementId(tables[i]);
            }
        }

        trxRepo.cleanPostings(accountType);
    }
    
    private void storeFields() {
        Map configMap = new HashMap();
        configMap.put("secu_LHVBEE22", DataMappingConfigurations.SECU_CONFIG);
        configMap.put("bank_LHVBEE22", DataMappingConfigurations.LHV_BANK_CONFIG);
        configMap.put("bank_HABAEE2X", DataMappingConfigurations.SWED_BANK_CONFIG);
        
        String key = accountType + "_" + bankCode;
        DataMappingConfig config = (DataMappingConfig) configMap.get(key);
        if (config != null) {
            DataProcessor dataProcessor = new DataProcessor(fields);
            dataProcessor.saveData(accountType, fileRowId, config);
        }
    }
    
    private void makeConsolidatedRows() {
        if ("secu".equals(accountType)) {
            processConsolidation(
                DataMappingConfigurations.SECU_CONFIG,
                "app_fd_secu_total_trx",
                "c_value_date, c_transaction_date, c_type, c_ticker, c_description, c_currency",
                new String[]{
                    "c_value_date", "c_transaction_date", "c_type", "c_ticker", "c_description",
                    "c_currency", "SUM(c_quantity) as quantity", "ROUND(AVG(c_price), 2) as price",
                    "ROUND(SUM(c_amount), 2) as amount", "ROUND(SUM(c_fee), 2) as fee", "ROUND(SUM(c_total_amount), 2) as total_amount"
                },
                new String[]{
                    "c_value_date", "c_transaction_date", "c_type", "c_ticker", "c_description",
                    "c_currency", "quantity", "price", "amount", "fee", "total_amount"
                },
                new String[]{
                    "c_value_date", "c_transaction_date", "c_type", "c_ticker", "c_description",
                    "c_currency", "c_quantity", "c_price", "c_amount", "c_fee", "c_total_amount"
                }
            );
        } else if ("bank".equals(accountType) && "LHVBEE22".equals(bankCode)) {
            processConsolidation(
                DataMappingConfigurations.LHV_BANK_CONFIG,
                "app_fd_bank_total_trx",
                "c_account_number, c_document_nr, c_payment_date, c_other_side_account, c_other_side_name, c_other_side_bank, c_d_c, c_payment_description, c_currency, c_customer_id, c_other_side_bic",
                new String[]{
                    "c_account_number", "c_document_nr", "c_payment_date", "c_other_side_account",
                    "c_other_side_name", "c_other_side_bank", "c_d_c", "c_payment_description",
                    "c_currency", "c_customer_id", "c_other_side_bic",
                    "ROUND(SUM(c_payment_amount), 2) as payment_amount", "ROUND(SUM(c_transaction_fee), 2) as transaction_fee"
                },
                new String[]{
                    "c_account_number", "c_document_nr", "c_payment_date", "c_other_side_account",
                    "c_other_side_name", "c_other_side_bank", "c_d_c", "c_payment_description",
                    "c_currency", "c_customer_id", "c_other_side_bic", "payment_amount", "transaction_fee"
                },
                new String[]{
                    "c_account_number", "c_document_nr", "c_payment_date", "c_other_side_account",
                    "c_other_side_name", "c_other_side_bank", "c_d_c", "c_payment_description",
                    "c_currency", "c_customer_id", "c_other_side_bic", "c_payment_amount", "c_transaction_fee"
                }
            );
        }
    }

    private void processConsolidation(
        DataMappingConfig config,
        String tableName,
        String groupByColumns,
        String[] summaryExpressions,
        String[] summaryAliases,
        String[] insertColumns
    ) throws SQLException {
        ConsolidationProcessor summaryProcessor = new ConsolidationProcessor(trxRepo);
        summaryProcessor.process(
            fileRowId,
            config,
            tableName,
            groupByColumns,
            summaryExpressions,
            summaryAliases,
            insertColumns
        );
    }
}



// 2. transaction Type Recognition


interface StatementRowProcessor {
    void process(Map rowFields) throws SQLException;
    String getNotProcessedTransactionsSQL();
    List getColumnNames();
}


// 2.1 Every implementation of the StatementRowProcessor should have 
// as a configuration SQL query string to get not-processed operations

class SecuProcessor implements StatementRowProcessor {

    private TransactionRepository trxRepo;
    private String accountType;
    
    private List columnNames;
    private double difference;
    private SecuAmountsDTO secuAmountsDTO;
    private InternalTransactionTypeDTO trxTypeDTO;
    private TransactionRegistrationService trxRegService;
    
    public SecuProcessor() {
    }

    public SecuProcessor(TransactionRepository trxRepo) {
        this.trxRepo = trxRepo;
        this.accountType = "secu";
        
        this.columnNames = Utils.extractColumnNamesFromSQL(getNotProcessedTransactionsSQL());
        this.trxRegService = new TransactionRegistrationService(trxRepo);
    }

    public String getNotProcessedTransactionsSQL() {
        return 
                "SELECT trx.id c_secu_trx_id, bnk.c_swift_code_bic, trx.c_statement_reference as c_trx_reference, " +
                "      stm.c_account_type, trx.c_amount, trx.c_currency, trx.c_type, trx.c_ticker as c_asset_code, " +
                "      trx.c_fee, trx.c_description as c_asset, trx.c_transaction_date, " +
                "      trx.c_quantity, trx.c_total_amount, trx.c_price, trx.c_value_date " +
                " FROM app_fd_bank bnk " +
                " JOIN app_fd_bank_statement stm on (bnk.id = stm.c_bank) " +
                " JOIN app_fd_secu_total_trx trx on (stm.id = trx.c_statement_id) " +
                "WHERE trx.c_acc_post_id is null " +
                "  AND trx.c_type NOT LIKE 'split%'";
    }

    public List getColumnNames() {
        return this.columnNames;
    }

    public void process(Map rowFields) throws SQLException {
        
        String trx_reference = rowFields.get("c_trx_reference");
        secuAmountsDTO = null;
        String accPostId = null;
        
        System.out.println("1.1 IN SecuProcessor.--------------------------------------------------");
        
        // 1. Find appropriate purchase or sell bank payment records (main + fee amounts)
        StatementLineDTO statementLineMain = trxRepo.getBankTransaction(
                    rowFields.get("c_swift_code_bic"),
                    rowFields.get("c_amount"),
                    rowFields.get("c_currency") );
                    
        System.out.println("1.2 IN SecuProcessor.");
        System.out.println("bic: " + rowFields.get("c_swift_code_bic") + " amt: " + rowFields.get("c_amount") + " cur: " + rowFields.get("c_currency"));
        
        if (statementLineMain != null) {
            
            System.out.println("1.3 IN SecuProcessor.");
            
            StatementLineDTO statementLineFee = trxRepo.getBankTransaction(
                        rowFields.get("c_swift_code_bic"),
                        rowFields.get("c_fee"),
                        rowFields.get("c_currency") );
            
            if (statementLineFee != null) {
                secuAmountsDTO = new SecuAmountsDTO(statementLineMain, statementLineFee, rowFields);
                rowFields.put(FEE_REF_FIELD_NAME, statementLineFee.getStatementReference());
            } else {
                secuAmountsDTO = new SecuAmountsDTO(statementLineMain, rowFields);
            }
            
            difference = secuAmountsDTO.getDifference();
            
            System.out.println("2. IN SecuProcessor: am: " + rowFields.get("c_amount") + " fee: " + rowFields.get("c_fee") + " tm: "+ rowFields.get("c_total_amount"));
            System.out.println(" difference: " + String.valueOf(difference));

            if (difference == 0) {
                
                secuTotalAmountInBank = Utils.round(secuAmountsDTO.getMainAmount() + secuAmountsDTO.getFeeAmount(), 2);
                
                if (secuAmountsDTO.getMainAmount() > 0) {
                    trxTypeDTO = trxRepo.getInternalTransactionType(accountType, "in", "SCR01");
                } else {
                    trxTypeDTO = trxRepo.getInternalTransactionType(accountType, "out", "SCR01");
                }

                String secu_trx_id = rowFields.get("c_secu_trx_id");
                String FK_NAME = "c_secu_total_trx";
                
                rowFields.put(FK_NAME, statementLineMain.getId());
                rowFields.put(BANK_REF_FIELD_NAME, statementLineMain.getStatementReference());
                rowFields.put(BANK_TOTAL_FIELD_NAME, secuTotalAmountInBank);
                
                

    
                // 2. Create Posting record
                // 3. Make Cross-References in initial secu transactions
                
                if (statementLineFee != null && statementLineFee.getId() != null) {
                    
                    System.out.println("2.1 IN SecuProcessor");
                    
                    trxRegService.registerSecuTransaction(trxTypeDTO, rowFields, secu_trx_id, statementLineMain, statementLineFee);
                    
                    System.out.println("2.2 IN SecuProcessor");

                } else {
                    
                    System.out.println("2.3 IN SecuProcessor");
                    
                    trxRegService.registerSecuTransaction(trxTypeDTO, rowFields, secu_trx_id, statementLineMain);
                    
                    System.out.println("2.4 IN SecuProcessor");
                    

                }
                
                System.out.println("3. IN SecuProcessor.--------------------------------------------------");
                
            } else {
                return;
            }
        } else {
            return;
        }
    }
}

class SplitProcessor implements StatementRowProcessor {

    private TransactionRepository trxRepo;
    private List columnNames;
    private InternalTransactionTypeDTO trxTypeDTO;
    private TransactionRegistrationService trxRegService;
    
    public SplitProcessor() {
        
    }
    
    public SplitProcessor(TransactionRepository trxRepo) {
        this.trxRepo = trxRepo;
        this.trxTypeDTO = trxRepo.getInternalTransactionType("SL");

        this.columnNames = Utils.extractColumnNamesFromSQL(getNotProcessedTransactionsSQL());
        this.trxRegService = new TransactionRegistrationService(trxRepo);
    }

    public String getNotProcessedTransactionsSQL() {
        return  
                "SELECT trx.id, bnk.c_swift_code_bic, trx.c_statement_reference as c_trx_reference, " +
                "      stm.c_account_type, trx1.c_statement_reference as c_trx_reference_new, " +
                "      trx.c_amount, trx.c_currency, trx.c_type, trx.c_ticker as c_asset_code, " +
                "      trx.c_fee, trx.c_description as c_asset, trx.c_transaction_date, " +
                "      trx.c_quantity, trx.c_total_amount, trx.c_price, trx.c_value_date, " +
                "      trx1.id as c_id_new, trx1.c_quantity as c_quantity_new, trx1.c_quantity as c_quantity_new, " +
                "      trx1.c_price as c_price_new " +
                " FROM app_fd_bank bnk " +
                " JOIN app_fd_bank_statement stm on (bnk.id = stm.c_bank) " +
                " JOIN app_fd_secu_total_trx trx on (stm.id = trx.c_statement_id) " +
                " JOIN app_fd_secu_total_trx trx1 on ( " +
                "          trx.c_transaction_date = trx1.c_transaction_date AND " +
                "          trx.c_ticker = trx1.c_ticker AND " +
                "          trx.c_description = trx1.c_description AND " +
                "          trx.c_currency = trx1.c_currency) " +
                "WHERE trx.c_acc_post_id is null " +
                "  AND trx1.c_acc_post_id is null " +
                "  AND trx.c_type = 'split-' " +
                "  AND trx1.c_type = 'split+' " +
                "  AND trx.c_quantity < 0 " +
                "  AND trx1.c_quantity > 0 ";
    }

    public List getColumnNames() {
        return this.columnNames;
    }

    public void process(Map rowFields) throws SQLException {

        String trx1_id = rowFields.get("id");
        String trx2_id = rowFields.get("c_id_new");
        
        String FK_NAME = "c_secu_total_trx";
        rowFields.put(FK_NAME, trx1_id);
        trxRegService.registerSplitTransaction(trxTypeDTO, rowFields, trx1_id, trx2_id);
    }
}

class BankOpsProcessor implements StatementRowProcessor {
    
    private TransactionRepository trxRepo;
    private List columnNames;
    private CustomerDTO cInfo;
    private InternalTransactionTypeDTO trxTypeDTO;
    private TransactionRegistrationService trxRegService;
    
    public BankOpsProcessor() {
        
    }

    public BankOpsProcessor(TransactionRepository trxRepo) {
        this.trxRepo = trxRepo;
        this.columnNames = Utils.extractColumnNamesFromSQL(getNotProcessedTransactionsSQL());
        this.trxRegService = new TransactionRegistrationService(trxRepo);
    }
    
    public String getNotProcessedTransactionsSQL() {
        return  
                "SELECT trx.id, bnk.c_swift_code_bic, trx.c_statement_reference as c_trx_reference, " +
                "       trx.c_payment_amount c_amount, trx.c_currency, 'xin' as c_type, TRX.c_currency c_asset_code, " +
                "       trx.c_transaction_fee c_fee, trx.c_payment_description c_asset, " +
                "       trx.c_payment_date c_transaction_date, trx.c_other_side_account, " +
                "       trx.c_d_c, trx.c_customer_id, trx.c_other_side_bic, trx.c_other_side_name c_name " +
                "  FROM app_fd_bank bnk " +
                "  JOIN app_fd_bank_statement stm on (bnk.id = stm.c_bank) " +
                "  JOIN app_fd_bank_total_trx trx on (stm.id = trx.c_statement_id) " +
                "  JOIN app_fd_currency cur on (cur.c_code = trx.c_currency) " +
                " WHERE trx.c_acc_post_id is null " +
                "   AND trx.c_payment_amount > 0 " +
                " UNION ALL " +
                "SELECT trx.id, bnk.c_swift_code_bic, trx.c_statement_reference as c_trx_reference, " +
                "       trx.c_payment_amount c_amount, trx.c_currency, 'xout' as c_type, TRX.c_currency c_asset_code, " +
                "       trx.c_transaction_fee c_fee, trx.c_payment_description c_asset, " +
                "       trx.c_payment_date c_transaction_date, trx.c_other_side_account, " +
                "       trx.c_d_c, trx.c_customer_id, trx.c_other_side_bic, trx.c_other_side_name c_name " +
                "  FROM app_fd_bank bnk " +
                "  JOIN app_fd_bank_statement stm on (bnk.id = stm.c_bank) " +
                "  JOIN app_fd_bank_total_trx trx on (stm.id = trx.c_statement_id) " +
                "  JOIN app_fd_currency cur on (cur.c_code = trx.c_currency) " +
                " WHERE trx.c_acc_post_id is null " +
                "   AND trx.c_payment_amount < 0 ";
    }
    
    public List getColumnNames() {
        return this.columnNames;
    }

    public void process(Map rowFields) throws SQLException {

        double amount = Double.parseDouble(rowFields.get("c_amount"));
        String customer_id = rowFields.get("c_customer_id");
        String customer_account = rowFields.get("c_other_side_account");
        String customer_name = rowFields.get("c_name");
        String FK_NAME = "c_bank_total_trx";
        rowFields.put(FK_NAME, rowFields.get("id"));
        rowFields.put(ACCOUNT_TYPE_FIELD_NAME, "bank");
        cInfo = null;
        trxTypeDTO = null;
        
        if (customer_id != null && (customer_id.length() == 8 || customer_id.length() == 11)) {
            
            cInfo = this.trxRepo.getCustomerByBankRef(customer_id);
            
        } else if (customer_account != null && customer_account.length() > 5) {

            cInfo = this.trxRepo.getCustomerByAccount(customer_account, customer_name);
            
            if (cInfo == null) {
                cInfo = this.trxRepo.getCustomerByIndBusinessName(customer_account, customer_name);
            }
        }

        if (cInfo != null) {

            if (amount > 0) {
                trxTypeDTO = trxRepo.getInternalTransactionType("bank", "in", "CSH01", "yes");
            } else {
                trxTypeDTO = trxRepo.getInternalTransactionType("bank", "out", "CSH01", "yes");
            }
            
            rowFields.put(CUSTOMER_ID_FIELD_NAME, cInfo.getCustomerId());
            rowFields.put(CUSTOMER_REF_FIELD_NAME, cInfo.getCustomerRef());

            String trxId = trxTypeDTO.getTrxTypeId();
            String trxCode = trxTypeDTO.getTrxTypeCode();
        }
        
        if (trxTypeDTO != null) {
            trxRegService.registerBankTransaction(trxTypeDTO, rowFields, rowFields.get("id"));
        }
    }
}


// 2.2 Processing not-processed operations from all registered StatementRowProcessors

public class TransactionRegistrationService {
    private TransactionRepository trxRepo;
    private List ledgerOpTypes;

    public TransactionRegistrationService(TransactionRepository trxRepo) {
        this.trxRepo = trxRepo;
        this.ledgerOpTypes = trxRepo.getLedgerOpTypeConfigurations();
    }

    private void updateSecuTransaction(String trxTypeCode, String acc_post_id, String secu_trx_id, String bank_payment_trx_id, String bank_fee_trx_id) {
        
        if (acc_post_id == null) {
            throw new Exception("1.ERROR acc_post_id is null, secu_trx_id: " + secu_trx_id + " trxTypeCode: " + trxTypeCode);
        }
        
        String sql = "UPDATE app_fd_secu_total_trx SET c_acc_post_id = ?, " +
                     "c_bank_payment_trx_id = ?, c_bank_fee_trx_id = ?, c_transaction_type = ?, c_status = 'posted' WHERE id = ?";

        String[] params = {acc_post_id, bank_payment_trx_id, bank_fee_trx_id, trxTypeCode, secu_trx_id};
        Utils.executeUpdate(sql, params);

        if (bank_fee_trx_id != null) {
            updateBankTransaction(trxTypeCode, acc_post_id, bank_payment_trx_id, bank_fee_trx_id);
        } else {
            updateBankTransaction(trxTypeCode, acc_post_id, bank_payment_trx_id, null);
        }
    }

    private void updateSecuTransaction(String trxTypeCode, String acc_post_id, String secu_trx_id, String bank_payment_trx_id) {
        
        if (acc_post_id == null) {
            throw new Exception("2.ERROR acc_post_id is null, secu_trx_id: " + secu_trx_id + " trxTypeCode: " + trxTypeCode);
        }
        
        updateSecuTransaction(trxTypeCode, acc_post_id, secu_trx_id, bank_payment_trx_id, null);
    }

    private void updateSecuTransaction(String trxTypeCode, String acc_post_id, String secu_trx_id) {
        
        if (acc_post_id == null) {
            throw new Exception("3.ERROR acc_post_id is null, secu_trx_id: " + secu_trx_id + " trxTypeCode: " + trxTypeCode);
        }
        
        String sql = "UPDATE app_fd_secu_total_trx SET c_acc_post_id = ?, c_transaction_type = ?, c_status = 'posted' WHERE id = ?";
        Utils.executeUpdate(sql, new String[]{acc_post_id, trxTypeCode, secu_trx_id});
    }

    private void updateBankTransaction(String trxTypeCode, String acc_post_id, String bank_payment_trx_id, String bank_fee_trx_id) {
        
        if (acc_post_id == null) {
            throw new Exception("4.ERROR acc_post_id is null, bank_trx_id: " + bank_trx_id + " trxTypeCode: " + trxTypeCode);
        }
        
        String feeSql = "UPDATE app_fd_bank_total_trx SET c_acc_post_id = ?, c_main_bank_total_trx = ?, c_type = 'secufee', c_transaction_type = ?, c_status = 'posted' WHERE id = ?";
        String paymentSql = "UPDATE app_fd_bank_total_trx SET c_acc_post_id = ?, c_type = ?, c_transaction_type = ?, c_status = 'posted' WHERE id = ?";

        if (bank_fee_trx_id != null) {
            Utils.executeUpdate(feeSql, new String[]{acc_post_id, bank_payment_trx_id, trxTypeCode, bank_fee_trx_id});
        }

        Utils.executeUpdate(paymentSql, new String[]{acc_post_id, "secupmt", trxTypeCode, bank_payment_trx_id});
    }

    private void updateBankTransaction(String trxTypeCode, String acc_post_id, String bank_trx_id) {
        
        if (acc_post_id == null) {
            throw new Exception("5.ERROR acc_post_id is null, bank_trx_id: " + bank_trx_id + " trxTypeCode: " + trxTypeCode);
        }
        
        String sql = "UPDATE app_fd_bank_total_trx SET c_acc_post_id = ?, c_transaction_type = ?, c_status = 'posted' WHERE id = ?";

        Utils.executeUpdate(sql, new String[]{acc_post_id, trxTypeCode, bank_trx_id});
    }
    

    public void registerSecuTransaction(InternalTransactionTypeDTO trxTypeDTO, Map rowFields, String secu_trx_id, StatementLineDTO statementLineMain, StatementLineDTO statementLineFee) throws SQLException {
        String accPostId = savePosting(trxTypeDTO.getTrxTypeCode(), rowFields, statementLineMain.getId());
        updateSecuTransaction(trxTypeDTO.getTrxTypeCode(), accPostId, secu_trx_id, statementLineMain.getId(), statementLineFee.getId());
    }

    public void registerSecuTransaction(InternalTransactionTypeDTO trxTypeDTO, Map rowFields, String secu_trx_id, StatementLineDTO statementLineMain) throws SQLException {
        String accPostId = savePosting(trxTypeDTO.getTrxTypeCode(), rowFields, statementLineMain.getId());
        updateSecuTransaction(trxTypeDTO.getTrxTypeCode(), accPostId, secu_trx_id, statementLineMain.getId());
    }

    public void registerSplitTransaction(InternalTransactionTypeDTO trxTypeDTO, Map rowFields, String trx1_id, String trx2_id) throws SQLException {
        String accPostId = savePosting(trxTypeDTO.getTrxTypeCode(), rowFields, trx1_id);
        updateSecuTransaction(trxTypeDTO.getTrxTypeCode(), accPostId, trx1_id);
        updateSecuTransaction(trxTypeDTO.getTrxTypeCode(), accPostId, trx2_id);
    }

    public void registerBankTransaction(InternalTransactionTypeDTO trxTypeDTO, Map rowFields, String bank_trx_id) throws SQLException {
        String accPostId = savePosting(trxTypeDTO.getTrxTypeCode(), rowFields, bank_trx_id);
        updateBankTransaction(trxTypeDTO.getTrxTypeCode(), accPostId, bank_trx_id);
    }

    private String savePosting(String trxTypeCode, Map rowFields, String transactionId) throws SQLException {
        
        // Prepare additional fields
        String generatedId = UUID.randomUUID().toString();
        rowFields.put("id", generatedId);
        rowFields.put("dateCreated", Utils.getCurrentDateAsString());
        rowFields.put("c_acc_post_date", Utils.getCurrentDateAsString());
        rowFields.put("c_transaction_type", trxTypeCode);
        rowFields.put("c_statement_id", trxRepo.getStatementId());
    
        // Logic for matching ledger operation types
        String assetValue = (String) rowFields.get("c_asset");  // description is here
        if (assetValue != null && assetValue.length() > 2) {
            String transactionType = (String) rowFields.get("c_transaction_type");
            LedgerOpTypeDTO matchingOpType = null;
    
            for (Object ledgerOpTypeObj : ledgerOpTypes) {
                LedgerOpTypeDTO ledgerOpType = (LedgerOpTypeDTO) ledgerOpTypeObj;
    
                if (transactionType != null && transactionType.equals(ledgerOpType.getBasisTrxType())) {
                    if (matchesIncludedWords(ledgerOpType.getIncludedWords(), assetValue) && 
                        !matchesExcludedWords(ledgerOpType.getExcludedWords(), assetValue)) {
                        matchingOpType = ledgerOpType;
                        break;
                    }
                }
            }
    
            if (matchingOpType != null) {
                rowFields.put("c_type", matchingOpType.getCode());
            }
        }
    
        List processors = Arrays.asList(new Object[]{new SecuProcessor(), new SplitProcessor(), new BankOpsProcessor()});
        String columnsStr = SqlUtils.getColumnsForSQL(rowFields, processors);
        List columns = Arrays.asList(columnsStr.split(",\\s*"));
    
        DatabaseInserter.insertRow("app_fd_acc_post", columns, rowFields);
    
        return generatedId;
    }

    private boolean matchesIncludedWords(String includedWords, String assetValue) {
    if (includedWords == null || includedWords.isEmpty()) {
        return true; // If no included words are specified, skip this check
    }

    String lowerAssetValue = assetValue.toLowerCase(); // Convert asset value to lowercase for case-insensitive comparison
    String[] words = includedWords.split(","); // Assuming words are comma-separated
    for (int i = 0; i < words.length; i++) {
        if (lowerAssetValue.contains(words[i].trim().toLowerCase())) {
            return true;
        }
    }
    return false;
}

    private boolean matchesExcludedWords(String excludedWords, String assetValue) {
    if (excludedWords == null || excludedWords.isEmpty()) {
        return false; // If no excluded words are specified, skip this check
    }

    String lowerAssetValue = assetValue.toLowerCase(); // Convert asset value to lowercase for case-insensitive comparison
    String[] words = excludedWords.split(","); // Assuming words are comma-separated
    for (int i = 0; i < words.length; i++) {
        if (lowerAssetValue.contains(words[i].trim().toLowerCase())) {
            return true;
        }
    }
    return false;
}    
    
}

public class TransactionRecognitionProcessor {

    private String statementId;
    private TransactionRepository trxRepo;
    private StatementRowProcessor stmRowProcessor;

    public TransactionRecognitionProcessor(String statementId, StatementRowProcessor stmRowProcessor) {
        this.statementId = statementId;
        this.trxRepo = new TransactionRepository(statementId);
        this.stmRowProcessor = stmRowProcessor;
    }

    public void process() {
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            con = Utils.getConnection();
            String sql = stmRowProcessor.getNotProcessedTransactionsSQL();
            stmt = con.prepareStatement(sql);
            rs = stmt.executeQuery();
            List columnNames = stmRowProcessor.getColumnNames();
            TransactionDTO transactionTemplate = createTransactionTemplate(columnNames);
            
            while (rs.next()) {
                TransactionDTO transaction = new TransactionDTO(transactionTemplate);
                for (int i = 0; i < columnNames.size(); i++) {
                    String columnName = (String) columnNames.get(i);
                    transaction.setColumnValue(columnName, rs.getString(columnName));
                }
                stmRowProcessor.process(transaction.getRowFields());
            }
        } finally {
            Utils.closeResources(rs, stmt, con);
        }
    }
    
    private TransactionDTO createTransactionTemplate(List columnNames) {
        TransactionDTO template = new TransactionDTO();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = (String) columnNames.get(i);
            template.setColumnValue(columnName, null);          // Set initial values to null
        }
        return template;
    }
}


// 3. Main entrance

private static final String CURRENT_FORM_ROW_ID = "#form.bank_statement.id#";

class StatementProcessor {

    private FileHandler fh;
    private StatementImportHelper stmImportHelper;
    private BankStatementRecordDTO recordDTO;
    private String accountType;
    
    private StatementImportProcessor statementProcessor;
    private String currRowId;
    

    public StatementProcessor(String currRowId) {
        this.fh = new FileHandler(currRowId);
        this.trxRepo = new TransactionRepository(fh.getFileRowId());
        this.stmImportHelper = new StatementImportHelper();
        
        this.statementImportProcessor = new StatementImportProcessor(fh, trxRepo, stmImportHelper);
        
        this.recordDTO = fh.getStatementRecord();
        this.accountType = recordDTO.getAccountType();
        this.currRowId = currRowId;
    }

    public void process() {

        System.out.println("1. Start import of statement with accountType: " + accountType);
        
        statementImportProcessor.process();
        
        if (Objects.equals(accountType, "secu")) {
            
            // 1. All Secu operations except Split
            TransactionRecognitionProcessor trxProcessor1 = new TransactionRecognitionProcessor(currRowId, new SecuProcessor(trxRepo));
            trxProcessor1.process();
            
            // 2. All Split operations
            TransactionRecognitionProcessor trxProcessor2 = new TransactionRecognitionProcessor(currRowId, new SplitProcessor(trxRepo));
            trxProcessor2.process();

        } else if (Objects.equals(accountType, "bank")) {
            
            // 3. All Bank operations
            TransactionRecognitionProcessor trxProcessor3 = new TransactionRecognitionProcessor(currRowId, new BankOpsProcessor(trxRepo));
            trxProcessor3.process();
        }

        System.out.println("2. Done. CURRENT_FORM_ROW_ID: " + CURRENT_FORM_ROW_ID);
    }
    public void processOtherStatements() {
        System.out.println("1. Seach for statement with unprocessed transactions");
    }
}

StatementProcessor stmProcessor = new StatementProcessor(CURRENT_FORM_ROW_ID);
stmProcessor.process();


