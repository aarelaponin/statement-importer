package com.fiscaladmin.gam.statementimporter.mapping;

import com.fiscaladmin.gam.statementimporter.parser.Format;

import java.util.Arrays;

/**
 * Static mapping configurations for all supported CSV formats.
 * <p>
 * Migrated from the BeanShell {@code DataMappingConfigurations} in {@code stm-processing.java}.
 * The authoritative reference for column mappings is {@code preprocessing-architecture.md} §6.4.
 */
public class MappingConfigurations {

    /**
     * Securities CSV → sec_account_trx (13 fields).
     * <p>
     * Source: BeanShell SECU_CONFIG
     */
    public static final MappingConfig SECU_CONFIG = new MappingConfig(
        "app_fd_sec_account_trx",
        Arrays.asList(
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
        )
    );

    /**
     * LHV Bank CSV → bank_account_trx (18 fields, after stripping empty index 6).
     * <p>
     * Source: BeanShell LHV_BANK_CONFIG
     * <p>
     * <b>IMPORTANT:</b> The CSV indices here are the <i>after-stripping</i> indices.
     * The raw CSV has 19 columns but field index 6 (empty column between bank name
     * and D/C) is stripped by {@code StatementParser} before mapping. So these
     * indices operate on the 18-element cleaned array.
     */
    public static final MappingConfig LHV_BANK_CONFIG = new MappingConfig(
        "app_fd_bank_account_trx",
        Arrays.asList(
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
        )
    );

    /**
     * Swedbank CSV → bank_account_trx (14 fields).
     * <p>
     * Source: BeanShell SWED_BANK_CONFIG.
     * Separator: semicolon (;).
     */
    public static final MappingConfig SWED_BANK_CONFIG = new MappingConfig(
        "app_fd_bank_account_trx",
        Arrays.asList(
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
        )
    );

    /**
     * Returns the mapping config for a given Format.
     *
     * @param format the detected CSV format
     * @return the corresponding MappingConfig
     * @throws IllegalArgumentException if no mapping config exists for the format
     */
    public static MappingConfig getConfig(Format format) {
        switch (format) {
            case LHV_BANK:    return LHV_BANK_CONFIG;
            case SWEDBANK:    return SWED_BANK_CONFIG;
            case SECURITIES:  return SECU_CONFIG;
            default:
                throw new IllegalArgumentException("No mapping config for format: " + format);
        }
    }

    private MappingConfigurations() {
        // utility class — no instantiation
    }
}
