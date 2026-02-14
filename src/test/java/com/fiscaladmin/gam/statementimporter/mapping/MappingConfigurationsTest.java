package com.fiscaladmin.gam.statementimporter.mapping;

import com.fiscaladmin.gam.statementimporter.parser.Format;
import org.junit.Test;

import static org.junit.Assert.*;

public class MappingConfigurationsTest {

    @Test
    public void lhvBankConfigHas18Fields() {
        assertEquals(18, MappingConfigurations.LHV_BANK_CONFIG.getFieldMappings().size());
    }

    @Test
    public void secuConfigHas13Fields() {
        assertEquals(13, MappingConfigurations.SECU_CONFIG.getFieldMappings().size());
    }

    @Test
    public void swedBankConfigHas14Fields() {
        assertEquals(14, MappingConfigurations.SWED_BANK_CONFIG.getFieldMappings().size());
    }

    @Test
    public void lhvBankFirstFieldIsAccountNumber() {
        FieldMapping first = MappingConfigurations.LHV_BANK_CONFIG.getFieldMappings().get(0);
        assertEquals(0, first.getCsvIndex());
        assertEquals("c_account_number", first.getDbColumnName());
    }

    @Test
    public void lhvBankLastFieldIsProviderReference() {
        FieldMapping last = MappingConfigurations.LHV_BANK_CONFIG.getFieldMappings().get(17);
        assertEquals(17, last.getCsvIndex());
        assertEquals("c_provider_reference", last.getDbColumnName());
    }

    @Test
    public void secuFirstFieldIsValueDate() {
        FieldMapping first = MappingConfigurations.SECU_CONFIG.getFieldMappings().get(0);
        assertEquals(0, first.getCsvIndex());
        assertEquals("c_value_date", first.getDbColumnName());
    }

    @Test
    public void secuLastFieldIsComment() {
        FieldMapping last = MappingConfigurations.SECU_CONFIG.getFieldMappings().get(12);
        assertEquals(12, last.getCsvIndex());
        assertEquals("c_comment", last.getDbColumnName());
    }

    @Test
    public void swedBankFirstFieldIsAccountNumber() {
        FieldMapping first = MappingConfigurations.SWED_BANK_CONFIG.getFieldMappings().get(0);
        assertEquals(0, first.getCsvIndex());
        assertEquals("c_account_number", first.getDbColumnName());
    }

    @Test
    public void swedBankLastFieldIsCustomerId() {
        FieldMapping last = MappingConfigurations.SWED_BANK_CONFIG.getFieldMappings().get(13);
        assertEquals(13, last.getCsvIndex());
        assertEquals("c_customer_id", last.getDbColumnName());
    }

    @Test
    public void getConfigReturnsCorrectMapping() {
        assertSame(MappingConfigurations.LHV_BANK_CONFIG, MappingConfigurations.getConfig(Format.LHV_BANK));
        assertSame(MappingConfigurations.SWED_BANK_CONFIG, MappingConfigurations.getConfig(Format.SWEDBANK));
        assertSame(MappingConfigurations.SECU_CONFIG, MappingConfigurations.getConfig(Format.SECURITIES));
    }

    @Test
    public void lhvBankTargetTableIsCorrect() {
        assertEquals("app_fd_bank_account_trx", MappingConfigurations.LHV_BANK_CONFIG.getTargetTable());
    }

    @Test
    public void secuTargetTableIsCorrect() {
        assertEquals("app_fd_sec_account_trx", MappingConfigurations.SECU_CONFIG.getTargetTable());
    }

    @Test
    public void swedBankTargetTableIsCorrect() {
        assertEquals("app_fd_bank_account_trx", MappingConfigurations.SWED_BANK_CONFIG.getTargetTable());
    }

    @Test
    public void csvIndicesAreSequential() {
        // Verify all configs have sequential indices starting from 0
        for (Format format : Format.values()) {
            MappingConfig config = MappingConfigurations.getConfig(format);
            for (int i = 0; i < config.getFieldMappings().size(); i++) {
                assertEquals("Index mismatch in " + format + " at position " + i,
                    i, config.getFieldMappings().get(i).getCsvIndex());
            }
        }
    }

    // TODO: T2.3 — Add more specific index→column pair assertions
}
