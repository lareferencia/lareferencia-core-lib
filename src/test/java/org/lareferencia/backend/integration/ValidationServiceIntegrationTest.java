package org.lareferencia.backend.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.backend.domain.Validator;
import org.lareferencia.backend.domain.ValidatorRule;
import org.lareferencia.backend.services.ValidationService;
import org.lareferencia.core.validation.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ValidationService Integration Tests")
class ValidationServiceIntegrationTest {

    private ValidationService validationService;

    @BeforeEach
    void setUp() {
        validationService = new ValidationService();
    }

    @Test
    @DisplayName("Should create validator from model")
    void testCreateValidatorFromModel() throws Exception {
        Validator validatorModel = new Validator();
        validatorModel.setName("Test Validator");
        
        ValidatorRule rule = new ValidatorRule();
        rule.setId(1L);
        rule.setName("Title Required");
        rule.setMandatory(true);
        rule.setQuantifier(QuantifierValues.ONE_ONLY);
        rule.setJsonserialization("{\"@type\":\"MandatoryFieldContentValidationRule\",\"ruleId\":1,\"mandatory\":true,\"quantifier\":\"ONE_ONLY\",\"fieldname\":\"dc:title\"}");
        
        validatorModel.getRules().add(rule);

        IValidator validator = validationService.createValidatorFromModel(validatorModel);

        assertNotNull(validator);
        assertEquals(1, validator.getRules().size());
    }
}
