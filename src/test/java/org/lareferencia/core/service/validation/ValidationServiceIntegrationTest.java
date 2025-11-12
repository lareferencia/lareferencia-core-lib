package org.lareferencia.core.service.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.domain.ValidatorRule;
import org.lareferencia.core.service.validation.ValidationService;
import org.lareferencia.core.worker.validation.validator.RegexFieldContentValidatorRule;
import org.lareferencia.core.worker.validation.*;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ValidationService Integration Tests")
class ValidationServiceIntegrationTest {

    private ValidationService validationService;
    private RuleSerializer ruleSerializer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        validationService = new ValidationService();
        ruleSerializer = new RuleSerializer();
        objectMapper = new ObjectMapper();
        
        // Inject the serializer using reflection (simulating @Autowired)
        ReflectionTestUtils.setField(validationService, "serializer", ruleSerializer);
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
        
        // Create a real validator rule and serialize it properly
        RegexFieldContentValidatorRule regexRule = new RegexFieldContentValidatorRule();
        regexRule.setFieldname("dc:title");
        regexRule.setRegexString(".*");
        
        String json = objectMapper.writeValueAsString(regexRule);
        rule.setJsonserialization(json);
        
        validatorModel.getRules().add(rule);

        IValidator validator = validationService.createValidatorFromModel(validatorModel);

        assertNotNull(validator);
        assertEquals(1, validator.getRules().size());
    }
}
