package org.lareferencia.core.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.domain.ValidatorRule;
import org.lareferencia.core.worker.validation.QuantifierValues;

class ValidatorFingerprintServiceTest {

    private final ValidatorFingerprintService service = new ValidatorFingerprintService();

    @Test
    void ignoresJsonWhitespaceObjectOrderAndRuleCollectionOrder() {
        Validator first = validator(rule(2L, true, "{ \"kind\": \"field\", \"paths\": [\"dc:title\"] }"),
                rule(1L, false, "{\"options\": {\"b\":2,\"a\":1},\"kind\":\"content\"}"));
        Validator second = validator(rule(1L, false, "{\"kind\":\"content\",\"options\":{\"a\":1,\"b\":2}}"),
                rule(2L, true, "{\"paths\":[\"dc:title\"],\"kind\":\"field\"}"));

        assertEquals(service.fingerprint(first).getHash(), service.fingerprint(second).getHash());
    }

    @Test
    void changesWhenEffectiveRuleSettingsChangeAndHasStableNoValidatorValue() {
        Validator baseline = validator(rule(1L, false, "{\"kind\":\"field\"}"));
        Validator mandatoryChanged = validator(rule(1L, true, "{\"kind\":\"field\"}"));

        assertNotEquals(service.fingerprint(baseline).getHash(), service.fingerprint(mandatoryChanged).getHash());
        assertEquals(service.fingerprint(null).getHash(), service.fingerprint(null).getHash());
    }

    private Validator validator(ValidatorRule... rules) {
        Validator validator = new Validator();
        for (ValidatorRule rule : rules) {
            validator.getRules().add(rule);
        }
        return validator;
    }

    private ValidatorRule rule(Long id, boolean mandatory, String json) {
        ValidatorRule rule = new ValidatorRule();
        rule.setId(id);
        rule.setMandatory(mandatory);
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setJsonserialization(json);
        return rule;
    }
}
