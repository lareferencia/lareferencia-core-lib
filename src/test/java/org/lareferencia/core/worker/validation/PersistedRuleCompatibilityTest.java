package org.lareferencia.core.worker.validation;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.util.List;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.Transformer;
import org.lareferencia.core.domain.TransformerRule;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.domain.ValidatorRule;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.repository.catalog.OAIRecord;
import org.lareferencia.core.service.validation.ValidationService;
import org.springframework.test.util.ReflectionTestUtils;

class PersistedRuleCompatibilityTest {
    @Test void executesAllHistoricalCoreTypesWithoutRewritingTheirRows() throws Exception {
        var fixtures = new ObjectMapper().readTree(getClass().getResource("/legacy-validation-rules.json"));
        var config = new Configuration().addAnnotatedClass(Validator.class).addAnnotatedClass(ValidatorRule.class)
                .addAnnotatedClass(Transformer.class).addAnnotatedClass(TransformerRule.class)
                .setProperty("hibernate.connection.url", "jdbc:h2:mem:historicalrules")
                .setProperty("hibernate.hbm2ddl.auto", "create-drop");
        try (var factory = config.buildSessionFactory(); var em = factory.createEntityManager()) {
            em.getTransaction().begin();
            var validator = new Validator();
            validator.setName("historical");
            for (var definition : fixtures.get("validator")) {
                var rule = new ValidatorRule();
                rule.setJsonserialization(definition.toPrettyString());
                rule.setName(definition.get("@class").asText());
                rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
                validator.getRules().add(rule);
            }
            var transformer = new Transformer();
            transformer.setName("historical");
            for (var definition : fixtures.get("transformer")) {
                var rule = new TransformerRule();
                rule.setName(definition.get("@class").asText());
                rule.setJsonserialization(definition.toPrettyString());
                rule.setRunorder(transformer.getRules().size() + 1);
                transformer.getRules().add(rule);
            }
            em.persist(validator);
            em.persist(transformer);
            em.getTransaction().commit();
            em.clear();

            em.getTransaction().begin();
            var service = new ValidationService();
            ReflectionTestUtils.setField(service, "serializer", new RuleSerializer());
            var validation = service.createValidatorFromModel(em.find(Validator.class, validator.getId()));
            var transformation = service.createTransformerFromModel(em.find(Transformer.class, transformer.getId()));
            assertEquals(20, transformation.getRules().size());
            assertEquals(transformer.getRules().stream().map(TransformerRule::getId).toList(),
                    transformation.getRules().stream().map(ITransformerRule::getRuleId).toList());
            var xml = new OAIRecordMetadata("id");
            xml.addFieldOcurrence("dc.type", "article");
            var snapshot = new SnapshotMetadata(1L);
            snapshot.setNetwork(new Network());
            var record = OAIRecord.create("id", LocalDateTime.of(2020, 1, 1, 0, 0), "abc", false);
            assertTrue(transformation.transform(snapshot, record, xml));
            assertEquals(List.of("org.lareferencia.backend.validation.validator.RegexFieldContentValidatorRule"),
                    xml.getFieldOcurrences("dc.note"));
            assertEquals(9, validation.validate(xml, new ValidatorResult()).getRulesResults().size());
            em.getTransaction().commit();
            em.clear();
            for (var row : validator.getRules()) {
                assertEquals(row.getJsonserialization(), em.find(ValidatorRule.class, row.getId()).getJsonserialization());
            }
            for (var row : transformer.getRules()) {
                assertEquals(row.getJsonserialization(), em.find(TransformerRule.class, row.getId()).getJsonserialization());
            }
        }
    }
}
