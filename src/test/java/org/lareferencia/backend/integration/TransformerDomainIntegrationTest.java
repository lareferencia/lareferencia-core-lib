/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.backend.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.backend.domain.Transformer;
import org.lareferencia.backend.domain.TransformerRule;
import org.lareferencia.backend.validation.transformer.FieldContentTranslateRule;
import org.lareferencia.backend.validation.transformer.RegexTranslateRule;
import org.lareferencia.backend.validation.transformer.FieldNameTranslateRule;
import org.lareferencia.core.validation.Translation;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Transformer Domain to Implementation Integration Tests")
class TransformerDomainIntegrationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Should create Transformer with TransformerRule and serialize/deserialize FieldContentTranslateRule")
    void testTransformerWithFieldContentRule() throws Exception {
        // Create domain entities
        Transformer transformer = new Transformer();
        transformer.setName("Language Normalizer");
        transformer.setDescription("Normalizes language codes");

        TransformerRule rule = new TransformerRule();
        rule.setName("ES to SPA");
        rule.setDescription("Translate ES to SPA");

        // Create implementation rule
        FieldContentTranslateRule translateRule = new FieldContentTranslateRule();
        translateRule.setTestFieldName("dc.language");
        translateRule.setWriteFieldName("dc.language");
        List<Translation> translations = new ArrayList<>();
        translations.add(new Translation("es", "spa"));
        translations.add(new Translation("en", "eng"));
        translateRule.setTranslationArray(translations);

        // Serialize implementation to JSON
        String json = objectMapper.writeValueAsString(translateRule);
        rule.setJsonserialization(json);

        // Add rule to transformer
        transformer.getRules().add(rule);

        // Verify transformer structure
        assertEquals("Language Normalizer", transformer.getName());
        assertEquals(1, transformer.getRules().size());
        assertEquals("ES to SPA", transformer.getRules().get(0).getName());

        // Verify JSON structure
        assertTrue(json.contains("@class"), "JSON should contain @class");

        // Deserialize back to concrete type
        FieldContentTranslateRule deserializedRule = objectMapper.readValue(
            transformer.getRules().get(0).getJsonserialization(),
            FieldContentTranslateRule.class
        );

        assertNotNull(deserializedRule);
        assertEquals("dc.language", deserializedRule.getTestFieldName());
        assertEquals("dc.language", deserializedRule.getWriteFieldName());
        assertEquals("spa", deserializedRule.getTranslationMap().get("es"));
    }

    @Test
    @DisplayName("Should handle multiple TransformerRules in single Transformer")
    void testTransformerWithMultipleRules() throws Exception {
        Transformer transformer = new Transformer();
        transformer.setName("Metadata Normalizer");

        // Rule 1: Field Content Translation
        TransformerRule rule1 = new TransformerRule();
        rule1.setName("Language Translation");
        
        FieldContentTranslateRule contentRule = new FieldContentTranslateRule();
        contentRule.setTestFieldName("dc.language");
        contentRule.setWriteFieldName("dc.language");
        List<Translation> contentTrans = new ArrayList<>();
        contentTrans.add(new Translation("en", "eng"));
        contentRule.setTranslationArray(contentTrans);
        rule1.setJsonserialization(objectMapper.writeValueAsString(contentRule));

        // Rule 2: Regex Translation
        TransformerRule rule2 = new TransformerRule();
        rule2.setName("URL Protocol Fix");
        
        RegexTranslateRule regexRule = new RegexTranslateRule();
        regexRule.setSourceFieldName("dc.identifier.uri");
        regexRule.setTargetFieldName("dc.identifier.uri");
        regexRule.setRegexSearch("^http://");
        regexRule.setRegexReplace("https://");
        rule2.setJsonserialization(objectMapper.writeValueAsString(regexRule));

        // Rule 3: Field Name Translation
        TransformerRule rule3 = new TransformerRule();
        rule3.setName("Field Rename");
        
        FieldNameTranslateRule nameRule = new FieldNameTranslateRule();
        nameRule.setSourceFieldName("dc.creator");
        nameRule.setTargetFieldName("dc.contributor.author");
        rule3.setJsonserialization(objectMapper.writeValueAsString(nameRule));

        transformer.getRules().add(rule1);
        transformer.getRules().add(rule2);
        transformer.getRules().add(rule3);

        // Verify
        assertEquals(3, transformer.getRules().size());
        
        // Deserialize each rule to concrete type
        FieldContentTranslateRule restored1 = objectMapper.readValue(
            transformer.getRules().get(0).getJsonserialization(),
            FieldContentTranslateRule.class
        );
        assertNotNull(restored1);
        
        RegexTranslateRule restored2 = objectMapper.readValue(
            transformer.getRules().get(1).getJsonserialization(),
            RegexTranslateRule.class
        );
        assertNotNull(restored2);
        
        FieldNameTranslateRule restored3 = objectMapper.readValue(
            transformer.getRules().get(2).getJsonserialization(),
            FieldNameTranslateRule.class
        );
        assertNotNull(restored3);
    }

    @Test
    @DisplayName("Should preserve rule order in Transformer")
    void testRuleOrder() throws Exception {
        Transformer transformer = new Transformer();
        
        for (int i = 0; i < 5; i++) {
            TransformerRule rule = new TransformerRule();
            rule.setName("Rule " + i);
            
            FieldContentTranslateRule translateRule = new FieldContentTranslateRule();
            translateRule.setTestFieldName("field" + i);
            translateRule.setWriteFieldName("field" + i);
            List<Translation> trans = new ArrayList<>();
            trans.add(new Translation("value" + i, "newvalue" + i));
            translateRule.setTranslationArray(trans);
            
            rule.setJsonserialization(objectMapper.writeValueAsString(translateRule));
            transformer.getRules().add(rule);
        }

        assertEquals(5, transformer.getRules().size());
        for (int i = 0; i < 5; i++) {
            assertEquals("Rule " + i, transformer.getRules().get(i).getName());
            
            FieldContentTranslateRule restored = objectMapper.readValue(
                transformer.getRules().get(i).getJsonserialization(),
                FieldContentTranslateRule.class
            );
            assertEquals("field" + i, restored.getTestFieldName());
            assertEquals("newvalue" + i, restored.getTranslationMap().get("value" + i));
        }
    }

    @Test
    @DisplayName("Should handle resetId() for Transformer and all rules")
    void testResetId() {
        Transformer transformer = new Transformer();
        transformer.setName("Test Transformer");
        
        // Simulate persisted entities with IDs
        transformer.setId(100L);
        
        TransformerRule rule1 = new TransformerRule();
        rule1.setId(1L);
        rule1.setName("Rule 1");
        
        TransformerRule rule2 = new TransformerRule();
        rule2.setId(2L);
        rule2.setName("Rule 2");
        
        transformer.getRules().add(rule1);
        transformer.getRules().add(rule2);

        // Reset IDs
        transformer.resetId();

        // Verify all IDs are null
        assertNull(transformer.getId());
        assertNull(transformer.getRules().get(0).getId());
        assertNull(transformer.getRules().get(1).getId());
    }

    @Test
    @DisplayName("Should handle empty rules list in Transformer")
    void testEmptyRulesList() {
        Transformer transformer = new Transformer();
        transformer.setName("Empty Transformer");
        
        assertNotNull(transformer.getRules());
        assertEquals(0, transformer.getRules().size());
    }

    @Test
    @DisplayName("Should handle TransformerRule with null jsonserialization")
    void testNullJsonSerialization() {
        TransformerRule rule = new TransformerRule();
        rule.setName("Incomplete Rule");
        
        assertNull(rule.getJsonserialization());
    }

    @Test
    @DisplayName("Should serialize and deserialize complex RegexTranslateRule")
    void testRegexTranslateRuleSerialization() throws Exception {
        TransformerRule rule = new TransformerRule();
        rule.setName("DOI Normalizer");
        
        RegexTranslateRule regexRule = new RegexTranslateRule();
        regexRule.setSourceFieldName("dc.identifier");
        regexRule.setTargetFieldName("dc.identifier");
        regexRule.setRegexSearch("^doi:(.+)$");
        regexRule.setRegexReplace("https://doi.org/$1");
        
        String json = objectMapper.writeValueAsString(regexRule);
        rule.setJsonserialization(json);

        // Deserialize to concrete type
        RegexTranslateRule restored = objectMapper.readValue(
            rule.getJsonserialization(), 
            RegexTranslateRule.class
        );
        
        assertNotNull(restored);
        assertEquals("dc.identifier", restored.getSourceFieldName());
        assertEquals("dc.identifier", restored.getTargetFieldName());
        assertEquals("^doi:(.+)$", restored.getRegexSearch());
        assertEquals("https://doi.org/$1", restored.getRegexReplace());
    }

    @Test
    @DisplayName("Should handle Transformer with description")
    void testTransformerDescription() {
        Transformer transformer = new Transformer();
        transformer.setName("Complete Transformer");
        transformer.setDescription("This transformer normalizes all metadata fields");
        
        assertEquals("Complete Transformer", transformer.getName());
        assertEquals("This transformer normalizes all metadata fields", transformer.getDescription());
    }

    @Test
    @DisplayName("Should handle TransformerRule with description")
    void testTransformerRuleDescription() {
        TransformerRule rule = new TransformerRule();
        rule.setName("Language Code Fixer");
        rule.setDescription("Converts ISO 639-1 to ISO 639-3 codes");
        
        assertEquals("Language Code Fixer", rule.getName());
        assertEquals("Converts ISO 639-1 to ISO 639-3 codes", rule.getDescription());
    }

    @Test
    @DisplayName("Should handle roundtrip serialization of Transformer with all rules")
    void testFullTransformerRoundtrip() throws Exception {
        // Create complete transformer
        Transformer original = new Transformer();
        original.setName("Complete Metadata Transformer");
        original.setDescription("Transforms all aspects of metadata");

        // Add FieldContentTranslateRule
        TransformerRule contentRule = new TransformerRule();
        contentRule.setName("Content Translation");
        FieldContentTranslateRule content = new FieldContentTranslateRule();
        content.setTestFieldName("dc.type");
        content.setWriteFieldName("dc.type");
        content.getTranslationMap().put("artículo", "article");
        contentRule.setJsonserialization(objectMapper.writeValueAsString(content));

        // Add RegexTranslateRule
        TransformerRule regexRule = new TransformerRule();
        regexRule.setName("Regex Translation");
        RegexTranslateRule regex = new RegexTranslateRule();
        regex.setSourceFieldName("dc.identifier");
        regex.setTargetFieldName("dc.identifier");
        regex.setRegexSearch("^handle:(.+)$");
        regex.setRegexReplace("https://hdl.handle.net/$1");
        regexRule.setJsonserialization(objectMapper.writeValueAsString(regex));

        // Add FieldNameTranslateRule
        TransformerRule nameRule = new TransformerRule();
        nameRule.setName("Field Rename");
        FieldNameTranslateRule name = new FieldNameTranslateRule();
        name.setSourceFieldName("dc.author");
        name.setTargetFieldName("dc.contributor.author");
        nameRule.setJsonserialization(objectMapper.writeValueAsString(name));

        original.getRules().add(contentRule);
        original.getRules().add(regexRule);
        original.getRules().add(nameRule);

        // Serialize entire transformer to JSON
        String transformerJson = objectMapper.writeValueAsString(original);

        // Deserialize
        Transformer restored = objectMapper.readValue(transformerJson, Transformer.class);

        // Verify structure
        assertEquals(original.getName(), restored.getName());
        assertEquals(original.getDescription(), restored.getDescription());
        assertEquals(3, restored.getRules().size());

        // Verify each rule name
        assertEquals("Content Translation", restored.getRules().get(0).getName());
        assertEquals("Regex Translation", restored.getRules().get(1).getName());
        assertEquals("Field Rename", restored.getRules().get(2).getName());
    }

    @Test
    @DisplayName("Should handle FieldNameTranslateRule serialization")
    void testFieldNameTranslateRuleSerialization() throws Exception {
        TransformerRule rule = new TransformerRule();
        rule.setName("Creator to Author");
        
        FieldNameTranslateRule nameRule = new FieldNameTranslateRule();
        nameRule.setSourceFieldName("dc.creator");
        nameRule.setTargetFieldName("dc.contributor.author");
        
        String json = objectMapper.writeValueAsString(nameRule);
        rule.setJsonserialization(json);

        // Deserialize to concrete type
        FieldNameTranslateRule restored = objectMapper.readValue(
            rule.getJsonserialization(), 
            FieldNameTranslateRule.class
        );
        
        assertNotNull(restored);
        assertEquals("dc.creator", restored.getSourceFieldName());
        assertEquals("dc.contributor.author", restored.getTargetFieldName());
    }

    @Test
    @DisplayName("Should handle multiple transformations on same field")
    void testMultipleTransformationsOnSameField() throws Exception {
        Transformer transformer = new Transformer();
        transformer.setName("Multi-step Transformer");

        // First: fix protocol
        TransformerRule rule1 = new TransformerRule();
        rule1.setName("Fix Protocol");
        RegexTranslateRule regex1 = new RegexTranslateRule();
        regex1.setSourceFieldName("dc.identifier.uri");
        regex1.setTargetFieldName("dc.identifier.uri");
        regex1.setRegexSearch("^http://");
        regex1.setRegexReplace("https://");
        rule1.setJsonserialization(objectMapper.writeValueAsString(regex1));

        // Second: normalize domain
        TransformerRule rule2 = new TransformerRule();
        rule2.setName("Normalize Domain");
        RegexTranslateRule regex2 = new RegexTranslateRule();
        regex2.setSourceFieldName("dc.identifier.uri");
        regex2.setTargetFieldName("dc.identifier.uri");
        regex2.setRegexSearch("www\\.");
        regex2.setRegexReplace("");
        rule2.setJsonserialization(objectMapper.writeValueAsString(regex2));

        transformer.getRules().add(rule1);
        transformer.getRules().add(rule2);

        // Verify both rules target same field (deserialize to check)
        RegexTranslateRule restored1 = objectMapper.readValue(
            transformer.getRules().get(0).getJsonserialization(),
            RegexTranslateRule.class
        );
        RegexTranslateRule restored2 = objectMapper.readValue(
            transformer.getRules().get(1).getJsonserialization(),
            RegexTranslateRule.class
        );

        assertEquals(restored1.getSourceFieldName(), restored2.getSourceFieldName());
    }
}
