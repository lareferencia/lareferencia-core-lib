package org.lareferencia.core.worker.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.domain.OAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.RuleSerializer;
import org.w3c.dom.Element;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AddProvenanceMetadataRule
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("AddProvenanceMetadataRule Tests")
class AddProvenanceMetadataRuleTest {

    @Mock
    private SnapshotMetadata snapshotMetadata;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    @Mock
    private NetworkSnapshot snapshot;

    @Mock
    private SnapshotMetadata.NetworkInfo networkInfo;

    private AddProvenanceMetadataRule rule;
    private Map<String, Object> attributes;

    @BeforeEach
    void setUp() {
        rule = new AddProvenanceMetadataRule();
        attributes = new HashMap<>();

        lenient().when(snapshotMetadata.getNetwork()).thenReturn(networkInfo);
        lenient().when(record.getSnapshot()).thenReturn(snapshot);
        lenient().when(networkInfo.getAttributes()).thenReturn(attributes);
        lenient().when(record.getDatestamp()).thenReturn(java.time.LocalDateTime.now());
    }

    @Test
    @DisplayName("Should add provenance metadata from network attributes")
    void testAddProvenanceMetadata() throws Exception {
        attributes.put("source_type", "Institutional Repository");
        attributes.put("source_url", "http://repository.edu");
        attributes.put("institution_type", "University");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, times(3)).removeFieldOcurrence(anyString());
        verify(metadata, atLeastOnce()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should handle empty attributes")
    void testEmptyAttributes() throws Exception {
        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, atLeastOnce()).removeFieldOcurrence(anyString());
    }

    @Test
    @DisplayName("Should handle null attribute values")
    void testNullAttributeValues() throws Exception {
        attributes.put("source_type", null);
        attributes.put("source_url", null);

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, atLeastOnce()).addFieldOcurrence(anyString(), eq(""));
    }

    @Test
    @DisplayName("Should remove existing fields before adding new ones")
    void testRemoveExistingFields() throws Exception {
        attributes.put("source_type", "Repository");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).removeFieldOcurrence(rule.getRepoNameField());
        verify(metadata).removeFieldOcurrence(rule.getContactEmailField());
        verify(metadata).removeFieldOcurrence(rule.getOaiIdentifierField());
    }

    @Test
    @DisplayName("Should handle all repository metadata fields")
    void testAllRepositoryFields() throws Exception {
        attributes.put("source_type", "Institutional Repository");
        attributes.put("source_url", "http://repo.edu");
        attributes.put("institution_type", "University");
        attributes.put("institution_url", "http://university.edu");
        attributes.put("oai_base_url", "http://repo.edu/oai");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, atLeastOnce()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should verify getter and setter for repoTypeField")
    void testRepoTypeFieldGetterSetter() {
        rule.setRepoTypeField("custom:repoType");
        assertEquals("custom:repoType", rule.getRepoTypeField());
    }

    @Test
    @DisplayName("Should verify getter and setter for repoUrlField")
    void testRepoUrlFieldGetterSetter() {
        rule.setRepoUrlField("custom:repoUrl");
        assertEquals("custom:repoUrl", rule.getRepoUrlField());
    }

    @Test
    @DisplayName("Should verify getter and setter for instTypeField")
    void testInstTypeFieldGetterSetter() {
        rule.setInstTypeField("custom:instType");
        assertEquals("custom:instType", rule.getInstTypeField());
    }

    @Test
    @DisplayName("Should verify getter and setter for instUrlField")
    void testInstUrlFieldGetterSetter() {
        rule.setInstUrlField("custom:instUrl");
        assertEquals("custom:instUrl", rule.getInstUrlField());
    }

    @Test
    @DisplayName("Should verify getter and setter for oaiUrlField")
    void testOaiUrlFieldGetterSetter() {
        rule.setOaiUrlField("custom:oaiUrl");
        assertEquals("custom:oaiUrl", rule.getOaiUrlField());
    }

    @Test
    @DisplayName("Should handle Unicode in attribute values")
    void testUnicodeAttributeValues() throws Exception {
        attributes.put("source_type", "Repositório Institucional");
        attributes.put("institution_type", "Universität");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, atLeastOnce()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should handle special characters in attribute values")
    void testSpecialCharactersInAttributes() throws Exception {
        attributes.put("source_url", "http://repository.edu/oai?verb=ListRecords");
        attributes.put("contact_email", "admin@repository.edu");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, atLeastOnce()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should always return true")
    void testAlwaysReturnsTrue() throws Exception {
        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
    }

    @Test
    void legacyProfilePublishesEmptyFieldsInOrderAndReplacesOnlyThreeFields() throws Exception {
        var loaded = new RuleSerializer().deserializeTransformerFromJsonString("""
                {"@class":"org.lareferencia.backend.validation.transformer.AddProvenanceMetadataRule"}
                """);
        attributes.put("source_type", null);
        attributes.put("country", "  Perú & Chile  ");
        attributes.put("repository_id", 123);
        var xml = new OAIRecordMetadata("oai:repo:1", "<metadata/>");
        assertTrue(loaded.transform(snapshotMetadata, record, xml));
        // Check the elements after serialization, even when an empty text node disappears.
        var published = new OAIRecordMetadata("oai:repo:1", xml.toString());
        var fields = published.getDOMDocument().getElementsByTagName("field");
        List<String> names = new ArrayList<>();
        for (int i = 0; i < fields.getLength(); i++) {
            names.add(((Element) fields.item(i)).getAttribute("name"));
        }
        assertEquals(List.of("repositoryType", "repositoryURL", "institutionType", "institutionURL",
                "baseURL", "mail", "country", "DOI", "ISSN", "ISSN_L", "repositoryID", "harvestDate",
                "name", "identifier"), names);
        assertEquals("", fields.item(0).getTextContent());
        assertEquals("", fields.item(1).getTextContent());
        assertEquals("  Perú & Chile  ", fields.item(6).getTextContent());
        assertEquals("opendoar:123", fields.item(10).getTextContent());
        attributes.remove("repository_id");
        assertTrue(loaded.transform(snapshotMetadata, record, xml));
        assertEquals(25, xml.getDOMDocument().getElementsByTagName("field").getLength());
        assertEquals(List.of("opendoar:123", "opendoar:"), xml.getFieldOcurrences("repository:repositoryID"));
        assertEquals(1, xml.getFieldNodesIncludingEmpty("repository:mail").size());
        assertEquals(1, xml.getFieldNodesIncludingEmpty("repository:name").size());
        assertEquals(List.of("oai:repo:1"), xml.getFieldOcurrences("others:identifier"));
        assertEquals(2, xml.getFieldNodesIncludingEmpty("repository:repositoryType").size());
    }

    @Test
    void customFieldsUseFirstAvailableParentAndKeepExistingFieldInItsOwnBranch() throws Exception {
        var loaded = new RuleSerializer().deserializeTransformerFromJsonString("""
                {"@class":"org.lareferencia.backend.validation.transformer.AddProvenanceMetadataRule",
                 "repoTypeField":"dc.source:type","repoUrlField":"dc.source:url"}
                """);
        var xml = new OAIRecordMetadata("id", """
                <metadata xmlns="http://www.lyncode.com/xoai"><element name="dc">
                  <element name="source"><field name="note">first</field></element>
                  <element name="source"><field name="url">old</field></element>
                </element></metadata>
                """);
        var first = xml.getFieldNodes("dc.source:note").get(0).getParentNode();
        var second = xml.getFieldNodes("dc.source:url").get(0).getParentNode();
        assertTrue(loaded.transform(snapshotMetadata, record, xml));
        var type = xml.getFieldNodesIncludingEmpty("dc.source:type").get(0);
        assertSame(first, type.getParentNode());
        assertSame(second, xml.getFieldNodesIncludingEmpty("dc.source:url").get(1).getParentNode());
        assertEquals(first.getNamespaceURI(), type.getNamespaceURI());
    }

    @Test
    @DisplayName("Should handle large attribute values")
    void testLargeAttributeValues() throws Exception {
        String largeValue = "a".repeat(1000);
        attributes.put("source_type", largeValue);

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should handle whitespace in attribute values")
    void testWhitespaceInAttributes() throws Exception {
        attributes.put("source_type", "  Institutional Repository  ");
        attributes.put("institution_type", "\tUniversity\n");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, atLeastOnce()).addFieldOcurrence(anyString(), anyString());
    }
}
