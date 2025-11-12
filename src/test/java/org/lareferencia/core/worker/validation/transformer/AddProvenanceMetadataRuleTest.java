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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
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
    private Network network;

    private AddProvenanceMetadataRule rule;
    private Map<String, Object> attributes;

    @BeforeEach
    void setUp() {
        rule = new AddProvenanceMetadataRule();
        attributes = new HashMap<>();
        
        lenient().when(snapshotMetadata.getNetwork()).thenReturn(network);
        lenient().when(record.getSnapshot()).thenReturn(snapshot);
        lenient().when(snapshot.getNetwork()).thenReturn(network);
        lenient().when(network.getAttributes()).thenReturn(attributes);
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
        verify(metadata).removeFieldOcurrence(AddProvenanceMetadataRule.getRepoNameField());
        verify(metadata).removeFieldOcurrence(AddProvenanceMetadataRule.getContactEmailField());
        verify(metadata).removeFieldOcurrence(AddProvenanceMetadataRule.getOaiIdentifierField());
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
        AddProvenanceMetadataRule.setRepoTypeField("custom:repoType");
        assertEquals("custom:repoType", AddProvenanceMetadataRule.getRepoTypeField());
    }

    @Test
    @DisplayName("Should verify getter and setter for repoUrlField")
    void testRepoUrlFieldGetterSetter() {
        AddProvenanceMetadataRule.setRepoUrlField("custom:repoUrl");
        assertEquals("custom:repoUrl", AddProvenanceMetadataRule.getRepoUrlField());
    }

    @Test
    @DisplayName("Should verify getter and setter for instTypeField")
    void testInstTypeFieldGetterSetter() {
        AddProvenanceMetadataRule.setInstTypeField("custom:instType");
        assertEquals("custom:instType", AddProvenanceMetadataRule.getInstTypeField());
    }

    @Test
    @DisplayName("Should verify getter and setter for instUrlField")
    void testInstUrlFieldGetterSetter() {
        AddProvenanceMetadataRule.setInstUrlField("custom:instUrl");
        assertEquals("custom:instUrl", AddProvenanceMetadataRule.getInstUrlField());
    }

    @Test
    @DisplayName("Should verify getter and setter for oaiUrlField")
    void testOaiUrlFieldGetterSetter() {
        AddProvenanceMetadataRule.setOaiUrlField("custom:oaiUrl");
        assertEquals("custom:oaiUrl", AddProvenanceMetadataRule.getOaiUrlField());
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
