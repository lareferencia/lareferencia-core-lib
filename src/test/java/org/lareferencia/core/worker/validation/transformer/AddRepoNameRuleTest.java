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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AddRepoNameRule
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("AddRepoNameRule Tests")
class AddRepoNameRuleTest {


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

    private AddRepoNameRule rule;

        @BeforeEach
    void setUp() {
        rule = new AddRepoNameRule();
        
        lenient().when(snapshotMetadata.getNetwork()).thenReturn(network);
        lenient().when(record.getSnapshot()).thenReturn(snapshot);
        lenient().when(snapshot.getNetwork()).thenReturn(network);
    }

    @Test
    @DisplayName("Should append repository name when doRepoNameAppend is true")
    void testAppendRepoName() {
        rule.setDoRepoNameAppend(true);
        rule.setRepoNameField("dc.source");
        rule.setRepoNamePrefix("reponame:");
        
        when(network.getName()).thenReturn("TestRepository");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        // RepositoryNameHelper.appendNameToMetadata is called internally
    }

    @Test
    @DisplayName("Should append institution name when doInstNameAppend is true")
    void testAppendInstName() {
        rule.setDoInstNameAppend(true);
        rule.setInstNameField("dc.source");
        rule.setInstNamePrefix("instname:");
        rule.setInstAcronField("dc.source");
        rule.setInstAcronPrefix("instacron:");
        
        when(network.getInstitutionName()).thenReturn("Test University");
        when(network.getInstitutionAcronym()).thenReturn("TU");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should append both repository and institution names")
    void testAppendBothNames() {
        rule.setDoRepoNameAppend(true);
        rule.setDoInstNameAppend(true);
        rule.setRepoNameField("dc.source");
        rule.setInstNameField("dc.source");
        rule.setInstAcronField("dc.source");
        
        when(network.getName()).thenReturn("TestRepo");
        when(network.getInstitutionName()).thenReturn("Test Institution");
        when(network.getInstitutionAcronym()).thenReturn("TI");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should return false when both flags are false")
    void testNoAppendWhenFlagsAreFalse() {
        rule.setDoRepoNameAppend(false);
        rule.setDoInstNameAppend(false);

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should use custom repository name prefix")
    void testCustomRepoNamePrefix() {
        rule.setDoRepoNameAppend(true);
        rule.setRepoNameField("dc.source");
        rule.setRepoNamePrefix("repository:");
        
        when(network.getName()).thenReturn("CustomRepo");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        assertEquals("repository:", rule.getRepoNamePrefix());
    }

    @Test
    @DisplayName("Should use custom institution name prefix")
    void testCustomInstNamePrefix() {
        rule.setDoInstNameAppend(true);
        rule.setInstNameField("dc.publisher");
        rule.setInstNamePrefix("institution:");
        rule.setInstAcronField("dc.publisher");
        rule.setInstAcronPrefix("acronym:");
        
        when(network.getInstitutionName()).thenReturn("Custom University");
        when(network.getInstitutionAcronym()).thenReturn("CU");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        assertEquals("institution:", rule.getInstNamePrefix());
        assertEquals("acronym:", rule.getInstAcronPrefix());
    }

    @Test
    @DisplayName("Should verify getter and setter for doRepoNameAppend")
    void testDoRepoNameAppendGetterSetter() {
        rule.setDoRepoNameAppend(true);
        assertTrue(rule.getDoRepoNameAppend());
        
        rule.setDoRepoNameAppend(false);
        assertFalse(rule.getDoRepoNameAppend());
    }

    @Test
    @DisplayName("Should verify getter and setter for doRepoNameReplace")
    void testDoRepoNameReplaceGetterSetter() {
        rule.setDoRepoNameReplace(true);
        assertTrue(rule.getDoRepoNameReplace());
    }

    @Test
    @DisplayName("Should verify getter and setter for repoNameField")
    void testRepoNameFieldGetterSetter() {
        rule.setRepoNameField("dc.identifier");
        assertEquals("dc.identifier", rule.getRepoNameField());
    }

    @Test
    @DisplayName("Should verify getter and setter for repoNamePrefix")
    void testRepoNamePrefixGetterSetter() {
        rule.setRepoNamePrefix("repo:");
        assertEquals("repo:", rule.getRepoNamePrefix());
    }

    @Test
    @DisplayName("Should verify getter and setter for doInstNameAppend")
    void testDoInstNameAppendGetterSetter() {
        rule.setDoInstNameAppend(true);
        assertTrue(rule.getDoInstNameAppend());
    }

    @Test
    @DisplayName("Should verify getter and setter for doInstNameReplace")
    void testDoInstNameReplaceGetterSetter() {
        rule.setDoInstNameReplace(true);
        assertTrue(rule.getDoInstNameReplace());
    }

    @Test
    @DisplayName("Should verify getter and setter for instNameField")
    void testInstNameFieldGetterSetter() {
        rule.setInstNameField("dc.publisher");
        assertEquals("dc.publisher", rule.getInstNameField());
    }

    @Test
    @DisplayName("Should verify getter and setter for instNamePrefix")
    void testInstNamePrefixGetterSetter() {
        rule.setInstNamePrefix("inst:");
        assertEquals("inst:", rule.getInstNamePrefix());
    }

    @Test
    @DisplayName("Should verify getter and setter for instAcronField")
    void testInstAcronFieldGetterSetter() {
        rule.setInstAcronField("dc.contributor");
        assertEquals("dc.contributor", rule.getInstAcronField());
    }

    @Test
    @DisplayName("Should verify getter and setter for instAcronPrefix")
    void testInstAcronPrefixGetterSetter() {
        rule.setInstAcronPrefix("acron:");
        assertEquals("acron:", rule.getInstAcronPrefix());
    }

    @Test
    @DisplayName("Should use default field names")
    void testDefaultFieldNames() {
        assertEquals("dc.source.none", rule.getRepoNameField());
        assertEquals("dc.source.none", rule.getInstNameField());
        assertEquals("dc.source.none", rule.getInstAcronField());
    }

    @Test
    @DisplayName("Should use default prefixes")
    void testDefaultPrefixes() {
        assertEquals("reponame:", rule.getRepoNamePrefix());
        assertEquals("instname:", rule.getInstNamePrefix());
        assertEquals("instacron:", rule.getInstAcronPrefix());
    }

    @Test
    @DisplayName("Should handle Unicode in repository name")
    void testUnicodeInRepoName() {
        rule.setDoRepoNameAppend(true);
        rule.setRepoNameField("dc.source");
        
        when(network.getName()).thenReturn("Repositório Científico");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should handle Unicode in institution name")
    void testUnicodeInInstName() {
        rule.setDoInstNameAppend(true);
        rule.setInstNameField("dc.source");
        rule.setInstAcronField("dc.source");
        
        when(network.getInstitutionName()).thenReturn("Universität München");
        when(network.getInstitutionAcronym()).thenReturn("UM");

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
    }
}
