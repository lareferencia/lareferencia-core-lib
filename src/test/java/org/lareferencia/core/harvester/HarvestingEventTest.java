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

package org.lareferencia.core.harvester;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.metadata.OAIRecordMetadata;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for HarvestingEvent
 */
@DisplayName("HarvestingEvent Tests")
class HarvestingEventTest {

    private HarvestingEvent event;

    @BeforeEach
    void setUp() {
        event = new HarvestingEvent();
    }

    @Test
    @DisplayName("Should initialize with empty collections")
    void testInitialization() {
        assertNotNull(event.getRecords());
        assertNotNull(event.getDeletedRecordsIdentifiers());
        assertNotNull(event.getMissingRecordsIdentifiers());
        
        assertTrue(event.getRecords().isEmpty());
        assertTrue(event.getDeletedRecordsIdentifiers().isEmpty());
        assertTrue(event.getMissingRecordsIdentifiers().isEmpty());
        
        assertNull(event.getMessage());
        assertNull(event.getOriginURL());
        assertNull(event.getStatus());
        assertNull(event.getResumptionToken());
        assertNull(event.getMetadataPrefix());
        assertFalse(event.isRecordMissing());
    }

    @Test
    @DisplayName("Should set and get message")
    void testMessageProperty() {
        String message = "Test harvesting message";
        event.setMessage(message);
        
        assertEquals(message, event.getMessage());
    }

    @Test
    @DisplayName("Should set and get origin URL")
    void testOriginURLProperty() {
        String url = "http://example.com/oai";
        event.setOriginURL(url);
        
        assertEquals(url, event.getOriginURL());
    }

    @Test
    @DisplayName("Should set and get status")
    void testStatusProperty() {
        event.setStatus(HarvestingEventStatus.OK);
        assertEquals(HarvestingEventStatus.OK, event.getStatus());
        
        event.setStatus(HarvestingEventStatus.ERROR_FATAL);
        assertEquals(HarvestingEventStatus.ERROR_FATAL, event.getStatus());
    }

    @Test
    @DisplayName("Should set and get resumption token")
    void testResumptionTokenProperty() {
        String token = "resumption-token-12345";
        event.setResumptionToken(token);
        
        assertEquals(token, event.getResumptionToken());
    }

    @Test
    @DisplayName("Should set and get metadata prefix")
    void testMetadataPrefixProperty() {
        String prefix = "oai_dc";
        event.setMetadataPrefix(prefix);
        
        assertEquals(prefix, event.getMetadataPrefix());
    }

    @Test
    @DisplayName("Should set and get record missing flag")
    void testRecordMissingProperty() {
        assertFalse(event.isRecordMissing());
        
        event.setRecordMissing(true);
        assertTrue(event.isRecordMissing());
        
        event.setRecordMissing(false);
        assertFalse(event.isRecordMissing());
    }

    @Test
    @DisplayName("Should add records to collection")
    void testAddRecords() throws Exception {
        OAIRecordMetadata record1 = new OAIRecordMetadata("record1", "<metadata><dc><title>Title 1</title></dc></metadata>");
        
        OAIRecordMetadata record2 = new OAIRecordMetadata("record2", "<metadata><dc><title>Title 2</title></dc></metadata>");
        
        event.getRecords().add(record1);
        event.getRecords().add(record2);
        
        assertEquals(2, event.getRecords().size());
        assertTrue(event.getRecords().contains(record1));
        assertTrue(event.getRecords().contains(record2));
    }

    @Test
    @DisplayName("Should add deleted records identifiers")
    void testAddDeletedRecordsIdentifiers() {
        event.getDeletedRecordsIdentifiers().add("deleted-1");
        event.getDeletedRecordsIdentifiers().add("deleted-2");
        event.getDeletedRecordsIdentifiers().add("deleted-3");
        
        assertEquals(3, event.getDeletedRecordsIdentifiers().size());
        assertTrue(event.getDeletedRecordsIdentifiers().contains("deleted-1"));
        assertTrue(event.getDeletedRecordsIdentifiers().contains("deleted-2"));
        assertTrue(event.getDeletedRecordsIdentifiers().contains("deleted-3"));
    }

    @Test
    @DisplayName("Should add missing records identifiers")
    void testAddMissingRecordsIdentifiers() {
        event.getMissingRecordsIdentifiers().add("missing-1");
        event.getMissingRecordsIdentifiers().add("missing-2");
        
        assertEquals(2, event.getMissingRecordsIdentifiers().size());
        assertTrue(event.getMissingRecordsIdentifiers().contains("missing-1"));
        assertTrue(event.getMissingRecordsIdentifiers().contains("missing-2"));
    }

    @Test
    @DisplayName("Should reset event to initial state")
    void testReset() throws Exception {
        // Set up event with data
        event.setMessage("Test message");
        event.setOriginURL("http://example.com");
        event.setStatus(HarvestingEventStatus.OK);
        event.setResumptionToken("token-123");
        event.setMetadataPrefix("oai_dc");
        event.setRecordMissing(true);
        
        OAIRecordMetadata record = new OAIRecordMetadata("test-record", "<metadata><dc><title>Test</title></dc></metadata>");
        event.getRecords().add(record);
        event.getDeletedRecordsIdentifiers().add("deleted-1");
        event.getMissingRecordsIdentifiers().add("missing-1");
        
        // Reset
        event.reset();
        
        // Verify reset state
        assertNull(event.getMessage());
        assertNull(event.getOriginURL());
        assertNull(event.getStatus());
        assertNull(event.getResumptionToken());
        assertNull(event.getMetadataPrefix());
        assertFalse(event.isRecordMissing());
        
        assertTrue(event.getRecords().isEmpty());
        assertTrue(event.getDeletedRecordsIdentifiers().isEmpty());
        assertTrue(event.getMissingRecordsIdentifiers().isEmpty());
    }

    @Test
    @DisplayName("Should allow reuse after reset")
    void testReuseAfterReset() throws Exception {
        // First use
        event.setMessage("First message");
        event.setStatus(HarvestingEventStatus.OK);
        event.getRecords().add(new OAIRecordMetadata("id1", "<metadata><dc><title>Test</title></dc></metadata>"));
        
        // Reset
        event.reset();
        
        // Second use
        event.setMessage("Second message");
        event.setStatus(HarvestingEventStatus.ERROR_RETRY);
        event.getDeletedRecordsIdentifiers().add("deleted-1");
        
        assertEquals("Second message", event.getMessage());
        assertEquals(HarvestingEventStatus.ERROR_RETRY, event.getStatus());
        assertEquals(1, event.getDeletedRecordsIdentifiers().size());
        assertTrue(event.getRecords().isEmpty());
    }

    @Test
    @DisplayName("Should handle multiple resets")
    void testMultipleResets() {
        event.setMessage("Test");
        event.reset();
        event.reset();
        event.reset();
        
        assertNull(event.getMessage());
        assertTrue(event.getRecords().isEmpty());
    }

    @Test
    @DisplayName("Should preserve collections references after reset")
    void testCollectionReferencesAfterReset() {
        var recordsRef = event.getRecords();
        var deletedRef = event.getDeletedRecordsIdentifiers();
        var missingRef = event.getMissingRecordsIdentifiers();
        
        event.reset();
        
        assertSame(recordsRef, event.getRecords());
        assertSame(deletedRef, event.getDeletedRecordsIdentifiers());
        assertSame(missingRef, event.getMissingRecordsIdentifiers());
    }

    @Test
    @DisplayName("Should handle null values")
    void testNullValues() {
        event.setMessage(null);
        event.setOriginURL(null);
        event.setStatus(null);
        event.setResumptionToken(null);
        event.setMetadataPrefix(null);
        
        assertNull(event.getMessage());
        assertNull(event.getOriginURL());
        assertNull(event.getStatus());
        assertNull(event.getResumptionToken());
        assertNull(event.getMetadataPrefix());
    }

    @Test
    @DisplayName("Should handle empty strings")
    void testEmptyStrings() {
        event.setMessage("");
        event.setOriginURL("");
        event.setResumptionToken("");
        event.setMetadataPrefix("");
        
        assertEquals("", event.getMessage());
        assertEquals("", event.getOriginURL());
        assertEquals("", event.getResumptionToken());
        assertEquals("", event.getMetadataPrefix());
    }

    @Test
    @DisplayName("Should support toString method")
    void testToString() {
        event.setMessage("Test message");
        event.setStatus(HarvestingEventStatus.OK);
        
        String toString = event.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("Test message") || toString.contains("message"));
        assertTrue(toString.contains("OK") || toString.contains("status"));
    }

    @Test
    @DisplayName("Should handle large number of records")
    void testLargeNumberOfRecords() throws Exception {
        for (int i = 0; i < 1000; i++) {
            OAIRecordMetadata record = new OAIRecordMetadata("record-" + i, "<metadata><dc><title>Record " + i + "</title></dc></metadata>");
            event.getRecords().add(record);
        }
        
        assertEquals(1000, event.getRecords().size());
        
        event.reset();
        assertTrue(event.getRecords().isEmpty());
    }

    @Test
    @DisplayName("Should handle all event statuses")
    void testAllEventStatuses() {
        for (HarvestingEventStatus status : HarvestingEventStatus.values()) {
            event.setStatus(status);
            assertEquals(status, event.getStatus());
        }
    }
}
