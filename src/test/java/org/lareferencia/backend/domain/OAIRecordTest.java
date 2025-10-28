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

package org.lareferencia.backend.domain;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.lareferencia.core.metadata.RecordStatus;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("OAIRecord Entity Unit Tests")
class OAIRecordTest {

    private OAIRecord oaiRecord;

    @BeforeEach
    void setUp() {
        oaiRecord = new OAIRecord();
    }

    @Test
    @DisplayName("Should create OAIRecord with default values")
    void testDefaultConstructor() {
        assertNotNull(oaiRecord);
        assertNull(oaiRecord.getId());
        assertEquals(RecordStatus.UNTESTED, oaiRecord.getStatus());
        assertFalse(oaiRecord.getTransformed());
        assertNotNull(oaiRecord.getDatestamp());
    }

    @Test
    @DisplayName("Should create OAIRecord with snapshot")
    void testConstructorWithSnapshot() {
        NetworkSnapshot snapshot = new NetworkSnapshot();
        OAIRecord record = new OAIRecord(snapshot);
        
        assertNotNull(record);
        assertEquals(snapshot, record.getSnapshot());
        assertEquals(RecordStatus.UNTESTED, record.getStatus());
    }

    @Test
    @DisplayName("Should set and get identifier")
    void testSetAndGetIdentifier() {
        String identifier = "oai:repository.example.com:12345";
        oaiRecord.setIdentifier(identifier);
        assertEquals(identifier, oaiRecord.getIdentifier());
    }

    @Test
    @DisplayName("Should set and get datestamp")
    void testSetAndGetDatestamp() {
        LocalDateTime datestamp = LocalDateTime.of(2023, 10, 26, 14, 30, 0);
        oaiRecord.setDatestamp(datestamp);
        assertEquals(datestamp, oaiRecord.getDatestamp());
    }

    @Test
    @DisplayName("Should set and get original metadata hash")
    void testSetAndGetOriginalMetadataHash() {
        String hash = "ABC123DEF456";
        oaiRecord.setOriginalMetadataHash(hash);
        assertEquals(hash, oaiRecord.getOriginalMetadataHash());
    }

    @Test
    @DisplayName("Should set and get published metadata hash")
    void testSetAndGetPublishedMetadataHash() {
        String hash = "XYZ789UVW012";
        oaiRecord.setPublishedMetadataHash(hash);
        assertEquals(hash, oaiRecord.getPublishedMetadataHash());
    }

    @Test
    @DisplayName("Should set and get status")
    void testSetAndGetStatus() {
        oaiRecord.setStatus(RecordStatus.VALID);
        assertEquals(RecordStatus.VALID, oaiRecord.getStatus());

        oaiRecord.setStatus(RecordStatus.INVALID);
        assertEquals(RecordStatus.INVALID, oaiRecord.getStatus());
    }

    @Test
    @DisplayName("Should set and get transformed flag")
    void testSetAndGetTransformed() {
        oaiRecord.setTransformed(true);
        assertTrue(oaiRecord.getTransformed());

        oaiRecord.setTransformed(false);
        assertFalse(oaiRecord.getTransformed());
    }

    @Test
    @DisplayName("Should get snapshot")
    void testGetSnapshot() {
        NetworkSnapshot snapshot = new NetworkSnapshot();
        OAIRecord record = new OAIRecord(snapshot);
        
        assertNotNull(record.getSnapshot());
        assertEquals(snapshot, record.getSnapshot());
    }

    @Test
    @DisplayName("Should get snapshot ID")
    void testGetSnapshotId() {
        // When created without explicit snapshot ID
        assertNull(oaiRecord.getSnapshotId());
    }

    @Test
    @DisplayName("Should have proper default status")
    void testDefaultStatus() {
        assertEquals(RecordStatus.UNTESTED, oaiRecord.getStatus());
    }

    @Test
    @DisplayName("Should have datestamp set on creation")
    void testDatestampOnCreation() {
        OAIRecord newRecord = new OAIRecord();
        assertNotNull(newRecord.getDatestamp());
        
        LocalDateTime now = LocalDateTime.now();
        assertTrue(newRecord.getDatestamp().isBefore(now.plusSeconds(1)));
        assertTrue(newRecord.getDatestamp().isAfter(now.minusSeconds(5)));
    }

    @Test
    @DisplayName("Should generate toString with identifier")
    void testToStringWithIdentifier() {
        oaiRecord.setIdentifier("test:123");
        
        String result = oaiRecord.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("OAIRecord"));
        assertTrue(result.contains("test:123"));
    }

    @Test
    @DisplayName("Should generate toString without identifier")
    void testToStringWithoutIdentifier() {
        String result = oaiRecord.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("OAIRecord"));
    }

    @Test
    @DisplayName("Should handle all RecordStatus values")
    void testAllRecordStatusValues() {
        oaiRecord.setStatus(RecordStatus.UNTESTED);
        assertEquals(RecordStatus.UNTESTED, oaiRecord.getStatus());

        oaiRecord.setStatus(RecordStatus.VALID);
        assertEquals(RecordStatus.VALID, oaiRecord.getStatus());

        oaiRecord.setStatus(RecordStatus.INVALID);
        assertEquals(RecordStatus.INVALID, oaiRecord.getStatus());

        oaiRecord.setStatus(RecordStatus.DELETED);
        assertEquals(RecordStatus.DELETED, oaiRecord.getStatus());
    }

    @Test
    @DisplayName("Should handle null identifier")
    void testNullIdentifier() {
        oaiRecord.setIdentifier(null);
        assertNull(oaiRecord.getIdentifier());
    }

    @Test
    @DisplayName("Should handle empty identifier")
    void testEmptyIdentifier() {
        oaiRecord.setIdentifier("");
        assertEquals("", oaiRecord.getIdentifier());
    }

    @Test
    @DisplayName("Should handle long identifier")
    void testLongIdentifier() {
        String longIdentifier = "oai:repository.example.com:" + "a".repeat(1000);
        oaiRecord.setIdentifier(longIdentifier);
        assertEquals(longIdentifier, oaiRecord.getIdentifier());
    }

    @Test
    @DisplayName("Should maintain state consistency")
    void testStateConsistency() {
        String identifier = "oai:test:123";
        LocalDateTime datestamp = LocalDateTime.now();
        String hash = "HASH123";
        
        oaiRecord.setIdentifier(identifier);
        oaiRecord.setDatestamp(datestamp);
        oaiRecord.setOriginalMetadataHash(hash);
        oaiRecord.setStatus(RecordStatus.VALID);
        oaiRecord.setTransformed(true);
        
        assertEquals(identifier, oaiRecord.getIdentifier());
        assertEquals(datestamp, oaiRecord.getDatestamp());
        assertEquals(hash, oaiRecord.getOriginalMetadataHash());
        assertEquals(RecordStatus.VALID, oaiRecord.getStatus());
        assertTrue(oaiRecord.getTransformed());
    }

    @Test
    @DisplayName("Should handle special characters in identifier")
    void testSpecialCharactersInIdentifier() {
        String identifier = "oai:repo.com:123/456#789?abc=def";
        oaiRecord.setIdentifier(identifier);
        assertEquals(identifier, oaiRecord.getIdentifier());
    }

    @Test
    @DisplayName("Should handle unicode in identifier")
    void testUnicodeInIdentifier() {
        String identifier = "oai:repo:文档123";
        oaiRecord.setIdentifier(identifier);
        assertEquals(identifier, oaiRecord.getIdentifier());
    }

    @Test
    @DisplayName("Should handle hash with 32 characters")
    void testHashLength() {
        String hash32 = "A".repeat(32);
        oaiRecord.setOriginalMetadataHash(hash32);
        assertEquals(hash32, oaiRecord.getOriginalMetadataHash());
        assertEquals(32, oaiRecord.getOriginalMetadataHash().length());
    }

    @Test
    @DisplayName("Should handle different hash values")
    void testDifferentHashes() {
        String originalHash = "ORIGINAL123";
        String publishedHash = "PUBLISHED456";
        
        oaiRecord.setOriginalMetadataHash(originalHash);
        oaiRecord.setPublishedMetadataHash(publishedHash);
        
        assertEquals(originalHash, oaiRecord.getOriginalMetadataHash());
        assertEquals(publishedHash, oaiRecord.getPublishedMetadataHash());
        assertNotEquals(oaiRecord.getOriginalMetadataHash(), oaiRecord.getPublishedMetadataHash());
    }
}
