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

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NetworkSnapshot Entity Unit Tests")
class NetworkSnapshotTest {

    private NetworkSnapshot snapshot;

    @BeforeEach
    void setUp() {
        snapshot = new NetworkSnapshot();
    }

    @Test
    @DisplayName("Should create NetworkSnapshot with default values")
    void testDefaultConstructor() {
        assertNotNull(snapshot);
        assertNull(snapshot.getId());
        assertEquals(SnapshotStatus.INITIALIZED, snapshot.getStatus());
        assertEquals(SnapshotIndexStatus.UNKNOWN, snapshot.getIndexStatus());
        assertEquals(0, snapshot.getSize());
        assertEquals(0, snapshot.getValidSize());
        assertEquals(0, snapshot.getTransformedSize());
        assertFalse(snapshot.isDeleted());
    }

    @Test
    @DisplayName("Should set and get previous snapshot ID")
    void testSetAndGetPreviousSnapshotId() {
        Long previousId = 123L;
        snapshot.setPreviousSnapshotId(previousId);
        assertEquals(previousId, snapshot.getPreviousSnapshotId());
    }

    @Test
    @DisplayName("Should set and get status correctly")
    void testStatusProperty() {
        snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
        assertEquals(SnapshotStatus.HARVESTING_FINISHED_VALID, snapshot.getStatus());
        
        snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_ERROR);
        assertEquals(SnapshotStatus.HARVESTING_FINISHED_ERROR, snapshot.getStatus());
    }

    @Test
    @DisplayName("Should set and get indexStatus correctly")
    void testIndexStatusProperty() {
        snapshot.setIndexStatus(SnapshotIndexStatus.INDEXED);
        assertEquals(SnapshotIndexStatus.INDEXED, snapshot.getIndexStatus());
        
        snapshot.setIndexStatus(SnapshotIndexStatus.FAILED);
        assertEquals(SnapshotIndexStatus.FAILED, snapshot.getIndexStatus());
    }    @Test
    @DisplayName("Should set and get start time")
    void testSetAndGetStartTime() {
        LocalDateTime startTime = LocalDateTime.of(2023, 10, 26, 10, 0, 0);
        snapshot.setStartTime(startTime);
        assertEquals(startTime, snapshot.getStartTime());
    }

    @Test
    @DisplayName("Should set and get end time")
    void testSetAndGetEndTime() {
        LocalDateTime endTime = LocalDateTime.of(2023, 10, 26, 12, 0, 0);
        snapshot.setEndTime(endTime);
        assertEquals(endTime, snapshot.getEndTime());
    }

    @Test
    @DisplayName("Should set and get last incremental time")
    void testSetAndGetLastIncrementalTime() {
        LocalDateTime incrementalTime = LocalDateTime.of(2023, 10, 26, 11, 30, 0);
        snapshot.setLastIncrementalTime(incrementalTime);
        assertEquals(incrementalTime, snapshot.getLastIncrementalTime());
    }

    @Test
    @DisplayName("Should set and get size")
    void testSetAndGetSize() {
        snapshot.setSize(1000);
        assertEquals(1000, snapshot.getSize());
    }

    @Test
    @DisplayName("Should set and get valid size")
    void testSetAndGetValidSize() {
        snapshot.setValidSize(850);
        assertEquals(850, snapshot.getValidSize());
    }

    @Test
    @DisplayName("Should set and get transformed size")
    void testSetAndGetTransformedSize() {
        snapshot.setTransformedSize(800);
        assertEquals(800, snapshot.getTransformedSize());
    }

    @Test
    @DisplayName("Should set and get resumption token")
    void testSetAndGetResumptionToken() {
        String token = "resumption-token-12345";
        snapshot.setResumptionToken(token);
        assertEquals(token, snapshot.getResumptionToken());
    }

    @Test
    @DisplayName("Should set and get network")
    void testSetAndGetNetwork() {
        Network network = new Network();
        network.setAcronym("TEST");
        snapshot.setNetwork(network);
        
        assertNotNull(snapshot.getNetwork());
        assertEquals("TEST", snapshot.getNetwork().getAcronym());
    }

    @Test
    @DisplayName("Should handle all SnapshotStatus values")
    void testAllSnapshotStatusValues() {
        snapshot.setStatus(SnapshotStatus.HARVESTING);
        assertEquals(SnapshotStatus.HARVESTING, snapshot.getStatus());
        
        snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
        assertEquals(SnapshotStatus.HARVESTING_FINISHED_VALID, snapshot.getStatus());
        
        snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_ERROR);
        assertEquals(SnapshotStatus.HARVESTING_FINISHED_ERROR, snapshot.getStatus());
        
        snapshot.setStatus(SnapshotStatus.HARVESTING_STOPPED);
        assertEquals(SnapshotStatus.HARVESTING_STOPPED, snapshot.getStatus());
    }        @Test
    @DisplayName("Should handle all SnapshotIndexStatus values")
    void testAllSnapshotIndexStatusValues() {
        snapshot.setIndexStatus(SnapshotIndexStatus.UNKNOWN);
        assertEquals(SnapshotIndexStatus.UNKNOWN, snapshot.getIndexStatus());
        
        snapshot.setIndexStatus(SnapshotIndexStatus.INDEXED);
        assertEquals(SnapshotIndexStatus.INDEXED, snapshot.getIndexStatus());
    }

    @Test
    @DisplayName("Should calculate time duration")
    void testCalculateTimeDuration() {
        LocalDateTime start = LocalDateTime.of(2023, 10, 26, 10, 0, 0);
        LocalDateTime end = LocalDateTime.of(2023, 10, 26, 12, 30, 0);
        
        snapshot.setStartTime(start);
        snapshot.setEndTime(end);
        
        assertNotNull(snapshot.getStartTime());
        assertNotNull(snapshot.getEndTime());
        assertTrue(snapshot.getEndTime().isAfter(snapshot.getStartTime()));
    }

    @Test
    @DisplayName("Should handle null resumption token")
    void testNullResumptionToken() {
        snapshot.setResumptionToken(null);
        assertNull(snapshot.getResumptionToken());
    }

    @Test
    @DisplayName("Should maintain state consistency")
    void testStateConsistency() {
        LocalDateTime startTime = LocalDateTime.now();
        Integer size = 1000;
        Integer validSize = 900;
        Integer transformedSize = 850;
        
        snapshot.setStartTime(startTime);
        snapshot.setSize(size);
        snapshot.setValidSize(validSize);
        snapshot.setTransformedSize(transformedSize);
        snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
        
        assertEquals(startTime, snapshot.getStartTime());
        assertEquals(size, snapshot.getSize());
        assertEquals(validSize, snapshot.getValidSize());
        assertEquals(transformedSize, snapshot.getTransformedSize());
        assertEquals(SnapshotStatus.HARVESTING_FINISHED_VALID, snapshot.getStatus());
    }

    @Test
    @DisplayName("Should handle zero sizes")
    void testZeroSizes() {
        snapshot.setSize(0);
        snapshot.setValidSize(0);
        snapshot.setTransformedSize(0);
        
        assertEquals(0, snapshot.getSize());
        assertEquals(0, snapshot.getValidSize());
        assertEquals(0, snapshot.getTransformedSize());
    }

    @Test
    @DisplayName("Should handle large sizes")
    void testLargeSizes() {
        Integer largeSize = Integer.MAX_VALUE;
        snapshot.setSize(largeSize);
        assertEquals(largeSize, snapshot.getSize());
    }

    @Test
    @DisplayName("Should handle edge case times")
    void testEdgeCaseTimes() {
        LocalDateTime midnight = LocalDateTime.of(2023, 10, 26, 0, 0, 0);
        LocalDateTime endOfDay = LocalDateTime.of(2023, 10, 26, 23, 59, 59);
        
        snapshot.setStartTime(midnight);
        snapshot.setEndTime(endOfDay);
        
        assertEquals(midnight, snapshot.getStartTime());
        assertEquals(endOfDay, snapshot.getEndTime());
    }

    @Test
    @DisplayName("Should handle same start and end time")
    void testSameStartAndEndTime() {
        LocalDateTime time = LocalDateTime.now();
        snapshot.setStartTime(time);
        snapshot.setEndTime(time);
        
        assertEquals(snapshot.getStartTime(), snapshot.getEndTime());
    }

    @Test
    @DisplayName("Should have valid transformed size not greater than valid size")
    void testTransformedSizeLogic() {
        snapshot.setSize(1000);
        snapshot.setValidSize(800);
        snapshot.setTransformedSize(750);
        
        assertTrue(snapshot.getTransformedSize() <= snapshot.getValidSize());
        assertTrue(snapshot.getValidSize() <= snapshot.getSize());
    }
}
