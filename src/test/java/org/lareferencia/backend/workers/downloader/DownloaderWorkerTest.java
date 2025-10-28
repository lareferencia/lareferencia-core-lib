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

package org.lareferencia.backend.workers.downloader;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.lareferencia.core.worker.NetworkRunningContext;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DownloaderWorker Tests")
class DownloaderWorkerTest {

    private DownloaderWorker worker;

    @BeforeEach
    void setUp() {
        worker = new DownloaderWorker();
    }

    @Test
    @DisplayName("Should create DownloaderWorker instance")
    void testConstructor() {
        assertNotNull(worker);
        assertEquals("DownloaderWorker", worker.getName());
    }

    @Test
    @DisplayName("Should initialize with default timeout")
    void testDefaultTimeout() {
        assertEquals(10000, worker.getTimeOut());
    }

    @Test
    @DisplayName("Should set and get timeout")
    void testSetAndGetTimeout() {
        worker.setTimeOut(5000);
        assertEquals(5000, worker.getTimeOut());
    }

    @Test
    @DisplayName("Should handle zero timeout")
    void testZeroTimeout() {
        worker.setTimeOut(0);
        assertEquals(0, worker.getTimeOut());
    }

    @Test
    @DisplayName("Should handle large timeout values")
    void testLargeTimeout() {
        worker.setTimeOut(60000);
        assertEquals(60000, worker.getTimeOut());
    }

    @Test
    @DisplayName("Should set and get target directory")
    void testSetAndGetTargetDirectory() {
        String targetDir = "/tmp/downloads";
        worker.setTargetDirectory(targetDir);
        assertEquals(targetDir, worker.getTargetDirectory());
    }

    @Test
    @DisplayName("Should handle null target directory")
    void testNullTargetDirectory() {
        worker.setTargetDirectory(null);
        assertNull(worker.getTargetDirectory());
    }

    @Test
    @DisplayName("Should handle absolute path")
    void testAbsolutePath() {
        String absolutePath = "/var/lib/lareferencia/downloads";
        worker.setTargetDirectory(absolutePath);
        assertEquals(absolutePath, worker.getTargetDirectory());
    }

    @Test
    @DisplayName("Should handle relative path")
    void testRelativePath() {
        String relativePath = "./downloads";
        worker.setTargetDirectory(relativePath);
        assertEquals(relativePath, worker.getTargetDirectory());
    }

    @Test
    @DisplayName("Should set running context")
    void testSetRunningContext() {
        NetworkRunningContext context = createMockContext();
        worker.setRunningContext(context);
        assertEquals(context, worker.getRunningContext());
    }

    @Test
    @DisplayName("Should set incremental mode")
    void testIncrementalMode() {
        assertFalse(worker.isIncremental());
        
        worker.setIncremental(true);
        assertTrue(worker.isIncremental());
    }

    @Test
    @DisplayName("Should extend BaseBatchWorker")
    void testInheritance() {
        assertTrue(worker instanceof org.lareferencia.core.worker.BaseBatchWorker);
    }

    @Test
    @DisplayName("Should set page size")
    void testPageSize() {
        worker.setPageSize(50);
        assertEquals(50, worker.getPageSize());
    }

    @Test
    @DisplayName("Should set serial lane ID")
    void testSerialLaneId() {
        worker.setSerialLaneId(99L);
        assertEquals(99L, worker.getSerialLaneId());
    }

    @Test
    @DisplayName("Should handle empty string target directory")
    void testEmptyTargetDirectory() {
        worker.setTargetDirectory("");
        assertEquals("", worker.getTargetDirectory());
    }

    @Test
    @DisplayName("Should handle paths with spaces")
    void testPathsWithSpaces() {
        String pathWithSpaces = "/path/with spaces/downloads";
        worker.setTargetDirectory(pathWithSpaces);
        assertEquals(pathWithSpaces, worker.getTargetDirectory());
    }

    @Test
    @DisplayName("Should handle negative timeout")
    void testNegativeTimeout() {
        worker.setTimeOut(-1000);
        assertEquals(-1000, worker.getTimeOut());
    }

    @Test
    @DisplayName("Should configure timeout and directory together")
    void testConfigureBothProperties() {
        worker.setTimeOut(15000);
        worker.setTargetDirectory("/data/downloads");
        
        assertEquals(15000, worker.getTimeOut());
        assertEquals("/data/downloads", worker.getTargetDirectory());
    }

    // Helper method to create mock context without heavy dependencies
    private NetworkRunningContext createMockContext() {
        org.lareferencia.backend.domain.Network network = new org.lareferencia.backend.domain.Network();
        network.setAcronym("TEST");
        network.setName("Test Network");
        return new NetworkRunningContext(network);
    }
}
