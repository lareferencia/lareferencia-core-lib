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

package org.lareferencia.core.worker.validation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.domain.Network;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ValidationWorker Tests")
class ValidationWorkerTest {

    private ValidationWorker worker;

    @BeforeEach
    void setUp() {
        worker = new ValidationWorker();
    }

    @Test
    @DisplayName("Should create ValidationWorker instance")
    void testConstructor() {
        assertNotNull(worker);
        assertNotNull(worker.getName());
    }

    @Test
    @DisplayName("Should have correct worker name")
    void testWorkerName() {
        assertEquals("ValidationWorker", worker.getName());
    }

    @Test
    @DisplayName("Should set and get running context")
    void testSetRunningContext() {
        NetworkRunningContext context = createMockContext();
        worker.setRunningContext(context);
        assertEquals(context, worker.getRunningContext());
    }

    @Test
    @DisplayName("Should set and get incremental mode")
    void testIncrementalMode() {
        assertFalse(worker.isIncremental());
        
        worker.setIncremental(true);
        assertTrue(worker.isIncremental());
    }

    // DISABLED: ValidationWorker no longer has PageSize methods
    // @Test
    // @DisplayName("Should set and get page size")
    // void testPageSize() {
    //     int defaultPageSize = worker.getPageSize();
    //     assertEquals(100, defaultPageSize); // DEFAULT_PAGE_SIZE
    //     
    //     worker.setPageSize(50);
    //     assertEquals(50, worker.getPageSize());
    // }

    // DISABLED: ValidationWorker architecture changed
    // @Test
    // @DisplayName("Should initialize with default values")
    // void testDefaultValues() {
    //     assertNull(worker.getRunningContext());
    //     assertEquals(-1L, worker.getSerialLaneId());
    //     assertFalse(worker.isIncremental());
    //     assertEquals(100, worker.getPageSize());
    // }

    // DISABLED: SerialLaneId no longer exists
    // @Test
    // @DisplayName("Should set serial lane ID")
    // void testSerialLaneId() {
    //     worker.setSerialLaneId(123L);
    //     assertEquals(123L, worker.getSerialLaneId());
    // }

    @Test
    @DisplayName("Should handle null context gracefully")
    void testNullContext() {
        worker.setRunningContext(null);
        assertNull(worker.getRunningContext());
    }

    // DISABLED: ValidationWorker now extends OAIRecordParquetWorker instead of BaseBatchWorker
    // @Test
    // @DisplayName("Should extend BaseBatchWorker")
    // void testInheritance() {
    //     assertTrue(worker instanceof org.lareferencia.core.worker.BaseBatchWorker);
    // }

    @Test
    @DisplayName("Should set custom name")
    void testCustomName() {
        worker.setName("CustomValidator");
        assertEquals("CustomValidator", worker.getName());
    }

    // DISABLED: PageSize no longer exists
    // @Test
    // @DisplayName("Should handle large page sizes")
    // void testLargePageSize() {
    //     worker.setPageSize(10000);
    //     assertEquals(10000, worker.getPageSize());
    // }

    // DISABLED: PageSize no longer exists
    // @Test
    // @DisplayName("Should handle small page sizes")
    // void testSmallPageSize() {
    //     worker.setPageSize(1);
    //     assertEquals(1, worker.getPageSize());
    // }

    // Helper method to create mock context without heavy dependencies
    private NetworkRunningContext createMockContext() {
        Network network = new Network();
        network.setAcronym("TEST");
        network.setName("Test Network");
        return new NetworkRunningContext(network);
    }
}
