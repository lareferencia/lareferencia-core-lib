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

package org.lareferencia.core.worker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.ScheduledFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("BaseWorker Tests")
class BaseWorkerTest {

    private TestWorker worker;
    private TestRunningContext context;

    // Concrete implementation for testing
    private static class TestWorker extends BaseWorker<TestRunningContext> {
        private boolean runCalled = false;

        @Override
        public void run() {
            runCalled = true;
        }

        public boolean isRunCalled() {
            return runCalled;
        }
    }

    private static class TestRunningContext implements IRunningContext {
        private final String contextId = "test-context";

        @Override
        public String getId() {
            return contextId;
        }

        @Override
        public String toString() {
            return contextId;
        }
    }

    @BeforeEach
    void setUp() {
        worker = new TestWorker();
        context = new TestRunningContext();
    }

    @Test
    @DisplayName("Should create worker with default values")
    void testDefaultConstructor() {
        assertNotNull(worker);
        assertEquals("TestWorker", worker.getName());
        assertEquals(-1L, worker.getSerialLaneId());
        assertFalse(worker.isIncremental());
        assertNull(worker.getRunningContext());
        assertNull(worker.getScheduledFuture());
    }

    @Test
    @DisplayName("Should create worker with context")
    void testConstructorWithContext() {
        TestWorker contextWorker = new TestWorker();
        contextWorker.setRunningContext(context);
        
        assertNotNull(contextWorker.getRunningContext());
        assertEquals(context, contextWorker.getRunningContext());
    }

    @Test
    @DisplayName("Should set and get name")
    void testSetAndGetName() {
        worker.setName("CustomWorker");
        assertEquals("CustomWorker", worker.getName());
    }

    @Test
    @DisplayName("Should set and get running context")
    void testSetAndGetRunningContext() {
        worker.setRunningContext(context);
        assertEquals(context, worker.getRunningContext());
    }

    @Test
    @DisplayName("Should set and get serial lane ID")
    void testSetAndGetSerialLaneId() {
        worker.setSerialLaneId(42L);
        assertEquals(42L, worker.getSerialLaneId());
    }

    @Test
    @DisplayName("Should set and get incremental mode")
    void testSetAndGetIncremental() {
        assertFalse(worker.isIncremental());
        
        worker.setIncremental(true);
        assertTrue(worker.isIncremental());
        
        worker.setIncremental(false);
        assertFalse(worker.isIncremental());
    }

    @Test
    @DisplayName("Should set and get scheduled future")
    void testSetAndGetScheduledFuture() {
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        
        worker.setScheduledFuture(mockFuture);
        assertEquals(mockFuture, worker.getScheduledFuture());
    }

    @Test
    @DisplayName("Should stop worker without scheduled future")
    void testStopWithoutScheduledFuture() {
        assertDoesNotThrow(() -> worker.stop());
    }

    @Test
    @DisplayName("Should stop worker and cancel scheduled future")
    void testStopWithScheduledFuture() {
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        worker.setScheduledFuture(mockFuture);
        
        worker.stop();
        
        verify(mockFuture, times(1)).cancel(true);
    }

    @Test
    @DisplayName("Should execute run method")
    void testRun() {
        assertFalse(worker.isRunCalled());
        
        worker.run();
        
        assertTrue(worker.isRunCalled());
    }

    @Test
    @DisplayName("Should handle null values gracefully")
    void testNullHandling() {
        worker.setRunningContext(null);
        assertNull(worker.getRunningContext());
        
        worker.setScheduledFuture(null);
        assertNull(worker.getScheduledFuture());
        
        worker.setName(null);
        assertNull(worker.getName());
    }

    @Test
    @DisplayName("Should set serial lane ID to -1 by default")
    void testDefaultSerialLaneId() {
        assertEquals(-1L, worker.getSerialLaneId());
    }

    @Test
    @DisplayName("Should handle negative serial lane IDs")
    void testNegativeSerialLaneId() {
        worker.setSerialLaneId(-100L);
        assertEquals(-100L, worker.getSerialLaneId());
    }

    @Test
    @DisplayName("Should handle large serial lane IDs")
    void testLargeSerialLaneId() {
        worker.setSerialLaneId(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, worker.getSerialLaneId());
    }
}
