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
import org.springframework.data.domain.Page;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("BaseBatchWorker Tests")
class BaseBatchWorkerTest {

    private TestBatchWorker worker;
    private TestRunningContext context;

    // Concrete implementation for testing
    private static class TestBatchWorker extends BaseBatchWorker<String, TestRunningContext> {
        
        private int preRunCallCount = 0;
        private int prePageCallCount = 0;
        private int postPageCallCount = 0;
        private int postRunCallCount = 0;
        private int processItemCallCount = 0;

        @Override
        protected void preRun() {
            preRunCallCount++;
        }

        @Override
        public void prePage() {
            prePageCallCount++;
        }

        @Override
        public void processItem(String item) {
            processItemCallCount++;
        }

        @Override
        public void postPage() {
            postPageCallCount++;
        }

        @Override
        protected void postRun() {
            postRunCallCount++;
        }

        public int getPreRunCallCount() {
            return preRunCallCount;
        }

        public int getPrePageCallCount() {
            return prePageCallCount;
        }

        public int getPostPageCallCount() {
            return postPageCallCount;
        }

        public int getPostRunCallCount() {
            return postRunCallCount;
        }

        public int getProcessItemCallCount() {
            return processItemCallCount;
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
        worker = new TestBatchWorker();
        context = new TestRunningContext();
        worker.setRunningContext(context);
    }

    @Test
    @DisplayName("Should create batch worker with default values")
    void testDefaultConstructor() {
        assertNotNull(worker);
        assertEquals(100, worker.getPageSize()); // DEFAULT_PAGE_SIZE
        assertEquals(1, worker.getTotalPages());
        assertEquals(0, worker.getActualPage());
    }

    @Test
    @DisplayName("Should set and get page size")
    void testSetAndGetPageSize() {
        worker.setPageSize(50);
        assertEquals(50, worker.getPageSize());
        
        worker.setPageSize(200);
        assertEquals(200, worker.getPageSize());
    }

    @Test
    @DisplayName("Should set paginator")
    void testSetPaginator() {
        @SuppressWarnings("unchecked")
        IPaginator<String> mockPaginator = mock(IPaginator.class);
        
        worker.setPaginator(mockPaginator);
        // Can't get paginator directly, but can verify it through run
    }

    @Test
    @DisplayName("Should handle page size boundaries")
    void testPageSizeBoundaries() {
        worker.setPageSize(1);
        assertEquals(1, worker.getPageSize());
        
        worker.setPageSize(10000);
        assertEquals(10000, worker.getPageSize());
    }

    @Test
    @DisplayName("Should get total pages default value")
    void testGetTotalPagesDefault() {
        assertEquals(1, worker.getTotalPages());
    }

    @Test
    @DisplayName("Should get actual page default value")
    void testGetActualPageDefault() {
        assertEquals(0, worker.getActualPage());
    }

    @Test
    @DisplayName("Should inherit from BaseWorker")
    void testInheritance() {
        assertTrue(worker instanceof BaseWorker);
        assertTrue(worker instanceof IBatchWorker);
    }

    @Test
    @DisplayName("Should implement IBatchWorker interface")
    void testInterfaceImplementation() {
        assertTrue(worker instanceof IBatchWorker);
    }

    @Test
    @DisplayName("Should set and get name from parent")
    void testNameFromParent() {
        worker.setName("TestBatch");
        assertEquals("TestBatch", worker.getName());
    }

    @Test
    @DisplayName("Should set and get serial lane ID from parent")
    void testSerialLaneIdFromParent() {
        worker.setSerialLaneId(123L);
        assertEquals(123L, worker.getSerialLaneId());
    }

    @Test
    @DisplayName("Should set and get incremental mode from parent")
    void testIncrementalFromParent() {
        worker.setIncremental(true);
        assertTrue(worker.isIncremental());
    }

    @Test
    @DisplayName("Should create worker with context constructor")
    void testConstructorWithContext() {
        TestBatchWorker contextWorker = new TestBatchWorker();
        contextWorker.setRunningContext(context);
        
        assertEquals(context, contextWorker.getRunningContext());
    }

    @Test
    @DisplayName("Should handle zero page size")
    void testZeroPageSize() {
        worker.setPageSize(0);
        assertEquals(0, worker.getPageSize());
    }

    @Test
    @DisplayName("Should handle negative page size")
    void testNegativePageSize() {
        worker.setPageSize(-10);
        assertEquals(-10, worker.getPageSize());
    }

    @Test
    @DisplayName("Should allow null paginator")
    void testNullPaginator() {
        assertDoesNotThrow(() -> worker.setPaginator(null));
    }
}
