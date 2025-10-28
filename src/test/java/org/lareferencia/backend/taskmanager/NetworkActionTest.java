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

package org.lareferencia.backend.taskmanager;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NetworkAction Tests")
class NetworkActionTest {

    private NetworkAction action;

    @BeforeEach
    void setUp() {
        action = new NetworkAction();
    }

    @Test
    @DisplayName("Should create NetworkAction with default values")
    void testDefaultConstructor() {
        assertNotNull(action);
        assertNotNull(action.getWorkers());
        assertNotNull(action.getProperties());
        assertEquals("DUMMY", action.getName());
        assertEquals("DUMMY", action.getDescription());
        assertFalse(action.isIncremental());
        assertFalse(action.getRunOnSchedule());
        assertFalse(action.getAllwaysRunOnSchedule());
    }

    @Test
    @DisplayName("Should initialize with empty lists")
    void testEmptyLists() {
        assertEquals(0, action.getWorkers().size());
        assertEquals(0, action.getProperties().size());
    }

    @Test
    @DisplayName("Should set and get name")
    void testSetAndGetName() {
        action.setName("HarvestAction");
        assertEquals("HarvestAction", action.getName());
    }

    @Test
    @DisplayName("Should set and get description")
    void testSetAndGetDescription() {
        action.setDescription("Harvest OAI-PMH records");
        assertEquals("Harvest OAI-PMH records", action.getDescription());
    }

    @Test
    @DisplayName("Should set and get incremental mode")
    void testSetAndGetIncremental() {
        assertFalse(action.isIncremental());
        
        action.setIncremental(true);
        assertTrue(action.isIncremental());
        
        action.setIncremental(false);
        assertFalse(action.isIncremental());
    }

    @Test
    @DisplayName("Should set and get runOnSchedule")
    void testSetAndGetRunOnSchedule() {
        assertFalse(action.getRunOnSchedule());
        
        action.setRunOnSchedule(true);
        assertTrue(action.getRunOnSchedule());
    }

    @Test
    @DisplayName("Should set and get allwaysRunOnSchedule")
    void testSetAndGetAllwaysRunOnSchedule() {
        assertFalse(action.getAllwaysRunOnSchedule());
        
        action.setAllwaysRunOnSchedule(true);
        assertTrue(action.getAllwaysRunOnSchedule());
    }

    @Test
    @DisplayName("Should add workers to list")
    void testAddWorkers() {
        List<String> workers = new ArrayList<>();
        workers.add("ValidationWorker");
        workers.add("IndexerWorker");
        
        action.setWorkers(workers);
        
        assertEquals(2, action.getWorkers().size());
        assertTrue(action.getWorkers().contains("ValidationWorker"));
        assertTrue(action.getWorkers().contains("IndexerWorker"));
    }

    @Test
    @DisplayName("Should add properties to list")
    void testAddProperties() {
        NetworkProperty prop1 = new NetworkProperty();
        prop1.setName("TIMEOUT");
        prop1.setDescription("Connection timeout");
        
        NetworkProperty prop2 = new NetworkProperty();
        prop2.setName("MAX_RECORDS");
        prop2.setDescription("Maximum records to harvest");
        
        action.getProperties().add(prop1);
        action.getProperties().add(prop2);
        
        assertEquals(2, action.getProperties().size());
        assertEquals("TIMEOUT", action.getProperties().get(0).getName());
        assertEquals("MAX_RECORDS", action.getProperties().get(1).getName());
    }

    @Test
    @DisplayName("Should handle null name")
    void testNullName() {
        action.setName(null);
        assertNull(action.getName());
    }

    @Test
    @DisplayName("Should handle null description")
    void testNullDescription() {
        action.setDescription(null);
        assertNull(action.getDescription());
    }

    @Test
    @DisplayName("Should handle empty worker list")
    void testEmptyWorkerList() {
        action.setWorkers(new ArrayList<>());
        assertEquals(0, action.getWorkers().size());
    }

    @Test
    @DisplayName("Should handle empty property list")
    void testEmptyPropertyList() {
        action.setProperties(new ArrayList<>());
        assertEquals(0, action.getProperties().size());
    }

    @Test
    @DisplayName("Should configure complete action")
    void testCompleteConfiguration() {
        action.setName("FullHarvest");
        action.setDescription("Complete harvest with validation and indexing");
        action.setIncremental(false);
        action.setRunOnSchedule(true);
        action.setAllwaysRunOnSchedule(false);
        
        List<String> workers = new ArrayList<>();
        workers.add("HarvesterWorker");
        workers.add("ValidationWorker");
        workers.add("IndexerWorker");
        action.setWorkers(workers);
        
        NetworkProperty prop = new NetworkProperty();
        prop.setName("SCHEDULE");
        prop.setDescription("0 0 2 * * ?");
        action.getProperties().add(prop);
        
        assertEquals("FullHarvest", action.getName());
        assertEquals("Complete harvest with validation and indexing", action.getDescription());
        assertFalse(action.isIncremental());
        assertTrue(action.getRunOnSchedule());
        assertFalse(action.getAllwaysRunOnSchedule());
        assertEquals(3, action.getWorkers().size());
        assertEquals(1, action.getProperties().size());
    }

    @Test
    @DisplayName("Should handle incremental action")
    void testIncrementalAction() {
        action.setName("IncrementalHarvest");
        action.setIncremental(true);
        action.setRunOnSchedule(true);
        
        assertTrue(action.isIncremental());
        assertTrue(action.getRunOnSchedule());
    }

    @Test
    @DisplayName("Should handle scheduled action")
    void testScheduledAction() {
        action.setRunOnSchedule(true);
        action.setAllwaysRunOnSchedule(true);
        
        assertTrue(action.getRunOnSchedule());
        assertTrue(action.getAllwaysRunOnSchedule());
    }
}
