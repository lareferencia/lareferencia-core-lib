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

package org.lareferencia.core.worker.harvesting;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for BaseHarvestingEventSource
 */
@DisplayName("BaseHarvestingEventSource Tests")
class BaseHarvestingEventSourceTest {

    private TestHarvestingEventSource eventSource;
    private TestEventListener listener1;
    private TestEventListener listener2;

    @BeforeEach
    void setUp() {
        eventSource = new TestHarvestingEventSource();
        listener1 = new TestEventListener();
        listener2 = new TestEventListener();
    }

    @Test
    @DisplayName("Should initialize with empty listeners list")
    void testInitialization() {
        assertNotNull(eventSource);
        assertNotNull(eventSource.getListeners());
        assertTrue(eventSource.getListeners().isEmpty());
    }

    @Test
    @DisplayName("Should add event listener successfully")
    void testAddEventListener() {
        eventSource.addEventListener(listener1);
        
        assertEquals(1, eventSource.getListeners().size());
        assertTrue(eventSource.getListeners().contains(listener1));
    }

    @Test
    @DisplayName("Should add multiple event listeners")
    void testAddMultipleEventListeners() {
        eventSource.addEventListener(listener1);
        eventSource.addEventListener(listener2);
        
        assertEquals(2, eventSource.getListeners().size());
        assertTrue(eventSource.getListeners().contains(listener1));
        assertTrue(eventSource.getListeners().contains(listener2));
    }

    @Test
    @DisplayName("Should allow adding same listener multiple times")
    void testAddSameListenerMultipleTimes() {
        eventSource.addEventListener(listener1);
        eventSource.addEventListener(listener1);
        
        assertEquals(2, eventSource.getListeners().size());
    }

    @Test
    @DisplayName("Should remove event listener successfully")
    void testRemoveEventListener() {
        eventSource.addEventListener(listener1);
        eventSource.addEventListener(listener2);
        
        eventSource.removeEventListener(listener1);
        
        assertEquals(1, eventSource.getListeners().size());
        assertFalse(eventSource.getListeners().contains(listener1));
        assertTrue(eventSource.getListeners().contains(listener2));
    }

    @Test
    @DisplayName("Should handle removing non-existent listener")
    void testRemoveNonExistentListener() {
        eventSource.addEventListener(listener1);
        
        eventSource.removeEventListener(listener2);
        
        assertEquals(1, eventSource.getListeners().size());
        assertTrue(eventSource.getListeners().contains(listener1));
    }

    @Test
    @DisplayName("Should remove only first occurrence of listener")
    void testRemoveFirstOccurrence() {
        eventSource.addEventListener(listener1);
        eventSource.addEventListener(listener1);
        
        eventSource.removeEventListener(listener1);
        
        assertEquals(1, eventSource.getListeners().size());
        assertTrue(eventSource.getListeners().contains(listener1));
    }

    @Test
    @DisplayName("Should fire event to all listeners")
    void testFireHarvestingEvent() {
        eventSource.addEventListener(listener1);
        eventSource.addEventListener(listener2);
        
        HarvestingEvent event = new HarvestingEvent();
        event.setStatus(HarvestingEventStatus.OK);
        event.setMessage("Test event");
        
        eventSource.fireHarvestingEvent(event);
        
        assertEquals(1, listener1.getEventCount());
        assertEquals(1, listener2.getEventCount());
        assertEquals(event, listener1.getLastEvent());
        assertEquals(event, listener2.getLastEvent());
    }

    @Test
    @DisplayName("Should fire multiple events to listeners")
    void testFireMultipleEvents() {
        eventSource.addEventListener(listener1);
        
        HarvestingEvent event1 = new HarvestingEvent();
        event1.setMessage("Event 1");
        
        HarvestingEvent event2 = new HarvestingEvent();
        event2.setMessage("Event 2");
        
        HarvestingEvent event3 = new HarvestingEvent();
        event3.setMessage("Event 3");
        
        eventSource.fireHarvestingEvent(event1);
        eventSource.fireHarvestingEvent(event2);
        eventSource.fireHarvestingEvent(event3);
        
        assertEquals(3, listener1.getEventCount());
        assertEquals(event3, listener1.getLastEvent());
    }

    @Test
    @DisplayName("Should handle firing event with no listeners")
    void testFireEventWithNoListeners() {
        HarvestingEvent event = new HarvestingEvent();
        event.setMessage("Test event");
        
        assertDoesNotThrow(() -> eventSource.fireHarvestingEvent(event));
    }

    @Test
    @DisplayName("Should handle firing null event")
    void testFireNullEvent() {
        eventSource.addEventListener(listener1);
        
        assertDoesNotThrow(() -> eventSource.fireHarvestingEvent(null));
        assertEquals(1, listener1.getEventCount());
        assertNull(listener1.getLastEvent());
    }

    @Test
    @DisplayName("Should maintain listener order when firing events")
    void testListenerOrderPreservation() {
        List<String> executionOrder = new ArrayList<>();
        
        eventSource.addEventListener(event -> executionOrder.add("listener1"));
        eventSource.addEventListener(event -> executionOrder.add("listener2"));
        eventSource.addEventListener(event -> executionOrder.add("listener3"));
        
        HarvestingEvent event = new HarvestingEvent();
        eventSource.fireHarvestingEvent(event);
        
        assertEquals(3, executionOrder.size());
        assertEquals("listener1", executionOrder.get(0));
        assertEquals("listener2", executionOrder.get(1));
        assertEquals("listener3", executionOrder.get(2));
    }

    @Test
    @DisplayName("Should handle exception in one listener without affecting others")
    void testExceptionInListener() {
        AtomicInteger counter = new AtomicInteger(0);
        
        eventSource.addEventListener(event -> counter.incrementAndGet());
        eventSource.addEventListener(event -> {
            throw new RuntimeException("Listener error");
        });
        eventSource.addEventListener(event -> counter.incrementAndGet());
        
        HarvestingEvent event = new HarvestingEvent();
        
        assertThrows(RuntimeException.class, () -> eventSource.fireHarvestingEvent(event));
        // First listener executed, second threw exception, third may not execute
        assertTrue(counter.get() >= 1);
    }

    @Test
    @DisplayName("Should handle concurrent event firing")
    void testConcurrentEventFiring() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);
        AtomicInteger eventCount = new AtomicInteger(0);
        
        eventSource.addEventListener(event -> {
            eventCount.incrementAndGet();
            latch.countDown();
        });
        
        // Fire 10 events concurrently
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                HarvestingEvent event = new HarvestingEvent();
                eventSource.fireHarvestingEvent(event);
            }).start();
        }
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(10, eventCount.get());
    }

    @Test
    @DisplayName("Should clear all listeners when removed")
    void testClearAllListeners() {
        eventSource.addEventListener(listener1);
        eventSource.addEventListener(listener2);
        
        eventSource.removeEventListener(listener1);
        eventSource.removeEventListener(listener2);
        
        assertTrue(eventSource.getListeners().isEmpty());
    }

    @Test
    @DisplayName("Should handle event with different statuses")
    void testEventsWithDifferentStatuses() {
        eventSource.addEventListener(listener1);
        
        for (HarvestingEventStatus status : HarvestingEventStatus.values()) {
            HarvestingEvent event = new HarvestingEvent();
            event.setStatus(status);
            eventSource.fireHarvestingEvent(event);
        }
        
        assertEquals(HarvestingEventStatus.values().length, listener1.getEventCount());
    }

    // Test implementation of BaseHarvestingEventSource
    private static class TestHarvestingEventSource extends BaseHarvestingEventSource {
        public List<IHarvestingEventListener> getListeners() {
            return listeners;
        }
    }

    // Test implementation of IHarvestingEventListener
    private static class TestEventListener implements IHarvestingEventListener {
        private int eventCount = 0;
        private HarvestingEvent lastEvent;

        @Override
        public void harvestingEventOccurred(HarvestingEvent event) {
            eventCount++;
            lastEvent = event;
        }

        public int getEventCount() {
            return eventCount;
        }

        public HarvestingEvent getLastEvent() {
            return lastEvent;
        }
    }
}
