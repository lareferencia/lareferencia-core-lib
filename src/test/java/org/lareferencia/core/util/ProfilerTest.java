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

package org.lareferencia.core.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Profiler Tests")
class ProfilerTest {

    private static final Logger logger = LogManager.getLogger(ProfilerTest.class);

    @Test
    @DisplayName("Should create profiler with message")
    void testConstructor() {
        Profiler profiler = new Profiler(true, "Test profiler");
        
        assertNotNull(profiler);
        assertTrue(profiler.getProfileMode());
    }

    @Test
    @DisplayName("Should create profiler in disabled mode")
    void testConstructorDisabled() {
        Profiler profiler = new Profiler(false, "Disabled profiler");
        
        assertNotNull(profiler);
        assertFalse(profiler.getProfileMode());
    }

    @Test
    @DisplayName("Should start profiler and set times")
    void testStart() {
        Profiler profiler = new Profiler(true, "Test");
        
        profiler.start();
        
        assertTrue(profiler.getStartTime() > 0);
        assertTrue(profiler.getProfileTime() > 0);
        assertEquals(profiler.getStartTime(), profiler.getProfileTime());
    }

    @Test
    @DisplayName("Should not set times when disabled")
    void testStartDisabled() {
        Profiler profiler = new Profiler(false, "Test");
        
        profiler.start();
        
        assertEquals(0L, profiler.getStartTime());
        assertEquals(0L, profiler.getProfileTime());
    }

    @Test
    @DisplayName("Should measure time intervals")
    void testMessure() throws InterruptedException {
        Profiler profiler = new Profiler(true, "Test");
        profiler.start();
        
        Thread.sleep(10);
        profiler.messure("Step 1");
        
        String report = profiler.report();
        
        assertTrue(report.contains("Step 1"));
        assertTrue(report.contains("ms"));
    }

    @Test
    @DisplayName("Should measure multiple steps")
    void testMultipleMessures() throws InterruptedException {
        Profiler profiler = new Profiler(true, "Multi-step");
        profiler.start();
        
        Thread.sleep(5);
        profiler.messure("First");
        
        Thread.sleep(5);
        profiler.messure("Second");
        
        Thread.sleep(5);
        profiler.messure("Third");
        
        String report = profiler.report();
        
        assertTrue(report.contains("First"));
        assertTrue(report.contains("Second"));
        assertTrue(report.contains("Third"));
        assertTrue(report.contains("Total time"));
    }

    @Test
    @DisplayName("Should not measure when disabled")
    void testMessureDisabled() throws InterruptedException {
        Profiler profiler = new Profiler(false, "Test");
        profiler.start();
        
        Thread.sleep(10);
        profiler.messure("Step");
        
        String report = profiler.report();
        
        assertEquals("", report);
    }

    @Test
    @DisplayName("Should support newline in measurements")
    void testMessureWithNewLine() {
        Profiler profiler = new Profiler(true, "Test");
        profiler.start();
        
        profiler.messure("First", false);
        profiler.messure("Second", true);
        profiler.messure("Third", false);
        
        String report = profiler.report();
        
        assertTrue(report.contains("First"));
        assertTrue(report.contains("\nSecond"));
        assertTrue(report.contains("Third"));
    }

    @Test
    @DisplayName("Should calculate total time correctly")
    void testTotalTime() throws InterruptedException {
        Profiler profiler = new Profiler(true, "Total time test");
        profiler.start();
        
        Thread.sleep(20);
        profiler.messure("Step 1");
        
        Thread.sleep(20);
        profiler.messure("Step 2");
        
        String report = profiler.report();
        
        assertTrue(report.contains("Total time"));
        // Should be at least 40ms
        String[] parts = report.split("Total time: ");
        if (parts.length > 1) {
            String timePart = parts[1].split(" ms")[0].trim();
            int totalMs = Integer.parseInt(timePart);
            assertTrue(totalMs >= 30, "Total time should be at least 30ms, was: " + totalMs);
        }
    }

    @Test
    @DisplayName("Should report to logger")
    void testReportToLogger() {
        Profiler profiler = new Profiler(true, "Logger test");
        profiler.start();
        
        profiler.messure("Operation");
        
        // Should not throw exception
        assertDoesNotThrow(() -> profiler.report(logger));
    }

    @Test
    @DisplayName("Should not report to logger when disabled")
    void testReportToLoggerDisabled() {
        Profiler profiler = new Profiler(false, "Disabled");
        profiler.start();
        
        profiler.messure("Operation");
        
        // Should not throw exception
        assertDoesNotThrow(() -> profiler.report(logger));
    }

    @Test
    @DisplayName("Should return report as string")
    void testReportAsString() {
        Profiler profiler = new Profiler(true, "String report");
        profiler.start();
        
        profiler.messure("Task");
        
        String report = profiler.report();
        
        assertNotNull(report);
        assertFalse(report.isEmpty());
        assertTrue(report.contains("String report"));
        assertTrue(report.contains("Task"));
        assertTrue(report.contains("Total time"));
    }

    @Test
    @DisplayName("Should return empty string when disabled")
    void testReportAsStringDisabled() {
        Profiler profiler = new Profiler(false, "Disabled");
        profiler.start();
        
        profiler.messure("Task");
        
        String report = profiler.report();
        
        assertEquals("", report);
    }

    @Test
    @DisplayName("Should handle rapid consecutive measurements")
    void testRapidMeasurements() {
        Profiler profiler = new Profiler(true, "Rapid");
        profiler.start();
        
        for (int i = 0; i < 10; i++) {
            profiler.messure("Step " + i);
        }
        
        String report = profiler.report();
        
        assertTrue(report.contains("Step 0"));
        assertTrue(report.contains("Step 9"));
    }

    @Test
    @DisplayName("Should preserve initial message in report")
    void testInitialMessageInReport() {
        String initialMsg = "Custom profiler message";
        Profiler profiler = new Profiler(true, initialMsg);
        profiler.start();
        
        profiler.messure("Operation");
        
        String report = profiler.report();
        
        assertTrue(report.contains(initialMsg));
    }

    @Test
    @DisplayName("Should handle measurement without newline by default")
    void testDefaultMessureNoNewLine() {
        Profiler profiler = new Profiler(true, "Test");
        profiler.start();
        
        profiler.messure("First");
        profiler.messure("Second");
        
        String report = profiler.report();
        
        // Default should not have newline between measurements
        assertTrue(report.contains("First"));
        assertTrue(report.contains("Second"));
    }

    @Test
    @DisplayName("Should allow enabling/disabling profile mode")
    void testSetProfileMode() {
        Profiler profiler = new Profiler(true, "Test");
        
        assertTrue(profiler.getProfileMode());
        
        profiler.setProfileMode(false);
        assertFalse(profiler.getProfileMode());
        
        profiler.setProfileMode(true);
        assertTrue(profiler.getProfileMode());
    }

    @Test
    @DisplayName("Should return this from start for chaining")
    void testStartChaining() {
        Profiler profiler = new Profiler(true, "Chain");
        
        Profiler result = profiler.start();
        
        assertSame(profiler, result);
    }
}
