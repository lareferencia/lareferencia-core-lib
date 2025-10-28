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

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NetworkProperty Tests")
class NetworkPropertyTest {

    private NetworkProperty property;

    @BeforeEach
    void setUp() {
        property = new NetworkProperty();
    }

    @Test
    @DisplayName("Should create NetworkProperty with default values")
    void testDefaultConstructor() {
        assertNotNull(property);
        assertEquals("DUMMY", property.getName());
        assertEquals("DUMMY", property.getDescription());
    }

    @Test
    @DisplayName("Should set and get name")
    void testSetAndGetName() {
        property.setName("TIMEOUT");
        assertEquals("TIMEOUT", property.getName());
    }

    @Test
    @DisplayName("Should set and get description")
    void testSetAndGetDescription() {
        property.setDescription("Connection timeout in milliseconds");
        assertEquals("Connection timeout in milliseconds", property.getDescription());
    }

    @Test
    @DisplayName("Should handle null name")
    void testNullName() {
        property.setName(null);
        assertNull(property.getName());
    }

    @Test
    @DisplayName("Should handle null description")
    void testNullDescription() {
        property.setDescription(null);
        assertNull(property.getDescription());
    }

    @Test
    @DisplayName("Should handle empty name")
    void testEmptyName() {
        property.setName("");
        assertEquals("", property.getName());
    }

    @Test
    @DisplayName("Should handle empty description")
    void testEmptyDescription() {
        property.setDescription("");
        assertEquals("", property.getDescription());
    }

    @Test
    @DisplayName("Should configure property with values")
    void testConfigureProperty() {
        property.setName("MAX_RECORDS");
        property.setDescription("Maximum number of records to harvest");
        
        assertEquals("MAX_RECORDS", property.getName());
        assertEquals("Maximum number of records to harvest", property.getDescription());
    }

    @Test
    @DisplayName("Should handle property name with special characters")
    void testSpecialCharactersInName() {
        property.setName("PROPERTY_NAME-123");
        assertEquals("PROPERTY_NAME-123", property.getName());
    }

    @Test
    @DisplayName("Should handle long description")
    void testLongDescription() {
        String longDesc = "This is a very long description that contains multiple words and describes in detail what this property is used for in the system configuration";
        property.setDescription(longDesc);
        assertEquals(longDesc, property.getDescription());
    }

    @Test
    @DisplayName("Should create multiple properties")
    void testMultipleProperties() {
        NetworkProperty prop1 = new NetworkProperty();
        prop1.setName("TIMEOUT");
        prop1.setDescription("Timeout value");
        
        NetworkProperty prop2 = new NetworkProperty();
        prop2.setName("RETRIES");
        prop2.setDescription("Number of retries");
        
        assertEquals("TIMEOUT", prop1.getName());
        assertEquals("RETRIES", prop2.getName());
        assertNotEquals(prop1.getName(), prop2.getName());
    }

    @Test
    @DisplayName("Should handle uppercase name")
    void testUppercaseName() {
        property.setName("SCHEDULE_CRON");
        assertEquals("SCHEDULE_CRON", property.getName());
    }

    @Test
    @DisplayName("Should handle mixed case name")
    void testMixedCaseName() {
        property.setName("scheduleCron");
        assertEquals("scheduleCron", property.getName());
    }

    @Test
    @DisplayName("Should handle numeric name")
    void testNumericName() {
        property.setName("12345");
        assertEquals("12345", property.getName());
    }

    @Test
    @DisplayName("Should overwrite name")
    void testOverwriteName() {
        property.setName("OLD_NAME");
        assertEquals("OLD_NAME", property.getName());
        
        property.setName("NEW_NAME");
        assertEquals("NEW_NAME", property.getName());
    }

    @Test
    @DisplayName("Should overwrite description")
    void testOverwriteDescription() {
        property.setDescription("Old description");
        assertEquals("Old description", property.getDescription());
        
        property.setDescription("New description");
        assertEquals("New description", property.getDescription());
    }
}
