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

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Network Entity Unit Tests")
class NetworkTest {

    private Network network;

    @BeforeEach
    void setUp() {
        network = new Network();
    }

    @Test
    @DisplayName("Should create network with default values")
    void testDefaultConstructor() {
        assertNotNull(network);
        assertNull(network.getId());
        assertNull(network.getPublished());
        assertNull(network.getAcronym());
        assertNull(network.getName());
    }

    @Test
    @DisplayName("Should set and get published status")
    void testSetAndGetPublished() {
        network.setPublished(true);
        assertTrue(network.getPublished());

        network.setPublished(false);
        assertFalse(network.getPublished());
    }

    @Test
    @DisplayName("Should set and get acronym")
    void testSetAndGetAcronym() {
        String acronym = "TEST";
        network.setAcronym(acronym);
        assertEquals(acronym, network.getAcronym());
    }

    @Test
    @DisplayName("Should set and get name")
    void testSetAndGetName() {
        String name = "Test Network";
        network.setName(name);
        assertEquals(name, network.getName());
    }

    @Test
    @DisplayName("Should set and get institution acronym")
    void testSetAndGetInstitutionAcronym() {
        String institutionAcronym = "INST";
        network.setInstitutionAcronym(institutionAcronym);
        assertEquals(institutionAcronym, network.getInstitutionAcronym());
    }

    @Test
    @DisplayName("Should set and get institution name")
    void testSetAndGetInstitutionName() {
        String institutionName = "Test Institution";
        network.setInstitutionName(institutionName);
        assertEquals(institutionName, network.getInstitutionName());
    }

    @Test
    @DisplayName("Should have default metadata prefix")
    void testDefaultMetadataPrefix() {
        assertNotNull(network.getMetadataPrefix());
        assertEquals("oai_dc", network.getMetadataPrefix());
    }

    @Test
    @DisplayName("Should set and get metadata prefix")
    void testSetAndGetMetadataPrefix() {
        String metadataPrefix = "oai_openaire";
        network.setMetadataPrefix(metadataPrefix);
        assertEquals(metadataPrefix, network.getMetadataPrefix());
    }

    @Test
    @DisplayName("Should have default metadata store schema")
    void testDefaultMetadataStoreSchema() {
        assertNotNull(network.getMetadataStoreSchema());
        assertEquals("xoai", network.getMetadataStoreSchema());
    }

    @Test
    @DisplayName("Should set and get metadata store schema")
    void testSetAndGetMetadataStoreSchema() {
        String schema = "custom_schema";
        network.setMetadataStoreSchema(schema);
        assertEquals(schema, network.getMetadataStoreSchema());
    }

    @Test
    @DisplayName("Should set and get origin URL")
    void testSetAndGetOriginURL() {
        String url = "http://example.com/oai";
        network.setOriginURL(url);
        assertEquals(url, network.getOriginURL());
    }

    @Test
    @DisplayName("Should set and get attributes")
    void testSetAndGetAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", 123);
        
        network.setAttributes(attributes);
        
        assertNotNull(network.getAttributes());
        assertEquals(2, network.getAttributes().size());
        assertEquals("value1", network.getAttributes().get("key1"));
        assertEquals(123, network.getAttributes().get("key2"));
    }

    @Test
    @DisplayName("Should set and get sets")
    void testSetAndGetSets() {
        List<String> sets = new ArrayList<>();
        sets.add("set1");
        sets.add("set2");
        
        network.setSets(sets);
        
        assertNotNull(network.getSets());
        assertEquals(2, network.getSets().size());
        assertTrue(network.getSets().contains("set1"));
        assertTrue(network.getSets().contains("set2"));
    }

    @Test
    @DisplayName("Should set and get properties")
    void testSetAndGetProperties() {
        Map<String, Boolean> properties = new HashMap<>();
        properties.put("enabled", true);
        properties.put("active", false);
        
        network.setProperties(properties);
        
        assertNotNull(network.getProperties());
        assertEquals(2, network.getProperties().size());
        assertTrue(network.getProperties().get("enabled"));
        assertFalse(network.getProperties().get("active"));
    }

    @Test
    @DisplayName("Should set and get snapshots")
    void testSetAndGetSnapshots() {
        Collection<NetworkSnapshot> snapshots = new LinkedHashSet<>();
        NetworkSnapshot snapshot1 = new NetworkSnapshot();
        NetworkSnapshot snapshot2 = new NetworkSnapshot();
        snapshots.add(snapshot1);
        snapshots.add(snapshot2);
        
        network.setSnapshots(snapshots);
        
        assertNotNull(network.getSnapshots());
        assertEquals(2, network.getSnapshots().size());
    }

    @Test
    @DisplayName("Should set and get schedule cron expression")
    void testSetAndGetScheduleCronExpression() {
        String cronExpression = "0 0 * * * ?";
        network.setScheduleCronExpression(cronExpression);
        assertEquals(cronExpression, network.getScheduleCronExpression());
    }

    @Test
    @DisplayName("Should set and get prevalidator")
    void testSetAndGetPrevalidator() {
        Validator prevalidator = new Validator();
        network.setPrevalidator(prevalidator);
        assertNotNull(network.getPrevalidator());
        assertEquals(prevalidator, network.getPrevalidator());
    }

    @Test
    @DisplayName("Should set and get validator")
    void testSetAndGetValidator() {
        Validator validator = new Validator();
        network.setValidator(validator);
        assertNotNull(network.getValidator());
        assertEquals(validator, network.getValidator());
    }

    @Test
    @DisplayName("Should set and get transformer")
    void testSetAndGetTransformer() {
        Transformer transformer = new Transformer();
        network.setTransformer(transformer);
        assertNotNull(network.getTransformer());
        assertEquals(transformer, network.getTransformer());
    }

    @Test
    @DisplayName("Should set and get secondary transformer")
    void testSetAndGetSecondaryTransformer() {
        Transformer secondaryTransformer = new Transformer();
        network.setSecondaryTransformer(secondaryTransformer);
        assertNotNull(network.getSecondaryTransformer());
        assertEquals(secondaryTransformer, network.getSecondaryTransformer());
    }

    @Test
    @DisplayName("Should get boolean property value when property exists")
    void testGetBooleanPropertyValueExists() {
        Map<String, Boolean> properties = new HashMap<>();
        properties.put("testProperty", true);
        network.setProperties(properties);
        
        Boolean result = network.getBooleanPropertyValue("testProperty");
        
        assertTrue(result);
    }

    @Test
    @DisplayName("Should return false for non-existent boolean property")
    void testGetBooleanPropertyValueNotExists() {
        Map<String, Boolean> properties = new HashMap<>();
        network.setProperties(properties);
        
        Boolean result = network.getBooleanPropertyValue("nonExistent");
        
        assertFalse(result);
    }

    @Test
    @DisplayName("Should return false when properties is null")
    void testGetBooleanPropertyValueNullProperties() {
        network.setProperties(null);
        
        Boolean result = network.getBooleanPropertyValue("testProperty");
        
        assertFalse(result);
    }

    @Test
    @DisplayName("Should handle toString method")
    void testToString() {
        network.setAcronym("TEST");
        network.setName("Test Network");
        
        String result = network.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("Network"));
    }

    @Test
    @DisplayName("Should maintain state consistency")
    void testStateConsistency() {
        network.setAcronym("ABC");
        network.setName("ABC Network");
        network.setPublished(true);
        network.setMetadataPrefix("oai_dc");
        
        assertEquals("ABC", network.getAcronym());
        assertEquals("ABC Network", network.getName());
        assertTrue(network.getPublished());
        assertEquals("oai_dc", network.getMetadataPrefix());
    }

    @Test
    @DisplayName("Should handle empty collections")
    void testEmptyCollections() {
        network.setSets(new ArrayList<>());
        network.setSnapshots(new LinkedHashSet<>());
        network.setProperties(new HashMap<>());
        
        assertNotNull(network.getSets());
        assertNotNull(network.getSnapshots());
        assertNotNull(network.getProperties());
        assertTrue(network.getSets().isEmpty());
        assertTrue(network.getSnapshots().isEmpty());
        assertTrue(network.getProperties().isEmpty());
    }
}
