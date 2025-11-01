/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.lareferencia.backend.domain.parquet.SnapshotValidationStats;
import org.lareferencia.backend.validation.FacetFieldEntry;
import org.lareferencia.backend.validation.ValidationStatsResult;
import org.lareferencia.core.metadata.SnapshotMetadata;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotValidationStatsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testConversionToValidationStatsResult() throws Exception {
        // Create test data
        SnapshotMetadata metadata = new SnapshotMetadata();
        metadata.setSnapshotId(1L);
        metadata.setSize(100L);

        // Add rule definitions
        SnapshotMetadata.RuleDefinition ruleDef = new SnapshotMetadata.RuleDefinition();
        ruleDef.setName("Test Rule");
        ruleDef.setDescription("Test description");
        ruleDef.setQuantifier("ONE_OR_MORE");
        ruleDef.setMandatory(true);
        metadata.getRuleDefinitions().put(1L, ruleDef);

        // Create validation stats
        SnapshotValidationStats stats = new SnapshotValidationStats(metadata);
        stats.setTotalRecords(100L);
        stats.setTransformedRecords(10L);
        stats.setValidRecords(90L);

        // Update rule stats (rule was already registered in constructor from metadata)
        stats.incrementRuleValid(1L);
        stats.incrementRuleInvalid(1L);

        // Add facets
        stats.updateFacet("record_is_valid", "true");
        stats.updateFacet("record_is_valid", "false");

        // Convert to ValidationStatsResult
        ValidationStatsResult result = ValidationStatsResult.fromSnapshotValidationStats(stats);

        // Verify conversion
        assertEquals(100, result.getSize());
        assertEquals(10, result.getTransformedSize());
        assertEquals(90, result.getValidSize());
        
        // Verify rules
        assertNotNull(result.getRulesByID());
        assertTrue(result.getRulesByID().containsKey("1"));
        assertEquals("Test Rule", result.getRulesByID().get("1").getName());
        assertEquals(1, result.getRulesByID().get("1").getValidCount());
        assertEquals(Integer.valueOf(1), result.getRulesByID().get("1").getInvalidCount());
        
        // Verify facets
        assertNotNull(result.getFacets());
        assertTrue(result.getFacets().containsKey("record_is_valid"));
        List<FacetFieldEntry> facetEntries = result.getFacets().get("record_is_valid");
        assertEquals(2, facetEntries.size());
        
        // Check facet entries
        boolean foundTrue = false, foundFalse = false;
        for (FacetFieldEntry entry : facetEntries) {
            if ("true".equals(entry.getValue())) {
                assertEquals(1L, entry.getValueCount());
                foundTrue = true;
            } else if ("false".equals(entry.getValue())) {
                assertEquals(1L, entry.getValueCount());
                foundFalse = true;
            }
        }
        assertTrue(foundTrue && foundFalse);

        // Test JSON serialization of ValidationStatsResult
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
        
        // Verify JSON structure contains expected fields
        assertTrue(json.contains("\"size\" : 100"));
        assertTrue(json.contains("\"transformedSize\" : 10"));
        assertTrue(json.contains("\"validSize\" : 90"));
        assertTrue(json.contains("\"rulesByID\""));
        assertTrue(json.contains("\"facets\""));
    }
}