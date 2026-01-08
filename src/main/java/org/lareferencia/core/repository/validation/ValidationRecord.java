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

package org.lareferencia.core.repository.validation;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * DTO for validation record data in SQLite storage.
 * 
 * FIELDS:
 * - identifierHash: MD5 of identifier (PK, matches catalog.id)
 * - identifier: OAI identifier (denormalized for queries)
 * - datestamp: Last modification date
 * - isValid: Global validation result
 * - isTransformed: Whether record was transformed
 * - publishedMetadataHash: Hash of XML to index
 * - ruleResults: Map of ruleId -> validity (dynamic columns)
 */
@Data
@NoArgsConstructor
public class ValidationRecord {

    private String identifierHash;
    private String identifier;
    private LocalDateTime datestamp;
    private boolean valid;
    private boolean transformed;
    private String publishedMetadataHash;

    // Rule results: ruleId -> isValid
    private Map<Long, Boolean> ruleResults = new HashMap<>();

    /**
     * Full constructor.
     */
    public ValidationRecord(String identifierHash, String identifier, LocalDateTime datestamp,
            boolean valid, boolean transformed, String publishedMetadataHash,
            Map<Long, Boolean> ruleResults) {
        this.identifierHash = identifierHash;
        this.identifier = identifier;
        this.datestamp = datestamp;
        this.valid = valid;
        this.transformed = transformed;
        this.publishedMetadataHash = publishedMetadataHash;
        this.ruleResults = ruleResults != null ? ruleResults : new HashMap<>();
    }

    /**
     * Sets a rule result.
     */
    public void setRuleResult(Long ruleId, boolean isValid) {
        this.ruleResults.put(ruleId, isValid);
    }

    /**
     * Gets a rule result.
     */
    public Boolean getRuleResult(Long ruleId) {
        return this.ruleResults.get(ruleId);
    }
}
