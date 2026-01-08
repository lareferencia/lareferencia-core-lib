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

package org.lareferencia.core.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.NetworkSnapshot;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

/**
 * Simple class to accumulate snapshot validation metadata.
 * Contains general snapshot information and references to validation
 * statistics.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotMetadata {

    private Long snapshotId;

    private Integer size;
    private Integer transformedSize;
    private Integer validSize;

    private Long createdAt;

    // Network info (lightweight copy to avoid lazy loading issues)
    private NetworkInfo network;

    // Rule definitions: ruleID -> rule information
    private Map<Long, RuleDefinition> ruleDefinitions = new LinkedHashMap<>();

    /**
     * Constructor que crea SnapshotMetadata directamente desde NetworkSnapshot.
     * 
     * @param snapshot el snapshot desde el cual construir la metadata
     */
    public SnapshotMetadata(NetworkSnapshot snapshot) {
        this.snapshotId = snapshot.getId();
        this.size = snapshot.getSize() != null ? snapshot.getSize() : 0;
        this.transformedSize = snapshot.getTransformedSize();
        this.validSize = snapshot.getValidSize();

        // Timestamp de creación
        if (snapshot.getStartTime() != null) {
            this.createdAt = snapshot.getStartTime().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        } else {
            this.createdAt = System.currentTimeMillis();
        }

        // Network info (copy essential fields only)
        Network net = snapshot.getNetwork();
        if (net != null) {
            this.network = new NetworkInfo(net);
        }

        // Rule definitions desde el validator
        this.ruleDefinitions = new LinkedHashMap<>();
        if (snapshot.getNetwork() != null && snapshot.getNetwork().getValidator() != null) {
            snapshot.getNetwork().getValidator().getRules().forEach(rule -> {
                this.ruleDefinitions.put(rule.getId(),
                        new RuleDefinition(
                                rule.getId(),
                                rule.getName(),
                                rule.getDescription(),
                                rule.getQuantifier().name(),
                                rule.getMandatory()));
            });
        }
    }

    public SnapshotMetadata(Long snapshotId) {
        this.snapshotId = snapshotId;
        this.size = 0;
        this.transformedSize = 0;
        this.validSize = 0;
        this.createdAt = System.currentTimeMillis();
    }

    /**
     * Inner class for network information (lightweight copy).
     * Only contains fields needed for serialization, without lazy-loaded
     * collections.
     */
    @Data
    @NoArgsConstructor
    public static class NetworkInfo {
        private Long id;
        private String acronym;
        private String name;
        private String institutionName;
        private String institutionAcronym;
        private String originURL;
        private String metadataPrefix;
        private Map<String, Object> attributes;

        public NetworkInfo(Network network) {
            this.id = network.getId();
            this.acronym = network.getAcronym();
            this.name = network.getName();
            this.institutionName = network.getInstitutionName();
            this.institutionAcronym = network.getInstitutionAcronym();
            this.originURL = network.getOriginURL();
            this.metadataPrefix = network.getMetadataPrefix();
            // Copy attributes map (safe - not a lazy collection)
            this.attributes = network.getAttributes() != null
                    ? new java.util.HashMap<>(network.getAttributes())
                    : new java.util.HashMap<>();
        }
    }

    /**
     * Inner class for rule definition/metadata (immutable once created)
     */
    @Data
    @NoArgsConstructor
    public static class RuleDefinition {
        private Long ruleID;
        private String name;
        private String description;
        private String quantifier;
        private Boolean mandatory;

        public RuleDefinition(Long ruleID, String name, String description, String quantifier, Boolean mandatory) {
            this.ruleID = ruleID;
            this.name = name;
            this.description = description;
            this.quantifier = quantifier;
            this.mandatory = mandatory;
        }
    }

    // Rule management methods
    public void registerRule(Long ruleID, String name, String description, String quantifier, Boolean mandatory) {
        if (!ruleDefinitions.containsKey(ruleID)) {
            ruleDefinitions.put(ruleID, new RuleDefinition(ruleID, name, description, quantifier, mandatory));
        }
    }

    public RuleDefinition getRuleDefinition(Long ruleID) {
        return ruleDefinitions.get(ruleID);
    }

    /**
     * Convenience setter that accepts a Network entity and converts it to
     * NetworkInfo.
     * Useful for backward compatibility with tests and legacy code.
     * 
     * @param network the Network entity to convert
     */
    public void setNetwork(Network network) {
        this.network = network != null ? new NetworkInfo(network) : null;
    }

    // Calculated field for invalid records - not serialized to avoid
    // deserialization issues
    @JsonIgnore
    public int getInvalidSize() {
        return (size != null && validSize != null) ? size - validSize : 0;
    }
}
