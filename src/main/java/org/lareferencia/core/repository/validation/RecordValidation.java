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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * DTO representing a validation record result.
 * Used for API responses and detailed diagnostics.
 */
public class RecordValidation {

    private String identifier;

    /**
     * Cache of recordId (MD5 of identifier).
     */
    private transient String cachedRecordId;

    /**
     * Datestamp of the record.
     */
    private LocalDateTime datestamp;

    private Boolean recordIsValid;
    private Boolean isTransformed;

    /**
     * MD5 Hash of the XML to index.
     */
    private String publishedMetadataHash;

    private List<RuleFact> ruleFacts;

    public RecordValidation() {
        this.ruleFacts = new ArrayList<>();
    }

    public RecordValidation(String identifier,
            Boolean recordIsValid, Boolean isTransformed) {
        this.identifier = identifier;
        this.recordIsValid = recordIsValid;
        this.isTransformed = isTransformed;
        this.ruleFacts = new ArrayList<>();
    }

    public RecordValidation(String identifier,
            Boolean recordIsValid, Boolean isTransformed, List<RuleFact> ruleFacts) {
        this.identifier = identifier;
        this.recordIsValid = recordIsValid;
        this.isTransformed = isTransformed;
        this.ruleFacts = ruleFacts != null ? ruleFacts : new ArrayList<>();
    }

    public RecordValidation(String identifier,
            LocalDateTime datestamp,
            Boolean recordIsValid, Boolean isTransformed,
            String publishedMetadataHash,
            List<RuleFact> ruleFacts) {
        this.identifier = identifier;
        this.datestamp = datestamp;
        this.recordIsValid = recordIsValid;
        this.isTransformed = isTransformed;
        this.publishedMetadataHash = publishedMetadataHash;
        this.ruleFacts = ruleFacts != null ? ruleFacts : new ArrayList<>();
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
        this.cachedRecordId = null;
    }

    public String getRecordId() {
        if (this.identifier == null)
            return null;
        if (this.cachedRecordId == null) {
            this.cachedRecordId = org.lareferencia.core.repository.catalog.OAIRecord
                    .generateIdFromIdentifier(this.identifier);
        }
        return this.cachedRecordId;
    }

    public LocalDateTime getDatestamp() {
        return datestamp;
    }

    public void setDatestamp(LocalDateTime datestamp) {
        this.datestamp = datestamp;
    }

    public Boolean getRecordIsValid() {
        return recordIsValid;
    }

    public void setRecordIsValid(Boolean recordIsValid) {
        this.recordIsValid = recordIsValid;
    }

    public Boolean getIsTransformed() {
        return isTransformed;
    }

    public void setIsTransformed(Boolean isTransformed) {
        this.isTransformed = isTransformed;
    }

    public String getPublishedMetadataHash() {
        return publishedMetadataHash;
    }

    public void setPublishedMetadataHash(String publishedMetadataHash) {
        this.publishedMetadataHash = publishedMetadataHash;
    }

    public List<RuleFact> getRuleFacts() {
        return ruleFacts;
    }

    public void setRuleFacts(List<RuleFact> ruleFacts) {
        this.ruleFacts = ruleFacts;
    }

    public void addRuleFact(RuleFact ruleFact) {
        if (this.ruleFacts == null) {
            this.ruleFacts = new ArrayList<>();
        }
        this.ruleFacts.add(ruleFact);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RecordValidation that = (RecordValidation) o;
        return Objects.equals(identifier, that.identifier) &&
                Objects.equals(datestamp, that.datestamp) &&
                Objects.equals(recordIsValid, that.recordIsValid) &&
                Objects.equals(isTransformed, that.isTransformed) &&
                Objects.equals(publishedMetadataHash, that.publishedMetadataHash) &&
                Objects.equals(ruleFacts, that.ruleFacts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, datestamp, recordIsValid, isTransformed,
                publishedMetadataHash, ruleFacts);
    }

    @Override
    public String toString() {
        return "RecordValidation{" +
                "recordId='" + getRecordId() + '\'' +
                ", identifier='" + identifier + '\'' +
                ", datestamp=" + datestamp +
                ", recordIsValid=" + recordIsValid +
                ", isTransformed=" + isTransformed +
                ", publishedMetadataHash='" + publishedMetadataHash + '\'' +
                ", ruleFacts=" + (ruleFacts != null ? ruleFacts.size() : 0) + " facts" +
                '}';
    }
}
