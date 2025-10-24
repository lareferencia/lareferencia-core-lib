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

package org.lareferencia.backend.validation;

/**
 * Auxiliary class to simulate Solr's FacetFieldEntry.
 * Represents a facet value with its occurrence count.
 */
public class FacetFieldEntry {
    private String value;
    private long valueCount;
    private FacetKey key;

    public FacetFieldEntry(String value, long valueCount, String keyName) {
        this.value = value;
        this.valueCount = valueCount;
        this.key = new FacetKey(keyName);
    }

    public String getValue() {
        return value;
    }

    public long getValueCount() {
        return valueCount;
    }
    
    public FacetKey getKey() {
        return key;
    }
}
