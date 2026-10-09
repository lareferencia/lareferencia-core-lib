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

package org.lareferencia.core.worker.validation.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.lareferencia.core.worker.validation.SchemaProperty;

/** A term in the complete vocabulary, with its LA Referencia acceptance policy. */
@Getter
public class VocabularyTerm {
    @SchemaProperty(order = 1)
    private final String value;

    @SchemaProperty(order = 2, defaultValue = "false")
    private final boolean accepted;

    @JsonCreator
    public VocabularyTerm(@JsonProperty("value") String value,
                          @JsonProperty("accepted") Boolean accepted) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Vocabulary terms must have a nonblank value");
        }
        this.value = value;
        this.accepted = Boolean.TRUE.equals(accepted);
    }
}
