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

import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * RULE FACT: Detail of a rule applied to a record.
 * 
 * Used for detailed diagnostics in the API response.
 */
@Getter
@Setter
@EqualsAndHashCode
public class RuleFact {

    private Integer ruleId;
    private List<String> validOccurrences;
    private List<String> invalidOccurrences;
    private Boolean isValid;

    public RuleFact() {
    }

    public RuleFact(Integer ruleId, List<String> validOccurrences,
            List<String> invalidOccurrences, Boolean isValid) {
        this.ruleId = ruleId;
        this.validOccurrences = validOccurrences;
        this.invalidOccurrences = invalidOccurrences;
        this.isValid = isValid;
    }
}
