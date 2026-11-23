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

package org.lareferencia.core.worker.validation;

/**
 * Enumeration defining quantifier constraints for validation rules.
 * Specifies how many occurrences of a field are required or allowed during validation.
 */
public enum QuantifierValues {
    // Existing profiles store these ordinals.
    ZERO_ONLY,
    ONE_ONLY,
    ZERO_OR_MORE,
    ONE_OR_MORE,
    ALL;

    /** Counts matching values in one operand, before applying boolean operators. */
    public boolean matchesExpressionOperand(int matches, int occurrences) {
        return switch (this) {
            case ZERO_ONLY -> matches == 0;
            case ONE_ONLY -> matches == 1;
            case ZERO_OR_MORE -> true;
            case ONE_OR_MORE -> matches > 0;
            case ALL -> matches == occurrences;
        };
    }

    /** Optional content may be absent. If present, at least one value must pass. */
    public boolean matchesFieldContent(int matches, int occurrences) {
        if (this == ZERO_OR_MORE) {
            return occurrences == 0 || matches > 0;
        }
        return matchesExpressionOperand(matches, occurrences);
    }

    /** XPath counts nodes already selected by its predicate. Historically ALL requires a node. */
    public boolean matchesSelectedNodes(int nodes) {
        if (this == ALL) {
            return nodes > 0;
        }
        return matchesExpressionOperand(nodes, nodes);
    }
}
