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
	/**
	 * Field must have exactly zero occurrences (must be absent).
	 */
	ZERO_ONLY,
	
	/**
	 * Field must have exactly one occurrence.
	 */
	ONE_ONLY,
	
	/**
	 * Field can have zero or more occurrences (optional, repeatable).
	 */
	ZERO_OR_MORE,
	
	/**
	 * Field must have one or more occurrences (required, repeatable).
	 */
	ONE_OR_MORE,
	
	/**
	 * Applies to all occurrences regardless of count.
	 */
	ALL
}
