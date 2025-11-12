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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Result of field content validation.
 * <p>
 * Contains validation status and the value that was validated.
 * Used by field content validator rules to return validation outcomes.
 * </p>
 * 
 * @author LA Referencia Team
 * @see org.lareferencia.core.validation.AbstractValidatorFieldContentRule
 */
@Getter
@Setter
@ToString
public class ContentValidatorResult {

	private boolean valid;
	private String receivedValue;

	/**
	 * Creates an empty validation result.
	 */
	public ContentValidatorResult() {
	}

	/**
	 * Creates a validation result with the specified status and value.
	 * 
	 * @param valid whether the validation passed
	 * @param receivedValue the value that was validated
	 */
	public ContentValidatorResult(boolean valid, String receivedValue) {
		super();
		this.valid = valid;
		this.receivedValue = receivedValue;
	}
}
