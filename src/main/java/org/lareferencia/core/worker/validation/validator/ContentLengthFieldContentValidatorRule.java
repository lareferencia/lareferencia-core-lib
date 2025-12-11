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

import org.lareferencia.core.worker.validation.AbstractValidatorFieldContentRule;
import org.lareferencia.core.worker.validation.SchemaProperty;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

/**
 * Validator rule that checks field content length.
 * <p>
 * Validates that field values are within specified minimum and maximum
 * length constraints. Useful for ensuring data quality and preventing
 * excessively long or empty values.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractValidatorFieldContentRule
 */
@Getter
@Setter
@ValidatorRuleMeta(name = "Validación por longitud de contenido", help = "Esta regla es válida si el campo contiene ocurrencias de longitud entre un mínimo y un máximo")
public class ContentLengthFieldContentValidatorRule extends AbstractValidatorFieldContentRule {

	static int MAX_EXPECTED_LENGTH = 50;

	@SchemaProperty(title = "Longitud mínima", description = "La longitud mínima aceptada", order = 2)
	@JsonProperty("minLength")
	private Integer minLength = 0;

	@SchemaProperty(title = "Longitud máxima", description = "La longitud máxima aceptada", order = 3)
	@JsonProperty("maxLength")
	private Integer maxLength = Integer.MAX_VALUE;

	/**
	 * Creates a new content length validator with default length constraints.
	 */
	public ContentLengthFieldContentValidatorRule() {
	}

	@Override
	public ContentValidatorResult validate(String content) {

		ContentValidatorResult result = new ContentValidatorResult();

		if (content == null) {
			result.setReceivedValue("NULL");
			result.setValid(false);
		} else {

			result.setReceivedValue(
					content.length() > MAX_EXPECTED_LENGTH ? content.substring(0, MAX_EXPECTED_LENGTH) + "..."
							: content + " | " + new Integer(content.length()).toString());
			result.setValid(content.length() >= minLength && content.length() <= maxLength);
		}

		return result;
	}

	@Override
	public String toString() {
		return "ContentLengthValidationRule [minLength=" + minLength + ", maxLength=" + maxLength + ", id=" + ruleId
				+ ", mandatory=" + mandatory + ", quantifier=" + quantifier
				+ "]";
	}

}
