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

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.lareferencia.core.worker.validation.AbstractValidatorFieldContentRule;
import org.lareferencia.core.worker.validation.SchemaProperty;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Validator rule that checks if year values are within a dynamic range.
 * <p>
 * Validates that extracted year values from field content fall within
 * configured lower and upper limits. Supports regex-based year extraction.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractValidatorFieldContentRule
 */
@ToString(exclude = { "pattern" })
@ValidatorRuleMeta(name = "Validación de fechas por rango dinámico de años", help = "Esta regla es válida sólo si valor de año (year) capturado por la expresión regular cumple: year <= año actual - límite inferior Y year >= año actual + limite superior")
public class DynamicYearRangeFieldContentValidatorRule extends AbstractValidatorFieldContentRule {

	/**
	 * Creates a new dynamic year range validator.
	 */
	public DynamicYearRangeFieldContentValidatorRule() {
		super();
		if (regexString != null)
			this.pattern = Pattern.compile(regexString);

	}

	private static final int MAX_EXPECTED_LENGTH = 255;
	private static final String DEFAULT_REGEX = "^([0-9]{3,4})";

	@Getter
	@SchemaProperty(title = "Expresión regular", description = "Expresión regular que debe ser válida para la ocurrencia", uiType = "textarea", order = 2, defaultValue = "^([0-9]{3,4})")
	@JsonProperty("regexString")
	private String regexString = DEFAULT_REGEX;

	@Getter
	@Setter
	@SchemaProperty(title = "límite superior", description = "Cantidad de años a partir del año actual hacia adelante. Ej: valor=2, año actual=2019 => años <= 2021 válidos", order = 4, defaultValue = "2")
	@JsonProperty("upperLimit")
	private int upperLimit;

	@Getter
	@Setter
	@SchemaProperty(title = "límite inferior", description = "Cantidad de años a partir del año actual hacia atrás. Ej: valor=30, año actual=2019 => años >= 1989 válidos", order = 3, defaultValue = "100")
	@JsonProperty("lowerLimit")
	private int lowerLimit;

	private Pattern pattern;

	/**
	 * Sets the regex string for year extraction.
	 * 
	 * @param reString the regex pattern
	 */
	public void setRegexString(String reString) {
		this.regexString = reString;
		this.pattern = Pattern.compile(reString);
	}

	@Override
	public ContentValidatorResult validate(String content) {

		ContentValidatorResult result = new ContentValidatorResult();

		if (content == null || content.length() == 0) {

			result.setReceivedValue("NULL or Empty");
			result.setValid(false);

		} else {

			Matcher matcher = pattern.matcher(content);
			Boolean containsYear = matcher.find();

			// if regex found a match and is numeric
			if (containsYear && StringUtils.isNumeric(matcher.group())) {

				Integer year = Integer.parseInt(matcher.group());

				// find a dynamic valid range using lower and upper limits
				Date actualDate = new Date();
				LocalDate localDate = actualDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

				Integer startYear = localDate.getYear() - lowerLimit;
				Integer endYear = localDate.getYear() + upperLimit;

				result.setReceivedValue(year.toString());
				result.setValid(year >= startYear && year <= endYear);

			} else { // if not result is false
				result.setReceivedValue("Regex not parsing a valid year value");
				result.setValid(false);
			}

		}

		return result;
	}

}
