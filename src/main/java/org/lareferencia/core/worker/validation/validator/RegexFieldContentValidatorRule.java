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

import java.util.regex.Pattern;

import org.lareferencia.core.worker.validation.AbstractValidatorFieldContentRule;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Validator rule that validates field content against a regular expression pattern.
 * Checks if the content matches the specified regex pattern.
 */
@ToString(exclude = { "pattern" })
public class RegexFieldContentValidatorRule extends AbstractValidatorFieldContentRule {

	private static final int MAX_EXPECTED_LENGTH = 100;

	/**
	 * Constructs a new regex field content validator rule.
	 */
	public RegexFieldContentValidatorRule() {
		super();
	}

	/**
	 * The regular expression string used for validation.
	 */
	@Getter
	@JsonProperty("regexString")
	private String regexString;

	/**
	 * The compiled regex pattern for efficient matching.
	 */
	private Pattern pattern;

	/**
	 * Sets the regular expression string and compiles it into a pattern.
	 * 
	 * @param reString the regex string to use for validation
	 */
	public void setRegexString(String reString) {
		this.regexString = reString;
		this.pattern = Pattern.compile(reString);
	}

	/**
	 * Validates the content against the configured regular expression pattern.
	 * <p>
	 * Returns a result indicating whether the content matches the pattern.
	 * Null content is treated as invalid. Long content is truncated for display.
	 * </p>
	 * 
	 * @param content the content string to validate
	 * @return the validation result with match status
	 */
	@Override
	public ContentValidatorResult validate(String content) {

		ContentValidatorResult result = new ContentValidatorResult();

		if (content == null) {
			result.setReceivedValue("NULL");
			result.setValid(false);
		} else {
			result.setReceivedValue(content.length() > MAX_EXPECTED_LENGTH ? content.substring(0, MAX_EXPECTED_LENGTH) + "..." : content);
			result.setValid(pattern.matcher(content).matches());
		}

		return result;
	}

}
