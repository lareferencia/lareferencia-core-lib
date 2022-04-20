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

package org.lareferencia.backend.validation.validator;

import java.util.regex.Pattern;

import org.lareferencia.core.validation.AbstractValidatorFieldContentRule;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(exclude = { "pattern" })
public class RegexFieldContentValidatorRule extends AbstractValidatorFieldContentRule {

	private static final int MAX_EXPECTED_LENGTH = 100;

	@Getter
	@JsonProperty("regexString")
	private String regexString;

	private Pattern pattern;

	public void setRegexString(String reString) {
		this.regexString = reString;
		this.pattern = Pattern.compile(reString);
	}

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
