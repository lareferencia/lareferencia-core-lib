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

import java.util.ArrayList;
import java.util.List;

import org.lareferencia.core.validation.AbstractValidatorFieldContentRule;

import lombok.Getter;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Getter
@Setter
public class ControlledValueFieldContentValidatorRule extends AbstractValidatorFieldContentRule {

	private static final int MAX_EXPECTED_LENGTH = 100;

	protected List<String> controlledValues;

	public ControlledValueFieldContentValidatorRule() {
		super();
		this.controlledValues = new ArrayList<String>();
	}

	@Override
	public ContentValidatorResult validate(String content) {

		ContentValidatorResult result = new ContentValidatorResult();

		if (content == null) {
			result.setReceivedValue("NULL");
			result.setValid(false);
		} else {
			result.setReceivedValue(content.length() > MAX_EXPECTED_LENGTH ? content.substring(0, MAX_EXPECTED_LENGTH) + "..." : content);
			result.setValid(this.controlledValues.contains(content));
		}

		return result;
	}

	@Override
	public String toString() {
		return "ControlledValueContentValidationRule [controlledValues=" + controlledValues + ", id=" + ruleId + ", mandatory=" + mandatory + ", quantifier=" + quantifier + "]";
	}

}
