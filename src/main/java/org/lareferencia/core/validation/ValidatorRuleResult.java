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

package org.lareferencia.core.validation;

import java.util.ArrayList;
import java.util.List;

import org.lareferencia.backend.validation.validator.ContentValidatorResult;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValidatorRuleResult {

	private Boolean valid;
	private IValidatorRule rule;

	private List<ContentValidatorResult> results;

	public ValidatorRuleResult() {
		results = new ArrayList<ContentValidatorResult>();
	}

	public ValidatorRuleResult(IValidatorRule rule, Boolean isValid, List<ContentValidatorResult> contentResults) {
		this.valid = isValid;
		this.results = contentResults;
		this.rule = rule;
	}

	@Override
	public String toString() {

		String toStr = "\t" + this.rule + "\n";

		for (ContentValidatorResult cr : results) {
			toStr += "\t" + cr.toString() + ":\n";
		}

		return toStr;
	}

}
