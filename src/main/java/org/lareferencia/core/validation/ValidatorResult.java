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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.lareferencia.backend.validation.validator.ContentValidatorResult;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValidatorResult {

	private boolean valid;
	private List<ValidatorRuleResult> rulesResults = new ArrayList<ValidatorRuleResult>();

	public ValidatorResult() {
		rulesResults = new ArrayList<ValidatorRuleResult>();
	}

	public String getValidationContentDetails() {

		StringBuilder sb = new StringBuilder();

		for (ValidatorRuleResult entry : rulesResults) {

			for (ContentValidatorResult result : entry.getResults()) {
				// Solo detalla los valores inválidos o válidos, según el caso
				sb.append(entry.getRule().getRuleId() + ":" + result.getReceivedValue());

				sb.append(";");
			}
		}

		if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ';')
			sb.deleteCharAt(sb.length() - 1);

		return sb.toString();
	}

	
	public void reset() {
		valid = false;
		rulesResults.clear();
	}
	
	@Override
	public String toString() {

		String toStr = "Validation: ";
		toStr += " record valid=" + valid + "\n\n";

		for (ValidatorRuleResult entry : rulesResults) {

			toStr += entry.getRule().getRuleId() + ":\n";
			toStr += entry.toString() + "\n\n";
		}
		return toStr;
	}
	
	

}
