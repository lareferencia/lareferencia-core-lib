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

package org.lareferencia.backend.workers.validator;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.IValidator;
import org.lareferencia.core.validation.IValidatorRule;
import org.lareferencia.core.validation.ValidationException;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.validation.ValidatorRuleResult;

import lombok.Getter;
import lombok.Setter;

/**
 * Default implementation of the IValidator interface for metadata validation.
 * Applies a list of validation rules to metadata and aggregates results,
 * determining overall validity based on mandatory rule outcomes.
 */
@Getter
@Setter
public class ValidatorImpl implements IValidator {
	
	private static Logger logger = LogManager.getLogger(ValidatorImpl.class);

	List<IValidatorRule> rules;

	/**
	 * Constructs a new ValidatorImpl with an empty list of validation rules.
	 */
	public ValidatorImpl() {
		super();
		rules = new ArrayList<IValidatorRule>();
	}

	/**
	 * Validates metadata against all configured rules and populates the result.
	 * Record is considered valid if all mandatory rules pass; optional rules don't affect overall validity.
	 *
	 * @param metadata the metadata to validate
	 * @param reusableResult the result object to populate (will be reset before use)
	 * @return the validation result with individual rule outcomes and overall validity
	 * @throws ValidationException if an error occurs during rule execution
	 */
	public ValidatorResult validate(OAIRecordMetadata metadata, ValidatorResult reusableResult) throws ValidationException {
		
		// clean result
		reusableResult.reset();
		
		boolean isRecordValid = true;

		for (IValidatorRule rule : rules) {
			try {
				ValidatorRuleResult ruleResult = rule.validate(metadata);
				reusableResult.getRulesResults().add(ruleResult);
				isRecordValid &= (ruleResult.getValid() || !rule.getMandatory());
			}
			catch (Exception | Error e) {
				throw new ValidationException("Error validating metadata " +  metadata.getIdentifier() + " rule: " + rule.getRuleId()  + " class: " +  rule.getClass() + " :: " +  e.getMessage(), e);
			}	
		}

		reusableResult.setValid(isRecordValid);
		
		return reusableResult;

	}

}
