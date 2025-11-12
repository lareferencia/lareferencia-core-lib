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

import java.util.List;

import org.lareferencia.core.metadata.OAIRecordMetadata;

/**
 * Interface for metadata validators.
 * <p>
 * Validators apply a list of validation rules to metadata records
 * and aggregate the results.
 * </p>
 * 
 * @author LA Referencia Team
 */
public interface IValidator {

	/**
	 * Validates the metadata using configured rules.
	 * 
	 * @param metadata the metadata to validate
	 * @param result the result object to populate
	 * @return the validation result
	 * @throws ValidationException if validation fails
	 */
	public ValidatorResult validate(OAIRecordMetadata metadata, ValidatorResult result) throws ValidationException;

	/**
	 * Gets the list of validation rules.
	 * 
	 * @return the list of rules
	 */
	public List<IValidatorRule> getRules();

	/**
	 * Sets the list of validation rules.
	 * 
	 * @param rules the list of rules to apply
	 */
	public void setRules(List<IValidatorRule> rules);
}
