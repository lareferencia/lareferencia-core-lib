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

import org.lareferencia.core.metadata.OAIRecordMetadata;

/**
 * Interface for metadata validation rules.
 * <p>
 * Defines the contract for rules that validate metadata records.
 * Validation rules check metadata quality, completeness, and conformance
 * to specific standards or requirements.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractValidatorRule
 */
public interface IValidatorRule {

	/**
	 * Validates the given metadata record.
	 * 
	 * @param metadata the metadata to validate
	 * @return the validation result containing status and details
	 */
	public ValidatorRuleResult validate(OAIRecordMetadata metadata);

	/**
	 * Gets the rule identifier.
	 * 
	 * @return the rule ID
	 */
	public Long getRuleId();

	/**
	 * Sets the rule identifier.
	 * 
	 * @param id the rule ID
	 */
	public void setRuleId(Long id);

	/**
	 * Checks if this rule is mandatory.
	 * 
	 * @return true if mandatory, false otherwise
	 */
	public Boolean getMandatory();

	/**
	 * Sets whether this rule is mandatory.
	 * 
	 * @param mandatory true to make the rule mandatory
	 */
	public void setMandatory(Boolean mandatory);

	/**
	 * Gets the quantifier for this rule.
	 * 
	 * @return the quantifier value
	 */
	public QuantifierValues getQuantifier();

	/**
	 * Sets the quantifier for this rule.
	 * 
	 * @param qv the quantifier value
	 */
	public void setQuantifier(QuantifierValues qv);

	/**
	 * Checks if this rule should store occurrence details for debugging/analysis.
	 * Default should be false for most rules to optimize memory usage.
	 * 
	 * @return true if occurrences should be stored, false otherwise
	 */
	public boolean isStoreOccurrences();

	/**
	 * Sets whether this rule should store occurrence details.
	 * 
	 * @param storeOccurrences true to store occurrence details
	 */
	public void setStoreOccurrences(boolean storeOccurrences);

}
