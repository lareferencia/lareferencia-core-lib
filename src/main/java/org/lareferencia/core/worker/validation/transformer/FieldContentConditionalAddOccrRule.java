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

package org.lareferencia.core.worker.validation.transformer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.core.worker.validation.validator.FieldExpressionEvaluator;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.lareferencia.core.worker.validation.QuantifierValues;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Transformation rule that conditionally adds field occurrences based on an expression.
 * <p>
 * Evaluates a conditional expression against metadata and adds a field value if true.
 * Can optionally remove duplicated occurrences after adding.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 */
public class FieldContentConditionalAddOccrRule extends AbstractTransformerRule {

	/** Expression evaluator for conditional logic. */
	FieldExpressionEvaluator evaluator;
	
	/** Name of the field to which the value will be added. */
	@Setter
	@Getter
	@JsonProperty("fieldName")
	String fieldName;
	
	/** Value to be added to the field when condition is met. */
	@Setter
	@Getter
	@JsonProperty("valueToAdd")
	String valueToAdd;
	
	/** Conditional expression to evaluate for adding the field occurrence. */
	@Setter
	@Getter
	@JsonProperty("conditionalExpression")
	private String conditionalExpression;
	
	/** Default quantifier for expression evaluation. */
	@JsonIgnore
	protected QuantifierValues quantifier = QuantifierValues.ONE_OR_MORE;

	/**
	 * Creates a new conditional add rule.
	 */
	public FieldContentConditionalAddOccrRule() {
		evaluator = new FieldExpressionEvaluator(this.quantifier);
	}

	/**
	 * Flag to remove duplicate occurrences after adding.
	 */
	@Setter
	@Getter
	@JsonProperty("removeDuplicatedOccurrences")
	private Boolean removeDuplicatedOccurrences = false;



	/**
	 * Transforms a record by conditionally adding a field occurrence.
	 * <p>
	 * Evaluates the conditional expression and adds the specified value if true.
	 * Optionally removes duplicate occurrences after adding.
	 * </p>
	 * 
	 * @param record the OAI record to transform
	 * @param metadata the record metadata to modify
	 * @return true if the field was added, false otherwise
	 */
	@Override
	public boolean transform(NetworkRunningContext context, IOAIRecord record, OAIRecordMetadata metadata) {

		boolean isExpressionValid = evaluator.evaluate(conditionalExpression, metadata);		
		boolean wasTransformed = false;
		
		
		if ( isExpressionValid ) {
			wasTransformed = true;
			metadata.addFieldOcurrence(fieldName, valueToAdd);
		}
		
		
		Set<String> occrSet  = new HashSet<String>();
		List<Node> removeList = new ArrayList<Node>();
		
		if ( removeDuplicatedOccurrences ) {

			// recorre las ocurrencias del campo de test
			for ( Node node : metadata.getFieldNodes(fieldName) ) {
	
				String occr = node.getFirstChild().getNodeValue();
	
				if ( occrSet.contains(occr) )
					removeList.add(node);
				else
					occrSet.add(occr);
			}
			
			for (Node node: removeList) 
				metadata.removeNode(node);
		}

		return wasTransformed;
	}

	

}
