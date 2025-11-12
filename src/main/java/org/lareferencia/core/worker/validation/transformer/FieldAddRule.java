
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

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;

/**
 * Transformation rule that adds a new field occurrence with a specified value.
 * <p>
 * This simple rule adds a static value to a target field in the metadata.
 * It can be used to inject constant values, labels, or identifiers into records.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 */
public class FieldAddRule extends AbstractTransformerRule {

	@Setter
	@Getter
	String targetFieldName;

	@Setter
	@Getter
	String value;

	/**
	 * Creates a new field addition rule.
	 */
	public FieldAddRule() {
		
	}


	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

		boolean wasTransformed = false;

		metadata.addFieldOcurrence(this.getTargetFieldName(),this.getValue());

		wasTransformed = true;

		return wasTransformed;
	}

}
