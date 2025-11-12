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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.ITransformer;
import org.lareferencia.core.worker.validation.ITransformerRule;
import org.lareferencia.core.worker.validation.ValidationException;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

/**
 * Default implementation of the ITransformer interface for metadata transformation.
 * Applies a sequence of transformation rules to record metadata, tracking whether
 * any transformations occurred and updating the datestamp accordingly.
 */
@Component
public class TransformerImpl implements ITransformer {
	
	private static Logger logger = LogManager.getLogger(TransformerImpl.class);


	@Getter
	@Setter
	List<ITransformerRule> rules;

	/**
	 * Constructs a new TransformerImpl with an empty list of transformation rules.
	 */
	public TransformerImpl() {
		super();
		rules = new ArrayList<ITransformerRule>();
	}

	/**
	 * Transforms record metadata by applying all configured transformation rules in sequence.
	 * If any transformation occurs, updates the metadata datestamp to the current time.
	 *
	 * @param record the OAI record being transformed
	 * @param metadata the record's metadata to transform
	 * @return true if any transformation rule modified the metadata, false otherwise
	 * @throws ValidationException if an error occurs during rule execution
	 */
	@Override
	public boolean transform(NetworkRunningContext networkContext, IOAIRecord record, OAIRecordMetadata metadata) throws ValidationException {

		boolean anyTransformationOccurred = false;

		for (ITransformerRule rule : rules) {

			try {
				logger.debug( "RecordID: " + record.getId() + "oai_id:" + record.getIdentifier() +  " rule::" + rule.getRuleId() + "::" + rule.getClass().getName() );
				anyTransformationOccurred |= rule.transform(networkContext,record, metadata);
				
			} catch (Exception | Error e) {
				logger.debug( e + e.getMessage() + "RecordID: " + record.getId() + "oai_id:" + record.getIdentifier() +  " rule"  + rule.getClass().getName()  );
				throw new ValidationException("Error loading transforming Record ID: " + record.getId() + " oai_id: " + record.getIdentifier() + " rule: " + rule.getRuleId()  + " class: " +  rule.getClass() + " :: " +  e.getMessage(), e);

			}
		}
		
		// if some transformation occurred then set datestamp to now
		if ( anyTransformationOccurred )
			metadata.setDatestamp( LocalDateTime.now() );

		return anyTransformationOccurred;
	}

}
