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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.ITransformer;
import org.lareferencia.core.validation.ITransformerRule;
import org.lareferencia.core.validation.ValidationException;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

@Component
public class TransformerImpl implements ITransformer {
	
	private static Logger logger = LogManager.getLogger(TransformerImpl.class);


	@Getter
	@Setter
	List<ITransformerRule> rules;

	public TransformerImpl() {
		super();
		rules = new ArrayList<ITransformerRule>();
	}

	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) throws ValidationException {

		boolean anyTransformationOccurred = false;

		for (ITransformerRule rule : rules) {

			try {
				logger.debug( "RecordID: " + record.getId() + "oai_id:" + record.getIdentifier() +  " rule::" + rule.getRuleId() + "::" + rule.getClass().getName() );
				anyTransformationOccurred |= rule.transform(record, metadata);
				
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
