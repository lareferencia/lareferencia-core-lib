
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
import org.w3c.dom.Node;

/**
 * Transformation rule that translates field names from source to target.
 * <p>
 * Renames all occurrences of the source field to the target field name.
 * </p>
 * 
 * @author LA Referencia Team
 */
public class FieldNameTranslateRule extends AbstractTransformerRule {

	static int MAX_NODE_COUNT = 100;
	
	@Setter
	@Getter
	String sourceFieldName;

	@Setter
	@Getter
	String targetFieldName;

	/**
	 * Creates a new field name translation rule.
	 */
	public FieldNameTranslateRule() {
	}

	/**
	 * Transforms the record by renaming fields from source to target.
	 * 
	 * @param record the OAI record to transform
	 * @param metadata the metadata to transform
	 * @return true if any field name was translated, false otherwise
	 */
	@Override
	public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {


		boolean wasTransformed = false;

		// ciclo de reemplazo
		// recorre las ocurrencias del campo de nombre source creando instancias
		// con nombre target
		int i = 0;
		for (Node node : metadata.getFieldNodes(this.getSourceFieldName())) {
			
			
			//System.out.println(  metadata.toString() );

			// Agrega instancia target con el contenido a reemplazar
			String occr = node.getFirstChild().getNodeValue();
			metadata.addFieldOcurrence(this.getTargetFieldName(), occr);
			
			//System.out.println(  metadata.toString() );


			// Remueve la actual
			metadata.removeNode(node);

			//System.out.println(  metadata.toString() );

			// si entra al ciclo al menos una vez entonces transformó
			wasTransformed = true;
			
			i++; if ( i > MAX_NODE_COUNT ) break;
		}

		return wasTransformed;
	}

}
