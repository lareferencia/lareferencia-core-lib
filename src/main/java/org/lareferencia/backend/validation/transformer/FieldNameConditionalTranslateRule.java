
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

package org.lareferencia.backend.validation.transformer;

import lombok.Getter;
import lombok.Setter;

import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.AbstractTransformerRule;
import org.w3c.dom.Node;

public class FieldNameConditionalTranslateRule extends AbstractTransformerRule {

	static int MAX_NODE_COUNT = 100;
	
	@Setter
	@Getter
	String targetFieldName;

    @Getter
    String sourceXPathExpression;

    public void setSourceXPathExpression(String regexPattern) {
        this.sourceXPathExpression = regexPattern;
        //regexPredicate = Pattern.compile(regexPattern).asPredicate();
    }

	@Override
	public boolean transform(OAIRecord record, OAIRecordMetadata metadata) {


		boolean wasTransformed = false;

		// ciclo de reemplazo
		// recorre las ocurrencias del campo de nombre source creando instancias
		// con nombre target
		int i = 0;
		for (Node node : metadata.getFieldNodesByXPath(this.getSourceXPathExpression())) {
			
			
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
