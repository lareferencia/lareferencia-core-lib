
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
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Transformation rule that removes whitespace from field values.
 * <p>
 * Trims leading and trailing whitespace from field occurrences.
 * </p>
 * 
 * @author LA Referencia Team
 * @see AbstractTransformerRule
 */
public class FieldContentRemoveWhiteSpacesTranslateRule extends AbstractTransformerRule {

	/**
	 * Name of the field to process for removing whitespace.
	 */
    @Setter
    @Getter
    @JsonProperty("fieldName")
    String fieldName;

	/**
	 * Constructs a new field content whitespace removal rule.
	 */
	public FieldContentRemoveWhiteSpacesTranslateRule() {
		super();
	}

    @Override
    public String toString() {
        return "FieldContentRemoveWhiteSpacesTranslateRule [fieldName=" + fieldName + "]";
    }

    String occr = null;
    String replace = null;

    @Override
    public boolean transform(SnapshotMetadata snapshotMetadata, IOAIRecord record, OAIRecordMetadata metadata) {

        boolean wasTransformed = false;

        for (Node node : metadata.getFieldNodes(fieldName)) {

            occr = node.getFirstChild().getNodeValue();

            int originalSize = occr.length();
            //Replace every whitespace with nothing (removing whitespaces)
            replace = occr.replaceAll("\\s", "");

            node.getFirstChild().setNodeValue(replace);

            if (replace != null && originalSize != replace.length()) {
                // si entra al ciclo al menos una vez entonces transformó
                wasTransformed = true;
            }

        }

        return wasTransformed;
    }

}
