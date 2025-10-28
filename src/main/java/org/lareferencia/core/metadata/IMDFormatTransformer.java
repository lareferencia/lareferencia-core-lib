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

package org.lareferencia.core.metadata;

import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Interface for metadata format transformation.
 * <p>
 * Transforms metadata documents from one format to another (e.g., oai_dc to mods).
 * </p>
 * 
 * @author LA Referencia Team
 */
public interface IMDFormatTransformer {

	/**
	 * Transforms a metadata document to the target format.
	 * 
	 * @param source the source document
	 * @return the transformed document
	 * @throws MDFormatTranformationException if transformation fails
	 */
	Document transform(Document source) throws MDFormatTranformationException;
	
	/**
	 * Transforms a metadata document to string representation.
	 * 
	 * @param source the source document
	 * @return the transformed document as string
	 * @throws MDFormatTranformationException if transformation fails
	 */
	String   transformToString(Document source) throws MDFormatTranformationException;
	
	/**
	 * Sets a transformation parameter with multiple values.
	 * 
	 * @param name the parameter name
	 * @param values the parameter values
	 */
	void setParameter(String name, List<String> values);
	
	/**
	 * Sets a transformation parameter with a single value.
	 * 
	 * @param name the parameter name
	 * @param value the parameter value
	 */
	void setParameter(String name, String value);

	/**
	 * Gets the source metadata format identifier.
	 * 
	 * @return the source format name
	 */
	String getSourceMDFormat();
	
	/**
	 * Gets the target metadata format identifier.
	 * 
	 * @return the target format name
	 */
	String getTargetMDFormat();

}
