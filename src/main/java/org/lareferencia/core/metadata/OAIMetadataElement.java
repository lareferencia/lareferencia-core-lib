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


import lombok.Getter;
import lombok.Setter;

/**
 * Represents a metadata element definition with name, type, and XPath expression.
 * Used to define metadata schema structure for OAI records.
 */
@Getter
@Setter
public class OAIMetadataElement {
	
	/**
	 * Constructs a new OAI metadata element with default values.
	 */
	public OAIMetadataElement() {
	}
	
	/**
	 * The name of this metadata element.
	 */
	String name;
	
	/**
	 * The type of this metadata element (field or element).
	 */
	Type type;
	
	/**
	 * The XPath expression to locate this element in the metadata document.
	 */
	String xpath;
	
	
	/**
	 * Enumeration of possible metadata element types.
	 */
	public enum Type {
		/** A field element containing text content */
		element, 
		/** A field element with specific data */
		field
	}

}


