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

import java.util.ArrayList;
import java.util.List;

import org.lareferencia.core.metadata.OAIMetadataElement.Type;

/**
 * Helper class for building XPath expressions for XOAI metadata elements.
 * Provides utilities to convert field names into XPath queries for metadata extraction.
 */
public class XOAIXPATHHelper {

	private static String XPATH_XOAI_RECORD_ROOT = "/*[local-name()='metadata']";
	private static String ELEMENT_SEPARATOR = "\\.";
	private static String FIELD_SEPARATOR = ":";

	/**
	 * Constructs a new XOAIXPATHHelper instance.
	 */
	public XOAIXPATHHelper() {
	}
	
	/**
	 * Gets the root XPath for XOAI records.
	 *
	 * @return the root XPath expression
	 */
	public static String getRootXPATH() {
		return XPATH_XOAI_RECORD_ROOT;
	}
	
	/**
	 * Generates an XPath expression for the given field name.
	 *
	 * @param fieldName the field name to convert to XPath
	 * @return the XPath expression
	 */
	public static String getXPATH(String fieldName) {
		return getXPATH(fieldName,true,true);
	}

	/**
	 * Generates an XPath expression for the given field name with options.
	 *
	 * @param fieldName the field name to convert to XPath
	 * @param includeFieldNodes whether to include field nodes in the result
	 * @param includeDocumentRoot whether to include the document root in the result
	 * @return the XPath expression
	 */
	public static String getXPATH(String fieldName, Boolean includeFieldNodes, Boolean includeDocumentRoot) {
		
		List<OAIMetadataElement> elements = getXPATHList(fieldName, false, includeDocumentRoot );
		
		StringBuffer stringBuffer = new StringBuffer();
		
		for (OAIMetadataElement elem:elements) {
			
			if ( elem.type != Type.field || includeFieldNodes )
				stringBuffer.append( elem.getXpath() );
		}
		
		return stringBuffer.toString();
	}
	
	/**
	 * Gets a list of OAI metadata elements representing the XPath components.
	 *
	 * @param fieldName the field name to parse
	 * @return a list of OAI metadata elements
	 */
	public static List<OAIMetadataElement> getXPATHList(String fieldName) {
		return getXPATHList(fieldName,true,true);
	}
	
	/**
	 * Gets a list of OAI metadata elements representing the XPath components with options.
	 *
	 * @param fieldName the field name to parse
	 * @param fullXPATHForEachElement whether to generate full XPath for each element
	 * @param includeDocumentRoot whether to include the document root
	 * @return a list of OAI metadata elements
	 */
	private static List<OAIMetadataElement> getXPATHList(String fieldName, boolean fullXPATHForEachElement, boolean includeDocumentRoot) {
		
		
		String[] elementParts = fieldName.split(ELEMENT_SEPARATOR);
		int lastElementIndex = elementParts.length-1;
		String field = null;
		
		String xpath = "/";
		
		if ( includeDocumentRoot )
			xpath = XPATH_XOAI_RECORD_ROOT;
		
		
		List<OAIMetadataElement> result = new ArrayList<>(elementParts.length + 1);
		
		
		if ( !fieldName.equals("") ) { // only if fieldName != ""
		
			/// Parsing of the field expression: dc.type:field
			if ( elementParts[lastElementIndex].contains(FIELD_SEPARATOR) ) {
				
					String[] fieldParts = elementParts[lastElementIndex].split(FIELD_SEPARATOR);
					elementParts[lastElementIndex] = fieldParts[0];
					field = fieldParts[1];
			}
			
			for (int i=0; i<elementParts.length; i++) {
	
				OAIMetadataElement elem = new OAIMetadataElement();
				String name = elem.name = elementParts[i];
				
				elem.type = OAIMetadataElement.Type.element;
			
				if ( elem.name.equals("*") )
					elem.xpath = xpath += "//*[local-name()='element']";
				
				// en caso de terminar con $ trunca el proceso y devuelve el xpath de esos elementos sin fields
				else if ( elem.name.equals("$") ) {
					elem.xpath = xpath; //+= "/*";
					result.add(elem);
					return result;
				}	
				else
					elem.xpath = xpath += "/*[local-name()='element' and @name='" + name  +"']";
	
				result.add( elem );
				
				// reset xpath if not full xpath is true
				if (!fullXPATHForEachElement)
					xpath = "";
	
			}
		} // end of if fieldName != ""
		
		OAIMetadataElement elem = new OAIMetadataElement();
		elem.type = OAIMetadataElement.Type.field;
		
		if ( field != null ) {
			// case -- dc.type:value	
			elem.name = field;
			elem.xpath =  xpath + "/*[local-name()='field' and @name='" + field  +"']";
		}
		else {
			// case -- dc.type
			elem.name = "value";
			elem.xpath =  xpath + "/*[local-name()='field' and @name='value']";	
		}
			
		result.add(elem);
			 
		return result;
	}
	
	
	
	
	

}
