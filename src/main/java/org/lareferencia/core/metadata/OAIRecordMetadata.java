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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.OAIMetadataElement.Type;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Represents OAI record metadata with DOM-based XML manipulation capabilities.
 * Provides methods for querying, modifying, and transforming metadata fields using XPath expressions.
 */
public class OAIRecordMetadata {
	
	private static Logger logger = LogManager.getLogger(OAIRecordMetadata.class);
	
	/**
	 * XPath expression representing the identity metadata (the entire document).
	 */
	public static String IDENTITY_METADATA_EXPRESION = ".";
	
	/**
	 * The internal DOM document representation of the metadata.
	 */
	private Document DOMDocument;

	/**
	 * The unique identifier for this OAI record.
	 */
	@Getter
	private String identifier;
	
	/**
	 * The timestamp when this record was created or last modified.
	 */
	@Getter
	@Setter
	private LocalDateTime datestamp;

	/**
	 * The origin source of this metadata record.
	 */
	@Getter
	@Setter
	private String origin;

	/**
	 * The OAI set specification this record belongs to.
	 */
	@Getter
	@Setter
	private String setSpec;
	
	/**
	 * The storage schema used for this metadata.
	 */
	@Getter
	@Setter
	private String storeSchema;

	private static final String EMPTY_DOCUMENT = "<metadata></metadata>";

	/**
	 * Gets the DOM document representation of this metadata.
	 * 
	 * @return the DOM document
	 */
	public Document getDOMDocument() {
		return DOMDocument;
	}

	/**
	 * Constructs OAI record metadata from an XML string.
	 * 
	 * @param identifier the unique identifier for this record
	 * @param xmlString the XML metadata content
	 * @throws OAIRecordMetadataParseException if XML parsing fails
	 */
	public OAIRecordMetadata(String identifier, String xmlString) throws OAIRecordMetadataParseException {

		this.identifier = identifier;

		try {

			DOMDocument = MedatadaDOMHelper.XMLString2Document(xmlString);

		} catch (ParserConfigurationException e) {
			throw new OAIRecordMetadataParseException("Error en configuración del parser. Idenfier" + identifier , e);
		} catch (SAXException e) {
			throw new OAIRecordMetadataParseException("Error parsing xml en: " + identifier, e);
		} catch (IOException e) {
			throw new OAIRecordMetadataParseException("Error desconocido en parsing de  " + identifier, e);
		}
	}

	/**
	 * Constructs an empty OAI record metadata with the specified identifier.
	 * 
	 * @param identifier the unique identifier for this record
	 * @throws OAIRecordMetadataParseException if creating the empty document fails
	 */
	public OAIRecordMetadata(String identifier) throws OAIRecordMetadataParseException {

		this.identifier = identifier;

		try {

			DOMDocument = MedatadaDOMHelper.XMLString2Document(EMPTY_DOCUMENT);

		} catch (ParserConfigurationException e) {
			throw new OAIRecordMetadataParseException("Error en configuración del parser. Idenfier" + identifier , e);
		} catch (SAXException e) {
			throw new OAIRecordMetadataParseException("Error parsing xml en: " + identifier, e);
		} catch (IOException e) {
			throw new OAIRecordMetadataParseException("Error desconocido en parsing de  " + identifier, e);
		}
	}
	
	/**
	 * Constructs OAI record metadata from a DOM node.
	 * 
	 * @param identifier the unique identifier for this record
	 * @param node the DOM node containing the metadata
	 */
	public OAIRecordMetadata(String identifier, Node node) {
		this.identifier = identifier;
		this.DOMDocument = MedatadaDOMHelper.createDocumentFromNode(node);
	}
	
	
	/**
	 * Constructs OAI record metadata from an existing DOM document.
	 * 
	 * @param identifier the unique identifier for this record
	 * @param document the DOM document containing the metadata
	 */
	public OAIRecordMetadata(String identifier, Document document) {
		this.identifier = identifier;
		this.DOMDocument = document;
	}

	/**
	 * Retrieves all occurrences of a field as a list of string values.
	 * 
	 * @param fieldName the name of the field to retrieve
	 * @return list of field values, or empty list if none found
	 */
	public List<String> getFieldOcurrences(String fieldName) {
		
		
		try {
			NodeList nodelist = MedatadaDOMHelper.getNodeList(DOMDocument, XOAIXPATHHelper.getXPATH(fieldName) );
			List<String> contents = new ArrayList<String>(nodelist.getLength());
			
			for (int i = 0; i < nodelist.getLength(); i++) {
				try {

					if (nodelist.item(i).hasChildNodes() && nodelist.item(i).getFirstChild().getNodeValue() != null)
						contents.add(nodelist.item(i).getFirstChild().getNodeValue());
				
				} catch (NullPointerException e) {
					// Esto no debiera ocurrir nunca
					// TODO: mejorar el tratamiento de esto
					logger.error("Error obteniendo occurrencias: " +MedatadaDOMHelper.Node2XMLString(nodelist.item(i)));
				}
			}
			return contents;

		} catch (TransformerException e) {
			// TODO: mejorar el tratamiento de esto
			e.printStackTrace();
			return new ArrayList<String>(0);
		}
	}

	
	/**
	 * Replaces the content of a field occurrence with new content.
	 * 
	 * @param fieldName the name of the field to replace
	 * @param content the new content for the field
	 */
	public void replaceFieldOcurrence(String fieldName, String content) {
		
		try {
			
			String xpath =  XOAIXPATHHelper.getXPATH(fieldName);
			
			if ( MedatadaDOMHelper.isNodeDefined(DOMDocument,xpath) ) {
				
				Node node = MedatadaDOMHelper.getSingleNode(DOMDocument, xpath);
				MedatadaDOMHelper.setNodeText(node, content);
				
			}
		} catch (DOMException e) {
			logger.error("Error en remplazo de valor de metadato:" +fieldName);
			e.printStackTrace();
		} 
		
		catch (TransformerException e) {
			logger.error("Error en reemplazo de valor de metadato:" +fieldName);
			e.printStackTrace();
		}
	}
	
	/**
	 * Removes all occurrences of a field from the metadata.
	 * 
	 * @param fieldName the name of the field to remove
	 */
	public void removeFieldOcurrence(String fieldName) {
		
		try {
			
			String xpath =  XOAIXPATHHelper.getXPATH(fieldName);
			
			if ( MedatadaDOMHelper.isNodeDefined(DOMDocument,xpath) ) {
				
				NodeList nodes = MedatadaDOMHelper.getNodeList(DOMDocument, xpath);
				
				for (int i=0; i<nodes.getLength();i++)
					MedatadaDOMHelper.removeNodeAndEmptyParents(nodes.item(i));
			}
			
		} catch (DOMException e) {
			logger.error("Error remoción de valor de metadato:" +fieldName);
			e.printStackTrace();
		} 
		
		catch (TransformerException e) {
			logger.error("Error remoción de valor de metadato:" +fieldName);
			e.printStackTrace();
		}
	}
	
	/**
	 * Removes a specific DOM node and its empty parent nodes from the metadata.
	 * 
	 * @param node the node to remove
	 */
	public void removeNode(Node node) {
		
		try {
		  MedatadaDOMHelper.removeNodeAndEmptyParents(node);
		} catch (DOMException e) {
			logger.error("Error remoción de nodo");
			e.printStackTrace();
		} 
		
		catch (TransformerException e) {
			logger.error("Error remoción de nodo");
			e.printStackTrace();
		}
	}
	
	/**
	 * Adds a new occurrence of a field with the specified content.
	 * 
	 * @param fieldName the name of the field to add
	 * @param content the content for the new field occurrence
	 */
	public void addFieldOcurrence(String fieldName, String content) {

		try {
			
			String parentXPATH = XOAIXPATHHelper.getRootXPATH();
			Node parentNode = MedatadaDOMHelper.getSingleNode(DOMDocument, parentXPATH);
			
			List<OAIMetadataElement> elements = XOAIXPATHHelper.getXPATHList(fieldName);

			// esto va a definir todos los nodos no definidos, mira hasta el anteultimo elemento
			for (int i=0; i<elements.size()-1; i++) {
				
				OAIMetadataElement elem = elements.get(i);
				String xpath =  elem.getXpath();
		
				if ( !MedatadaDOMHelper.isNodeDefined(DOMDocument, xpath) ) {
					Node newNode = MedatadaDOMHelper.addChildElementWithNameAttr(parentNode, elem.getType().toString(), elem.getName());
				}
				
					
				parentNode = MedatadaDOMHelper.getSingleNode(DOMDocument, xpath);
			}
			
			// trata el ultimo elemento aparte
			
			OAIMetadataElement lastElem = elements.get(elements.size()-1); 
			String xpath =  lastElem.getXpath();
			
			// si el ultimo elemento ya esta definido
			if ( MedatadaDOMHelper.isNodeDefined(DOMDocument, xpath) ) {
				
				// lo obtiene
				Node node = MedatadaDOMHelper.getSingleNode(DOMDocument, xpath);
				
				if ( lastElem.getType() == Type.field ) { // si es de tipo field
					
					// agrega un nuevo nodo al padre (element), de tipo field, con el mismo nombre y con el valor deseado
					Node newNode = MedatadaDOMHelper.addChildElementWithNameAttr(node.getParentNode(), Type.field.toString(), lastElem.getName());
					MedatadaDOMHelper.setNodeText(newNode, content);
				
				}
				else { // si no es de tipo field es de tipo element, entonces
				
					// agrega un nuevo nodo hijo, de tipo field, con nombre value y el contenido
					Node newNode = MedatadaDOMHelper.addChildElementWithNameAttr(node, Type.field.toString(), "value");
					MedatadaDOMHelper.setNodeText(newNode, content);
					
				}
	
			}
			else { // si no esta definido
				

				if ( lastElem.getType() == Type.field ) { // si es de tipo field
					
					// agrega un nuevo nodo al padre (anteultimo elemento), de tipo field, con el mismo nombre y con el valor deseado
					Node newNode = MedatadaDOMHelper.addChildElementWithNameAttr(parentNode, Type.field.toString(), lastElem.getName());
					MedatadaDOMHelper.setNodeText(newNode, content);
				
				}
				else { // si no es de tipo field es de tipo element, entonces
					
					// agrega un nuevo nodo hijo al anteultimo nodo, de tipo element, usando el nombre del ultimo elemento
					Node newElementNode = MedatadaDOMHelper.addChildElementWithNameAttr(parentNode, Type.element.toString(), lastElem.getName());
				
					// a ese nuevo nodo element le agrega un nuevo nodo hijo, de tipo field, con nombre value y el contenido
					Node newFieldNode = MedatadaDOMHelper.addChildElementWithNameAttr(newElementNode, Type.field.toString(), "value");
					MedatadaDOMHelper.setNodeText(newFieldNode, content);
					
				}
				
				
				/**
				 * Nota: Se puede resumir este codigo juntando casos y puede quedar más corto, pero prefiero dejar los casos 
				 * separados para facilitar el debug.
				 */
			}
			

		
		} catch (DOMException e) {
			logger.error("Error en agregador de metadato:" +fieldName);
			e.printStackTrace();
		} 
		
		catch (TransformerException e) {
			logger.error("Error en agregador de metadato:" +fieldName);
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Gets the content of a field that starts with a specified prefix, removing the prefix.
	 * 
	 * @param fieldname the name of the field to query
	 * @param prefix the prefix to match and remove
	 * @return the field content with the prefix removed, or "UNKNOWN" if not found
	 */
	public String getFieldPrefixedContent(String fieldname, String prefix) {

		String name = "UNKNOWN";

		for (Node node : getFieldNodes(fieldname)) {

			String occr = node.getFirstChild().getNodeValue();

			if (occr.startsWith(prefix))
				name = occr.substring(prefix.length()).trim();
		}

		return name;
	}

	
	/**
	 * Gets a list of field nodes matching the specified XPath expression.
	 * 
	 * @param xpathSelector the XPath expression to evaluate
	 * @return list of matching nodes, or empty list if none found
	 */
    public List<Node> getFieldNodesByXPath (String xpathSelector) {
        

        try {

            return MedatadaDOMHelper.getListOfTextNodes(DOMDocument, xpathSelector );

        } catch (Exception e) {
            // TODO: mejorar el tratamiento de esto
            e.printStackTrace();
            return new ArrayList<Node>(0);
        }
    }

	
	/**
	 * Gets a list of field nodes for the specified field name.
	 * 
	 * @param fieldName the name of the field
	 * @return list of text nodes for the field, or empty list if none found
	 */
	public List<Node> getFieldNodes(String fieldName) {
		

		try {

			return MedatadaDOMHelper.getListOfTextNodes(DOMDocument, XOAIXPATHHelper.getXPATH(fieldName) );

		} catch (Exception e) {
			// TODO: mejorar el tratamiento de esto
			e.printStackTrace();
			return new ArrayList<Node>(0);
		}
	}
	

	
	/**
	 * Extracts bitstream (file) metadata from this record.
	 * 
	 * @return list of bitstream metadata objects
	 */
	public List<OAIMetadataBitstream> getBitstreams() {
		
		List<OAIMetadataBitstream> bundles = new ArrayList<OAIMetadataBitstream>();
		
		try {
			
			List<Node> bundleNodes = MedatadaDOMHelper.getListOfNodes(
					DOMDocument, XOAIXPATHHelper.getXPATH("bundles.bundle", false, true) 
			);
			
			
			for (int i=0; i<bundleNodes.size(); i++ ) {
				
				
				Node bundleNode = bundleNodes.get(i);
				
				
				Document bundleDoc = MedatadaDOMHelper.createDocumentFromNode(bundleNode);
				
				//System.out.println( MedatadaDOMHelper.Node2XMLString(bundleDoc) );
				
				List<Node> bitstreamNodes = MedatadaDOMHelper.getListOfNodes(
						bundleDoc, XOAIXPATHHelper.getXPATH("bundle.bitstreams.bitstream", false, false)) ;
				
				
				String type = MedatadaDOMHelper.getSingleString(bundleDoc, XOAIXPATHHelper.getXPATH("bundle:name", true, false) );
				
				
				for (Node bitstreamNode : bitstreamNodes ) {
					
					OAIMetadataBitstream bitstream = new OAIMetadataBitstream();
					bitstream.setType(type);
					
					for ( int j = 0; j < bitstreamNode.getChildNodes().getLength(); j++ ) {
				
						Node propertyNode = bitstreamNode.getChildNodes().item(j);
						
						if ( propertyNode.getAttributes() != null && propertyNode.getAttributes().getLength() > 0 
								&& propertyNode.getAttributes().getNamedItem("name") != null ) {
						
							switch ( propertyNode.getAttributes().getNamedItem("name").getTextContent() ) {
								
								case "name":
									bitstream.setName( propertyNode.getFirstChild().getTextContent() );
									break;
									
								case "url":
									bitstream.setUrl(new URI(URLDecoder.decode(propertyNode.getFirstChild().getTextContent(), "UTF-8")).toURL().toString());
									break;
									
								case "size":
									bitstream.setSize( propertyNode.getFirstChild().getTextContent() );
									break;
								
								case "format":
									bitstream.setFormat( propertyNode.getFirstChild().getTextContent() );
									break;
									
								case "checksum":
									bitstream.setChecksum( propertyNode.getFirstChild().getTextContent() );
									break;
								
								case "sid":
									bitstream.setSid( Integer.getInteger( propertyNode.getFirstChild().getTextContent() ) );
									if ( bitstream.getSid() == null ) bitstream.setSid(10*i+j); 
									break;
								
								
								default:
									break;
							}
						}
					}
					
					bundles.add(bitstream);
					
				}
			}
			
		} catch (Exception e) {
			
			logger.debug( "Problemas en getBundles :: " + e.getMessage());
		}
		
		
		logger.debug("bundles " + bundles.toString());
		
		return bundles;
	}
	

	@Override
	public String toString() {
			return MedatadaDOMHelper.document2XMLString(this.DOMDocument);
		
	}

	
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////// V4 Metadata API ///////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Gets metadata occurrences for the specified field expression.
	 * Returns this metadata object if the expression is the identity expression.
	 * 
	 * @param metadataExpression the field expression or identity marker
	 * @return list of metadata objects matching the expression
	 */
	public List<OAIRecordMetadata> getFieldMetadataOccurrences(String metadataExpression) {
		
		List< OAIRecordMetadata > result = new ArrayList<OAIRecordMetadata>();
		
		// If the expresion is identity, then return a list with this metadata object
		if ( metadataExpression.equals(IDENTITY_METADATA_EXPRESION) ) {
			result.add(this);
			return result;
		}
		
		try {
			String xpath = XOAIXPATHHelper.getXPATH(metadataExpression, false, false);
			List<Node> nodes = MedatadaDOMHelper.getListOfNodes(DOMDocument, xpath);
					
			for (Node n : nodes ) {
				result.add( new OAIRecordMetadata(this.identifier, n) );
			}
			
			return result;

		} catch (Exception e) {
			// TODO: mejorar el tratamiento de esto
			logger.error( "OAIRecordMetadata.getFieldOAIMetatadaList:" + metadataExpression + " error: " + e.getMessage() );
			return result;
		}
	}
	
	/**
	 * Gets metadata occurrences matching a raw XPath expression.
	 * 
	 * @param xpathExpression the XPath expression to evaluate
	 * @return list of metadata objects for matching nodes
	 */
	public List<OAIRecordMetadata> getFieldMetadataOccurrencesFromXPATHExpression(String xpathExpression) {
		
		List< OAIRecordMetadata > result = new ArrayList<OAIRecordMetadata>();
		
		try {
			List<Node> nodes = MedatadaDOMHelper.getListOfNodes(DOMDocument, xpathExpression);
					
			for (Node n : nodes ) {
				result.add( new OAIRecordMetadata(this.identifier, n) );
			}
			
			return result;

		} catch (Exception e) {
			// TODO: mejorar el tratamiento de esto
			logger.error( "OAIRecordMetadata.getFieldOAIMetatadaListFromXPATH:" + xpathExpression + " error: " + e.getMessage() );
			return result;
		}
	}
	
	/**
	 * Gets the value of a field as a string.
	 * 
	 * @param fieldName the name of the field
	 * @return the field value, or empty string if not found
	 */
	public String getFieldValue(String fieldName) {
		
		try {
			return MedatadaDOMHelper.getSingleString(DOMDocument, XOAIXPATHHelper.getXPATH(fieldName, true, false) );
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			logger.error( "OAIRecordMetadata.getFieldValue:" + fieldName + " error: " + e.getMessage() );
			return "";
		}
	
	}
	
	/**
	 * Gets the value of a field using a raw XPath expression.
	 * 
	 * @param xpathExpression the XPath expression to evaluate
	 * @return the field value, or empty string if not found
	 */
	public String getFieldValueFromXPATHExpression(String xpathExpression) {
		
		try {
			return MedatadaDOMHelper.getSingleString(DOMDocument, xpathExpression );
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			logger.error( "OAIRecordMetadata.getFieldValueFromXpath:" + xpathExpression + " error: " + e.getMessage() );
			return "";
		}
	
	}
	
	/**
	 * Gets a field value if  element (fieldName) contains a subelemement (selectoFieldName) with field value == selectorValue 
	 * 
	 * @param fieldName field name 
	 * @param discriminatorFieldName subfield to be used as discriminator
	 * @param discriminatorValue discriminator value 
	 * @return field value if field with selector exists or null otherwise
	 */
	public String getFieldValue(String fieldName, String discriminatorFieldName, String discriminatorValue) {
		
		for ( OAIRecordMetadata candidate : this.getFieldMetadataOccurrences(fieldName) ) {
			
			if ( candidate.getFieldValue(discriminatorFieldName).equals( discriminatorValue ) ) 
				return candidate.getFieldValue(fieldName);
			
		}
		
		return null;
	
	}


	

}
