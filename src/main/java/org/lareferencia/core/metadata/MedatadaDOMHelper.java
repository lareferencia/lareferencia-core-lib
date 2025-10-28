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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.xpath.XPathAPI;
import org.apache.xpath.objects.XObject;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Abstract helper class for DOM manipulation of metadata XML documents.
 */
public abstract class MedatadaDOMHelper {

	/**
	 * Private constructor to prevent instantiation of abstract utility class.
	 */
	private MedatadaDOMHelper() {
		throw new UnsupportedOperationException("Utility class");
	}

	private static Element namespaceElement = null;
	private static DocumentBuilderFactory factory;
	private static TransformerFactory xformFactory = new net.sf.saxon.TransformerFactoryImpl();
	private static HashMap<String, DocumentBuilder> builderMap = new HashMap<String, DocumentBuilder>();

	static {
		try {
			factory = DocumentBuilderFactory.newInstance();
			factory.setNamespaceAware(true);
			//factory.setExpandEntityReferences(false);

			DOMImplementation impl = obtainThreadBuider().getDOMImplementation();
			Document namespaceHolder = impl.createDocument("http://www.openarchives.org/OAI/2.0/oai_dc", "oaidc:namespaceHolder", null);

			/**
			 * TODO: Este listado comprende los namespaces que es capaza de
			 * reconocer, esta lista debe ampliarse pues limita los metadatos
			 * que pueden manejarse, actualmente están declarados solo los NS de
			 * DC
			 */
			namespaceElement = namespaceHolder.getDocumentElement();
			namespaceElement.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
			namespaceElement.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:oai20", "http://www.openarchives.org/OAI/2.0/");
			namespaceElement.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
			namespaceElement.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:dc", "http://purl.org/dc/elements/1.1/");
			namespaceElement.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:xoai", "http://www.lyncode.com/xoai");
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Creates a new Document from a DOM Node.
	 *
	 * @param node the source node
	 * @return a new Document containing the node
	 */
	public static Document  createDocumentFromNode(Node node) {
		DocumentBuilder builder = obtainThreadBuider();
		Document document = builder.newDocument();
		
		//logger.debug(( node2XMLString(node)));
		
		Node newNode = document.importNode(node, true);
		document.appendChild(newNode);
		
		return document;
	}
	
	/**
	 * Converts an XML string to a Document object.
	 *
	 * @param xmlstring the XML string to parse
	 * @return the parsed Document
	 * @throws ParserConfigurationException if parser configuration error occurs
	 * @throws SAXException if XML parsing error occurs
	 * @throws IOException if I/O error occurs
	 */
	public static Document XMLString2Document(String xmlstring) throws ParserConfigurationException, SAXException, IOException {
		InputSource is = new InputSource();
		is.setCharacterStream(new StringReader(xmlstring));
		return obtainThreadBuider().parse(is);
	}
	
	/**
	 * Converts a Document to an XML string.
	 *
	 * @param document the document to convert
	 * @return the XML string representation
	 */
	public static String document2XMLString(Document document) {
		try {
			StringWriter sw = new StringWriter();
			Result output = new StreamResult(sw);
			Transformer idTransformer = xformFactory.newTransformer();
			idTransformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			idTransformer.setOutputProperty(OutputKeys.INDENT, "yes");
			idTransformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			idTransformer.transform(new DOMSource(document), output);
			return sw.toString();
		} catch (TransformerException e) {
			throw new RuntimeException("Error converting document to XML string", e);
		}
	}
	
	/**
	 * Evaluates an XPath expression and returns matching nodes.
	 *
	 * @param node the context node
	 * @param xpath the XPath expression
	 * @return list of matching nodes
	 * @throws TransformerException if XPath evaluation fails
	 */
	public static NodeList getNodeList(Node node, String xpath) throws TransformerException {
		return XPathAPI.selectNodeList(node, xpath, namespaceElement);
	}
	
	/**
	 * Gets a list of nodes matching the XPath expression (excludes text-only nodes).
	 *
	 * @param node the context node
	 * @param xpath the XPath expression
	 * @return list of matching nodes with children
	 * @throws TransformerException if XPath evaluation fails
	 */
	public static List<Node> getListOfNodes(Node node, String xpath) throws TransformerException {
		NodeList nodelist = XPathAPI.selectNodeList(node, xpath, namespaceElement);
		
		List<Node> result = new ArrayList<Node>(nodelist.getLength());
		
		for (int i = 0; i < nodelist.getLength(); i++) {
			if (nodelist.item(i).hasChildNodes())
				result.add(nodelist.item(i));

		}
		return result;
	}
	
	/**
	 * Gets a list of text nodes matching the XPath expression.
	 *
	 * @param node the context node
	 * @param xpath the XPath expression
	 * @return list of matching text nodes
	 * @throws TransformerException if XPath evaluation fails
	 */
	public static List<Node> getListOfTextNodes(Node node, String xpath) throws TransformerException {
		NodeList nodelist = XPathAPI.selectNodeList(node, xpath, namespaceElement);
		
		List<Node> result = new ArrayList<Node>(nodelist.getLength());
		
		for (int i = 0; i < nodelist.getLength(); i++) {
			if (nodelist.item(i).hasChildNodes() && nodelist.item(i).getFirstChild().getNodeValue() != null)
				result.add(nodelist.item(i));

		}
		return result;
	}
	
	/**
	 * Checks if a node exists for the given XPath expression.
	 *
	 * @param node the context node
	 * @param xpath the XPath expression
	 * @return true if node exists, false otherwise
	 */
	public static boolean isNodeDefined(Node node, String xpath)  {
		try {
			return getNodeList(node, xpath).getLength() > 0;
		} catch (TransformerException e) {
			throw new RuntimeException("Error evaluating XPath: " + xpath, e);
		}
	}

	/**
	 * Evaluates an XPath expression and returns the result as a string.
	 *
	 * @param node the context node
	 * @param xpath the XPath expression
	 * @return the string result
	 * @throws TransformerException if XPath evaluation fails
	 */
	public static String getSingleString(Node node, String xpath) throws TransformerException {
		return XPathAPI.eval(node, xpath, namespaceElement).str();
	}

	/**
	 * Evaluates an XPath expression and returns the result as an XObject.
	 *
	 * @param node the context node
	 * @param xpath the XPath expression
	 * @return the XObject result
	 * @throws TransformerException if XPath evaluation fails
	 */
	public static XObject getSingleXObjet(Node node, String xpath) throws TransformerException {
		return XPathAPI.eval(node, xpath, namespaceElement);
	}
	
	/**
	 * Selects a single node matching the XPath expression.
	 *
	 * @param node the context node
	 * @param xpath the XPath expression
	 * @return the matching node or null if not found
	 * @throws TransformerException if XPath evaluation fails
	 */
	public static Node getSingleNode(Node node, String xpath) throws TransformerException {
		return XPathAPI.selectSingleNode(node, xpath);
	}
	
	/**
	 * Adds a child element with a name attribute to the given node.
	 *
	 * @param node the parent node
	 * @param elementName the name of the new element
	 * @param nameAttrValue the value for the name attribute
	 * @return the newly created element
	 * @throws TransformerException if element creation fails
	 */
	public static Node addChildElementWithNameAttr(Node node, String elementName, String nameAttrValue) throws TransformerException {
		
		Document doc = node.getOwnerDocument();
		
		Element newDomElement = doc.createElementNS(node.getNamespaceURI(), elementName);; // .createElement( elementName );
		
		newDomElement.setAttribute("name", nameAttrValue );
		node.appendChild(newDomElement);
	
		return newDomElement;
	}
	
	/**
	 * Removes a node and its empty parent nodes recursively.
	 *
	 * @param node the node to remove
	 * @throws TransformerException if removal fails
	 */
	public static void removeNodeAndEmptyParents(Node node) throws TransformerException {
		Node parentNode = node.getParentNode();
		
		// Cannot remove root node
		if (parentNode == null) {
			return;
		}
		
		parentNode.removeChild(node); 
		
		while ( parentNode != null && node != parentNode && countChildsOfTypeElement(parentNode) == 0) {
			node = parentNode; 
			parentNode = node.getParentNode();
			
			if (parentNode != null) {
				parentNode.removeChild(node);
			}
		}
	}
	
	/**
	 * Counts the number of child elements (excluding text nodes) of a node.
	 *
	 * @param node the parent node
	 * @return count of element children
	 */
	private static int countChildsOfTypeElement(Node node) {
		
		int size = 0;
		for (int i=0; i<node.getChildNodes().getLength(); i++) {
			
			if ( node.getChildNodes().item(i).getNodeType() == Node.ELEMENT_NODE )
				size++;
			
		}
		return size;
	}

	
	/**
	 * Sets the text content of a node.
	 *
	 * @param node the node to modify
	 * @param content the text content
	 * @return the modified node
	 * @throws TransformerException if modification fails
	 */
	public static Node setNodeText(Node node, String content) throws TransformerException {
		
		
		if ( node.hasChildNodes() ) {
			node.removeChild( node.getFirstChild() );
		}
		
		Text text = node.getOwnerDocument().createTextNode(content);
		node.appendChild(text);
		return node;
	}
	
	
	
	
	/**
	 * Obtains a thread-local DocumentBuilder instance.
	 *
	 * @return the DocumentBuilder for the current thread
	 */
	protected static DocumentBuilder obtainThreadBuider() {

		DocumentBuilder builder = builderMap.get(Thread.currentThread().getName());
		if (builder == null) {
			try {
				builder = factory.newDocumentBuilder();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
				return null;
			}
			builderMap.put(Thread.currentThread().getName(), builder);
		}
		return builder;
	}

	/**
	 * Converts a DOM Node to an XML string representation.
	 * <p>
	 * Transforms the node using Saxon transformer with indentation and UTF-8 encoding.
	 * The XML declaration is omitted from the output.
	 * </p>
	 *
	 * @param node the DOM node to convert
	 * @return the XML string representation
	 * @throws TransformerException if transformation error occurs
	 */
	public static String Node2XMLString(Node node) throws TransformerException {

		StringWriter sw = new StringWriter();
		Result output = new StreamResult(sw);
		Transformer idTransformer = xformFactory.newTransformer();
		idTransformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		idTransformer.setOutputProperty(OutputKeys.INDENT, "yes");
		idTransformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		idTransformer.transform(new DOMSource(node), output);

		String result = sw.toString();

		//result = result.replaceAll("&#5[0-9]{4}", " ");

		return result;
	}
}
