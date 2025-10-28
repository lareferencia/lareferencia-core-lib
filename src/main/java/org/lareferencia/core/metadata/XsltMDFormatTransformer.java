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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

/**
 * Metadata format transformer using XSLT stylesheets for conversion between formats.
 * Transforms XML metadata from one format to another using configurable XSLT transformations.
 */
public class XsltMDFormatTransformer implements IMDFormatTransformer {


	private static Logger logger = LogManager.getLogger(XsltMDFormatTransformer.class);



	/**
	 * The source metadata format identifier.
	 */
	@Getter
	private String sourceMDFormat;
	
	/**
	 * The target metadata format identifier.
	 */
	@Getter
	private String targetMDFormat;


	private Transformer trf = null;

	/**
	 * Constructs a new XSLT metadata transformer with the specified formats and stylesheet.
	 * Validates stylesheet existence and initializes the transformer with output properties.
	 *
	 * @param sourceMDFormat the source metadata format name
	 * @param targetMDFormat the target metadata format name
	 * @param stylesheetFileName the path to the XSLT stylesheet file
	 * @throws IllegalArgumentException if the stylesheet file does not exist
	 * @throws RuntimeException if transformer creation fails
	 */
	public XsltMDFormatTransformer(String sourceMDFormat, String targetMDFormat, String stylesheetFileName)  {

		File stylesheetFile = new File(stylesheetFileName);
		
		if (!stylesheetFile.exists()) {
			String errorMsg = "XSLT stylesheet file not found: " + stylesheetFileName;
			logger.error(errorMsg);
			throw new IllegalArgumentException(errorMsg);
		}

		try {
			trf = buildXSLTTransformer(stylesheetFile);
			trf.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			trf.setOutputProperty(OutputKeys.INDENT, "yes");
			trf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

		} catch (TransformerConfigurationException e) {
			String errorMsg = "Error creating XSLT transformer from " + sourceMDFormat + 
			                  " to " + targetMDFormat + " using stylesheet: " + stylesheetFileName;
			logger.error(errorMsg, e);
			throw new RuntimeException(errorMsg, e);
		}

		this.sourceMDFormat = sourceMDFormat;
		this.targetMDFormat = targetMDFormat;
	}

	@Override
	public void setParameter(String name, List<String> values) {
		if (name == null) {
			logger.warn("Attempted to set parameter with null name, ignoring");
			return;
		}
		if (values == null) {
			logger.warn("Attempted to set null values for parameter: " + name + ", ignoring");
			return;
		}
		
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document xmlDoc = db.newDocument();
			Element root = xmlDoc.createElement("items");
			
			for (String value : values) {
				if (value != null) {  // Skip null values in the list
					Node item = xmlDoc.createElement("item");
					item.appendChild(xmlDoc.createTextNode(value));
					root.appendChild(item);
				}
			}
			xmlDoc.appendChild(root);
			trf.setParameter(name, xmlDoc);
		} catch (ParserConfigurationException e) {
			logger.error("Error creating XML document for parameter: " + name, e);
			throw new RuntimeException("Error creating XML document for parameter: " + name, e);
		} catch (DOMException e) {
			logger.error("Error building parameter XML for: " + name, e);
			throw new RuntimeException("Error building parameter XML for: " + name, e);
		}
	}

	@Override
	public void setParameter(String name, String value) {
		if (name == null) {
			logger.warn("Attempted to set parameter with null name, ignoring");
			return;
		}
		if (value == null) {
			logger.warn("Attempted to set null value for parameter: " + name + ", ignoring");
			return;
		}
		trf.setParameter(name, value);
	}

	@Override
	public String transformToString(Document source) throws MDFormatTranformationException {

		StringWriter stringWritter = new StringWriter();
		Result output = new StreamResult(stringWritter);

		try {
			trf.transform(new DOMSource(source), output);
		} catch (TransformerException e) {
			throw new MDFormatTranformationException(e.getMessage(),e.getCause() );
		}
		return stringWritter.toString();

	}


	@Override
	public Document transform(Document source) throws MDFormatTranformationException {

		DOMResult result = new DOMResult();

		try {
			trf.transform(new DOMSource(source), result);
		} catch (TransformerException e) {
			throw new MDFormatTranformationException(e.getMessage(),e.getCause() );
		}

		return (Document) result.getNode();
	}

	// ///////////////////////////// STATIC
	// /////////////////////////////////////////
	// private static TransformerFactory xformFactory =
	// TransformerFactory.newInstance();
	// / Ahora se usa saxon para ofrecer xslt2.0
	private static TransformerFactory xformFactory = new net.sf.saxon.TransformerFactoryImpl();

	private static Transformer buildXSLTTransformer(String xlstString) throws TransformerConfigurationException {

		StringReader reader = new StringReader(xlstString);
		StreamSource stylesource = new StreamSource(reader);
		return xformFactory.newTransformer(stylesource);
	}

	private static Transformer buildXSLTTransformer(File stylefile) throws TransformerConfigurationException {

		StreamSource stylesource = new StreamSource(stylefile);
		return xformFactory.newTransformer(stylesource);
	}

	// ///////////////////////////////// FIN STATIC
	// ////////////////////////////////////////////////////////

}
