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

package org.lareferencia.core.harvester.workers;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;

import javax.print.Doc;
import javax.xml.transform.TransformerException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.harvester.BaseHarvestingEventSource;
import org.lareferencia.core.harvester.HarvestingEvent;
import org.lareferencia.core.harvester.HarvestingEventStatus;
import org.lareferencia.core.harvester.IHarvester;
import org.lareferencia.core.harvester.NoRecordsMatchException;
import org.lareferencia.core.metadata.IMDFormatTransformer;
import org.lareferencia.core.metadata.MDFormatTranformationException;
import org.lareferencia.core.metadata.MDFormatTransformerService;
import org.lareferencia.core.metadata.MedatadaDOMHelper;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.util.date.DateHelper;
import org.oclc.oai.harvester2.verb.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@Component
@Scope(value = "prototype")
public class OCLCBasedHarvesterImpl extends BaseHarvestingEventSource implements IHarvester {
	
	private static Logger logger = LogManager.getLogger(OCLCBasedHarvesterImpl.class);


	private static final String METADATA_NODE_NAME = "metadata";
	private static final Object STATUS_DELETED = "deleted";
	
	//@Value("${metadata.store.format}")
	//private String METADATA_STORE_FORMAT;
	
	@Autowired
	MDFormatTransformerService trfService;
	
    @Autowired
    private DateHelper dateHelper;	

	private IMDFormatTransformer metadataTransformer;
	
	@Value("${harvester.retry.seconds}")
	private int INITIAL_SECONDS_TO_RETRY;

	@Value("${harvester.retry.factor}")
	private int RETRY_FACTOR;

	//@Value("${ssl.truststore.path}")
	//private String sslTrustStorePath;

	
	private boolean stopSignalReceived = false;
	
	HarvestingEvent reusableEvent = new HarvestingEvent();

	@Override
	public void stop() {
		stopSignalReceived = true;
	}

	@Override
	public void reset() {
		stopSignalReceived = false;
		reusableEvent = new HarvestingEvent();
	}

	// private static TransformerFactory xformFactory =
	// TransformerFactory.newInstance();

	public OCLCBasedHarvesterImpl() {
		super();
		logger.debug("Creando Harvester: " + this.toString());
		reset();
	}

	public void harvest(String originURL, String set, String metadataPrefix, String metadataStoreSchema, String from, String until, String resumptionToken, int maxRetries) {
		

		ListRecords actualListRecords = null;

		int batchIndex = 0;
		int actualRetry = 0;
		int secondsToNextRetry = INITIAL_SECONDS_TO_RETRY;

		// La condición es que sea la primera corrida o que no sea null el
		// resumption (caso de fin)
		// Si levantan la stopSignal entonces corta el ciclo de harvesting
		while (!stopSignalReceived && (batchIndex == 0 || (resumptionToken.trim().length() != 0))) {

			do {
				try {

					logger.debug( "URL: " + originURL + "  Request:" + resumptionToken + " Set:" + set + " From: " + from);

					actualListRecords = listRecords(originURL, from, until, set, metadataPrefix, batchIndex, resumptionToken);
					resumptionToken = actualListRecords.getResumptionToken();

					// se crea un evento a partir del resultado de listRecords
					reusableEvent = createResultFromListRecords(reusableEvent, actualListRecords, originURL, metadataPrefix, metadataStoreSchema);
					reusableEvent.setStatus(HarvestingEventStatus.OK);
					reusableEvent.setResumptionToken(resumptionToken);
					reusableEvent.setMetadataPrefix(metadataPrefix);

					// se lanza el evento
					fireHarvestingEvent(reusableEvent);

					batchIndex++;
					actualRetry = 0;
					secondsToNextRetry = INITIAL_SECONDS_TO_RETRY;
					break;
					
				} catch (NoRecordsMatchException e) {
					logger.info( originURL + " -- No new or deleted records were detected - from: " + from);
					
					// se lanza el evento
					reusableEvent.reset();
					reusableEvent.setMessage("No new or deleted records were detected - from: " + from);
					reusableEvent.setStatus(HarvestingEventStatus.NO_MATCHING_QUERY);
					fireHarvestingEvent(reusableEvent);
					
					
					batchIndex++;
					resumptionToken = "";

					// no hay retries
					break;
				

				} catch (RecoverableHarvestingException e) {

					String message = buildErrorMessage(e, batchIndex, actualRetry);
					message += "Last RToken: " + resumptionToken + "\n";
					message += "\nWaiting " + secondsToNextRetry + " seconds for the next try ..";

					reusableEvent.reset();
					reusableEvent.setMessage(message);
					reusableEvent.setStatus(HarvestingEventStatus.ERROR_RETRY);
					fireHarvestingEvent(reusableEvent);

					// Una espera de secondsToNextRetry
					try {
						Thread.sleep(secondsToNextRetry * 1000);
					} catch (InterruptedException t) {
					}

					// Se incrementa el retry y se duplica el tiempo de espera
					actualRetry++;
					secondsToNextRetry = secondsToNextRetry * RETRY_FACTOR;
					
				} catch (FatalHarvestingException e) {
					batchIndex++;
					resumptionToken = "";
					reusableEvent.reset();
					reusableEvent.setMessage(e.getMessage());
					reusableEvent.setStatus(HarvestingEventStatus.ERROR_FATAL);
					fireHarvestingEvent(reusableEvent);					
					break; // no hay retries
				}

			} while (actualRetry < maxRetries && !stopSignalReceived);

			if (actualRetry == maxRetries) {
				String message = "Max retries reached.  Aborting harvesting processs.";
				reusableEvent.reset();
				reusableEvent.setMessage(message);
				reusableEvent.setStatus(HarvestingEventStatus.ERROR_FATAL);
				fireHarvestingEvent(reusableEvent);
				break;
			}

			if (stopSignalReceived) {
				String message = "Stop signal received.";
				message += "  Origen: " + originURL;
				message += "  Set: " + set;
				
				reusableEvent.reset();
				reusableEvent.setMessage(message);
				reusableEvent.setStatus(HarvestingEventStatus.STOP_SIGNAL_RECEIVED);
				fireHarvestingEvent(reusableEvent);
				break;
			}

		}
	}

	private String buildErrorMessage(Exception e, int batchIndex, int actualRetry) {
		String message = "Error batch: " + batchIndex + " retry: " + actualRetry + "\n";
		message += "Details:\n";
		message += e.getMessage() + "\n";

		return message;
	}

	private ListRecords listRecords(String baseURL, String from, String until, String setSpec, String metadataPrefix, int batchIndex, String resumptionToken) throws NoRecordsMatchException, FatalHarvestingException, RecoverableHarvestingException {

		ListRecords listRecords = null;
		/*
		 * Se encapsulan las dos llamadas distintas en una sola, que depende de
		 * la existencia del RT
		 */
	

		if (batchIndex == 0)
			listRecords = new ListRecords(baseURL, from, until, setSpec, metadataPrefix);
		else
			listRecords = new ListRecords(baseURL, resumptionToken);

		NodeList errors = listRecords.getErrors();

		if (errors != null && errors.getLength() > 0) {
			
			
			for (int i = 0; i < errors.getLength(); i++) {
				Node error = errors.item(i);
				
				if ( error.getAttributes().getNamedItem("code") != null ) {
					String code = error.getAttributes().getNamedItem("code").getNodeValue();
				
					if ( code.equals("noRecordsMatch") )
						throw new NoRecordsMatchException();
					else
						throw new FatalHarvestingException("ListRecords call error :: error code: " + code);

				}
				else
					throw new FatalHarvestingException("ListRecords call error :: error code: Unknown or null");
			}
			
			
			
			//throw new HarvestingException(listRecords.toString());
		} else {
			resumptionToken = listRecords.getResumptionToken();
			if (resumptionToken != null && resumptionToken.length() == 0)
				resumptionToken = null;
		}
		
		

		return listRecords;
	}

	private HarvestingEvent createResultFromListRecords(HarvestingEvent reusableEvent, ListRecords listRecords, String originURL, String metadataPrefix, String metadataStoreSchema) throws FatalHarvestingException  {

		reusableEvent.reset();
		reusableEvent.setOriginURL(originURL);
		
		Boolean transformMetadataFormat = ! metadataStoreSchema.equals(metadataPrefix);
		
		// Inicializa el transformador si corresponde
		if ( transformMetadataFormat )
			try {
				metadataTransformer = trfService.getMDTransformer(metadataPrefix, metadataStoreSchema);
			} catch (MDFormatTranformationException e) {
				throw new FatalHarvestingException("ListRecords XML schema transform error :: getting Metadata transformer from: " + metadataPrefix + " to: " + metadataStoreSchema+ " ::" + e.getMessage(), e);    
			}
		

		// La obtención de registros por xpath se realiza de acuerdo al schema
		// correspondiente
		NodeList nodes = null;
		String namespace = null;

		try {
			if (listRecords.getSchemaLocation().indexOf(ListRecords.SCHEMA_LOCATION_V2_0) != -1) {
				nodes = listRecords.getNodeList("/oai20:OAI-PMH/oai20:ListRecords/oai20:record");
				namespace = "oai20";
			} else if (listRecords.getSchemaLocation().indexOf(ListRecords.SCHEMA_LOCATION_V1_1_LIST_RECORDS) != -1) {
				namespace = "oai11_ListRecords";
				nodes = listRecords.getNodeList("/oai11_ListRecords:ListRecords/oai11_ListRecords:record");
			} else {
				throw new FatalHarvestingException("ListRecords XML parsing error :: record tag entries not found");
			}
		} catch ( TransformerException e ) {
			throw new FatalHarvestingException("ListRecords XML parsing error :: record tag entries parsing problems" + e.getMessage());
		}

		// logger.debug(( listRecords.toString() ));

		// Determina la dirección de cosecha
		//String origin = listRecords.getSingleString("/" + namespace + ":OAI-PMH/" + namespace + ":request");		
		
		for (int i = 0; i < nodes.getLength(); i++) {

			String identifier = "unknown";
			String status = "unknown";
			LocalDateTime datestamp;

			try {
				identifier = listRecords.getSingleString(nodes.item(i), namespace + ":header/" + namespace + ":identifier");
				datestamp = dateHelper.parseDate( listRecords.getSingleString(nodes.item(i), namespace + ":header/" + namespace + ":datestamp") );
				
				String setSpec = listRecords.getSingleString(nodes.item(i), namespace + ":header/" + namespace + ":setSpec");
				status = listRecords.getSingleString(nodes.item(i), namespace + ":header/@status");

				if (!status.equals(STATUS_DELETED)) {
					
					//logger.debug(( MedatadaDOMHelper.node2XMLString( nodes.item(i) )));

					// Obtiene el iésimo node de metadatos del listrecords
					Document domDocument = getMetadataNode( nodes.item(i), listRecords.getDocument() );
					logger.debug( "Processed id:" + identifier);

					// Si el formato de metadatos cosechado no es el usado para store lo transforma
					if ( transformMetadataFormat ) {

						// identifier del record
						metadataTransformer.setParameter("identifier", identifier);
						metadataTransformer.setParameter("timestamp", DateHelper.getDateTimeMachineString(datestamp));

						domDocument = metadataTransformer.transform(domDocument);
					}

					// crea un elemento de metadata con esa información
					OAIRecordMetadata metadata = new OAIRecordMetadata(identifier, domDocument);
					metadata.setOrigin( originURL);
					metadata.setSetSpec(setSpec);
					metadata.setStoreSchema(metadataStoreSchema);
					metadata.setDatestamp(datestamp);

					reusableEvent.getRecords().add(metadata);

				} else {
					reusableEvent.getDeletedRecordsIdentifiers().add(identifier); 
				}

			} catch (NoSuchFieldException e) {
				reusableEvent.getMissingRecordsIdentifiers().add(identifier);
				reusableEvent.setRecordMissing(true);
			} catch (DateTimeParseException e) {
				throw new FatalHarvestingException("XML Record parsing error :: record datestamp parsing exception: " + e.getMessage() + " :: identifier " + identifier );
			} catch (TransformerException e) {
				throw new FatalHarvestingException("XML Record parsing error :: record field not found: " + e.getMessage() + "\n" + " :: identifier " + identifier );
			} catch (MDFormatTranformationException e) {
				throw new FatalHarvestingException("XML Record schema transform from: " +  metadataPrefix + " to: " + metadataStoreSchema + " :: "+ e.getMessage() +  " :: identifier " + identifier );
			} catch (Exception e) {
				throw new FatalHarvestingException("XML Record parsing unknown error" + e.getMessage() + " :: identifier " + identifier);
			}

		} /* fin for nodes */ 
			
		

		return reusableEvent;
	}


	/**
	 * @param node
	 * @param document
	 * @return
	 * @throws TransformerException
	 * @throws NoSuchFieldException
	 */
	private Document getMetadataNode(Node node, Document document) throws TransformerException, NoSuchFieldException {

		/**
		 * TODO: búsqueda secuencial, puede ser ineficiente pero xpath no esta
		 * implementado sobre nodos individuales en la interfaz listRecords, en
		 * necesario construir un DomHelper para Harvester, es sencillo dada la
		 * clase base BaseMetadataDOMHelper
		 */

		NodeList childs = node.getChildNodes();
		Node metadataNode = null;
 		for (int i = 0; i < childs.getLength(); i++)
			if (childs.item(i).getNodeName().contains(METADATA_NODE_NAME))
				metadataNode = childs.item(i);
 		
		//logger.debug(( MedatadaDOMHelper.node2XMLString( metadataNode )));


		if (metadataNode == null)
			throw new NoSuchFieldException("Metadata node don´t exist: " + METADATA_NODE_NAME + " in response.\n" + MedatadaDOMHelper.Node2XMLString(node));

		// este rename unifica los casos distintos de namespace encontrados en
		// repositorios
		//document.renameNode(metadataNode, metadataNode.getNamespaceURI(), METADATA_NODE_NAME);
		
		
		
		/**
		 * Este fragmento encuentra el nodo hijo con mas de 1 hijo, eso es necesario pq a veces texto vacio es leido como nodo
		 */
		childs = metadataNode.getChildNodes();
		Node childNode = null;

		for (int i = 0; i < childs.getLength(); i++)
			if (childs.item(i).getChildNodes().getLength() > 1 )
				childNode = childs.item(i);

		
		//logger.debug(( MedatadaDOMHelper.node2XMLString( childNode )));

		
		if (childNode == null)
			throw new NoSuchFieldException("Empty metadata node.\n" + MedatadaDOMHelper.Node2XMLString(metadataNode));

		// TODO: Ver el tema del char &#56256;
		return MedatadaDOMHelper.createDocumentFromNode(childNode);
	}


	@Override
	public List<String> listSets(String uri) {

		List<String> setList = new ArrayList<String>();

		try {
			ListSets listSets = new ListSets(uri);
			NodeList list = listSets.getDocument().getElementsByTagName("setSpec");

			for (int i = 0; i < list.getLength(); i++) {
				if (list.item(i).getFirstChild() != null && list.item(i).getFirstChild().getNodeValue() != null)
					setList.add(list.item(i).getFirstChild().getNodeValue());
			}

		} catch (Exception e) {
			logger.error("ListSets Error: " + e.getMessage() );
		}
		
		return setList;
	}

	@Override
	public Map<String, String> identify(String originURL) {

		Map<String, String> identifyMap = new HashMap<String, String>();

		try {
			Identify identify = new Identify(originURL);

			// La obtención de registros por xpath se realiza de acuerdo al schema
	        // correspondiente
	        NodeList nodes = null;
	        String namespace = null;

	        try {
	            if (identify.getSchemaLocation().indexOf(Identify.SCHEMA_LOCATION_V2_0) != -1) {
	                nodes = identify.getNodeList("/oai20:OAI-PMH/oai20:Identify");
	                namespace = "oai20";
	            } else if (identify.getSchemaLocation().indexOf(Identify.SCHEMA_LOCATION_V1_1_LIST_RECORDS) != -1) {
	                namespace = "oai11_Identify";
	                nodes = identify.getNodeList("/oai11_Identify:Identify");
	            } else {
	                throw new FatalHarvestingException("ListRecords XML parsing error :: record tag entries not found");
	            }
	        } catch ( TransformerException e ) {
	            throw new FatalHarvestingException("ListRecords XML parsing error :: record tag entries parsing problems" + e.getMessage());
	        }

			if (nodes.getLength() == 0) {
	            throw new FatalHarvestingException("ListRecords XML parsing error :: record tag entries not found");
	        }
	        Node node = nodes.item(0);

			identifyMap.put("repositoryName", identify.getSingleString(node, namespace + ":repositoryName"));
			identifyMap.put("baseURL", identify.getSingleString(node, namespace + ":baseURL"));
			identifyMap.put("protocolVersion", identify.getSingleString(node, namespace + ":protocolVersion"));
			identifyMap.put("adminEmail", identify.getSingleString(node, namespace + ":adminEmail"));
			identifyMap.put("earliestDatestamp", identify.getSingleString(node, namespace + ":earliestDatestamp"));
			identifyMap.put("deletedRecord", identify.getSingleString(node, namespace + ":deletedRecord"));
			identifyMap.put("granularity", identify.getSingleString(node, namespace + ":granularity"));
			identifyMap.put("compression", identify.getSingleString(node, namespace + ":compression"));

			return identifyMap;

		} catch (FatalHarvestingException e) {
			logger.error("Identify Error: " + e.getMessage());
		} catch (RecoverableHarvestingException e) {
			logger.error("Identify Error: " + e.getMessage());
		} catch (TransformerException e) {
			logger.error("Identify Error: " + e.getMessage());
		}

		return null;
	}
}
