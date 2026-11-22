package org.lareferencia.core.worker.indexing;

import org.apache.solr.common.SolrInputDocument;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** Preserves repeated bibliographic fields produced by the Solr crosswalk. */
final class SolrDocuments {
	private SolrDocuments() { }

	static SolrInputDocument fromXml(Document doc) {
		SolrInputDocument solrDoc = new SolrInputDocument();
		NodeList fields = doc.getElementsByTagName("field");
		for (int i = 0; i < fields.getLength(); i++) {
			Element field = (Element) fields.item(i);
			String name = field.getAttribute("name");
			String value = field.getTextContent();
			solrDoc.addField(name, value);
		}
		return solrDoc;
	}
}
