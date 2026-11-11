
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

package org.lareferencia.core.worker.indexing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.lareferencia.core.worker.solr.RunningSolrException;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.worker.solr.BaseSolrWorker;
import lombok.Getter;
import lombok.Setter;

/**
 * Worker that removes all records from the Solr index for a specific network.
 * <p>
 * Used for full index cleanup operations.
 * </p>
 * 
 * @author LA Referencia Team
 */
public class FullUnindexerWorker extends BaseSolrWorker<NetworkRunningContext> {

    private static Logger logger = LogManager.getLogger(FullUnindexerWorker.class);

    @Getter
    @Setter
    private String solrNetworkIDField;

    @Getter
    @Setter
    private String solrRecordIDField = "id";

    /**
     * Creates a full unindexer worker with the specified Solr URL.
     * 
     * @param solrURL the Solr server URL
     */
    public FullUnindexerWorker(String solrURL) {
        super(solrURL);
    }

    /**
     * Executes the unindexing operation for the network.
     */
    private String networkQuery;
    private long documentsBefore;

    @Override
    public void execute() throws RunningSolrException {
        String acronym = runningContext.getNetwork().getAcronym();
        networkQuery = solrNetworkIDField + ":" + acronym;
        try {
            documentsBefore = countNetworkDocuments();
            logger.info("WORKER:{}: Unindex target={} query={} documentsBefore={}",
                    getName(), getSolrUrl(), networkQuery, documentsBefore);
            if (documentsBefore == 0) {
                logger.warn("No documents match {} in {}. Check this worker's Solr URL against the indexer and VuFind.",
                        networkQuery, getSolrUrl());
            }
            deleteByField(solrNetworkIDField, acronym);
        } catch (Exception exception) {
            throw new RunningSolrException("Unindex failed: target=" + getSolrUrl()
                    + " query=" + networkQuery + " :: " + exception.getMessage());
        }
    }

    @Override
    protected void postRun() throws RunningSolrException {
        try {
            solrCommit();
            long remaining = countNetworkDocuments();
            logger.info("WORKER:{}: Unindex committed target={} query={} documentsBefore={} documentsRemaining={}",
                    getName(), getSolrUrl(), networkQuery, documentsBefore, remaining);
            if (remaining > 0) {
                throw new RunningSolrException("Unindex verification failed: " + remaining
                        + " documents remain in " + getSolrUrl() + " for " + networkQuery
                        + ". Check concurrent indexing and the Solr query.");
            }
        } catch (RunningSolrException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new RunningSolrException("Unindex commit or verification failed: target=" + getSolrUrl()
                    + " query=" + networkQuery + " :: " + exception.getMessage());
        }
    }

    private long countNetworkDocuments() throws Exception {
        SolrQuery query = new SolrQuery(networkQuery);
        query.setRows(0);
        return solrClient.query(query).getResults().getNumFound();
    }

    @Override
    public String toString() {
        return "UnIndexer[Delete by " + this.solrNetworkIDField + "]";
    }

}
