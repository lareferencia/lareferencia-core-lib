
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

package org.lareferencia.core.worker.solr;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.IRunningContext;

/**
 * Base class for workers that interact with Solr.
 * <p>
 * Provides common functionality for sending updates, commits, and deletes
 * to a Solr search index.
 * </p>
 * 
 * @param <C> the running context type
 * @author LA Referencia Team
 * @see BaseWorker
 */
public abstract class BaseSolrWorker<C extends IRunningContext> extends BaseWorker<C> {
    /**
     * Solr HTTP client for server communication.
     */
    protected HttpSolrClient solrClient;
    
    private static Logger logger = LogManager.getLogger(BaseSolrWorker.class);

    /**
     * Creates a Solr worker with the specified Solr URL.
     * 
     * @param solrURL the Solr server URL
     */
    public BaseSolrWorker(String solrURL) {
        super();
        this.solrClient = new HttpSolrClient.Builder(solrURL).build();
    }

    /**
     * Sends an update request to Solr.
     * 
     * @param data the XML data to send
     * @throws SolrServerException if Solr encounters an error
     * @throws IOException if an I/O error occurs
     * @throws HttpSolrClient.RemoteSolrException if a remote error occurs
     */
    protected void sendUpdateToSolr(String data)
            throws SolrServerException, IOException, HttpSolrClient.RemoteSolrException {
        DirectXmlRequest request = new DirectXmlRequest("/update", data);
        solrClient.request(request);
    }

    /**
     * Commits pending changes to Solr.
     * 
     * @throws Exception if the commit fails
     */
    protected void solrCommit() throws Exception {
        this.sendUpdateToSolr("<commit/>");
    }

    /**
     * Rolls back pending changes in Solr.
     * 
     * @throws Exception if the rollback fails
     */
    protected void solrRollback() throws Exception {
        this.sendUpdateToSolr("<rollback/>");
    }

    /**
     * Deletes documents from Solr by field value.
     * 
     * @param field the field name
     * @param value the field value
     * @throws Exception if the delete fails
     */
    protected void deleteByField(String field, String value) throws Exception {
        this.sendUpdateToSolr("<delete><query>" + field + ":" + value + "</query></delete>");
    }

    /**
     * Hook called before worker execution starts.
     * 
     * @throws RunningSolrException if an error occurs during pre-run
     */
    protected void preRun() throws RunningSolrException {

    }

    /**
     * Main execution method to be implemented by subclasses.
     * 
     * @throws RunningSolrException if an error occurs during execution
     */
    protected abstract void execute() throws RunningSolrException;

    /**
     * Hook called after worker execution completes.
     * Automatically commits changes to Solr.
     * 
     * @throws RunningSolrException if an error occurs during post-run
     */
    protected void postRun() throws RunningSolrException {
        try {
            this.solrCommit();
        } catch (Exception e) {
            try {
                this.solrRollback();
                logger.error("Issues when commiting to SOLR: " + runningContext.toString() + ": " + e.getMessage());
            } catch (Exception e1) {
                logger.error("Issues when commiting to SOLR: " + runningContext.toString() + ": " + e.getMessage()
                        + " :: " + e1.getMessage());
                throw new RunningSolrException("Issues when commiting to SOLR: " + runningContext.toString() + ": "
                        + e.getMessage() + " :: " + e1.getMessage());
            }
        }
    }

    public void run() {
        logger.info("WORKER: " + getName() + " :: STARTED");
        try {
            preRun();
            execute();
            postRun();
        } catch (RunningSolrException e) {
            logger.error("WORKER: ERROR " + getName() + " Solr Exception occurred: " + e.getMessage());
        }
        logger.info("WORKER: " + getName() + " :: ENDED");
    }
}
