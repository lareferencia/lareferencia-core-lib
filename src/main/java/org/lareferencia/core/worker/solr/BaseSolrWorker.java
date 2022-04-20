
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

public abstract class BaseSolrWorker<C extends IRunningContext> extends BaseWorker<C> {
    protected HttpSolrClient solrClient;
    private static Logger logger = LogManager.getLogger(BaseSolrWorker.class);

    public BaseSolrWorker(String solrURL) {
        super();
        this.solrClient = new HttpSolrClient.Builder(solrURL).build();
    }

    protected void sendUpdateToSolr(String data)
            throws SolrServerException, IOException, HttpSolrClient.RemoteSolrException {
        DirectXmlRequest request = new DirectXmlRequest("/update", data);
        solrClient.request(request);
    }

    protected void solrCommit() throws Exception {
        this.sendUpdateToSolr("<commit/>");
    }

    protected void solrRollback() throws Exception {
        this.sendUpdateToSolr("<rollback/>");
    }

    protected void deleteByField(String field, String value) throws Exception {
        this.sendUpdateToSolr("<delete><query>" + field + ":" + value + "</query></delete>");
    }

    protected void preRun() throws RunningSolrException {

    }

    protected abstract void execute() throws RunningSolrException;

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
