
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

package org.lareferencia.backend.workers.indexer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.worker.solr.BaseSolrWorker;
import lombok.Getter;
import lombok.Setter;

public class FullUnindexerWorker extends BaseSolrWorker<NetworkRunningContext> {

    private static Logger logger = LogManager.getLogger(IndexerWorker.class);

    @Getter
    @Setter
    private String solrNetworkIDField;

    @Getter
    @Setter
    private String solrRecordIDField = "id";

    public FullUnindexerWorker(String solrURL) {
        super(solrURL);
    }

    @Override
    public void execute() {
        String networkAcronym = runningContext.getNetwork().getAcronym();

        logger.debug("Executing index deletion (without indexing): " + networkAcronym);
        logInfo("WORKER:" + this.getName() + ":  UnIndex:: by: " + this.solrNetworkIDField + ":"
                + networkAcronym);

        try {
            deleteByField(this.solrNetworkIDField, networkAcronym);
        } catch (Exception e) {
            error();
            logError("WORKER:" + this.getName() + ": UnIndex:: by: " + this.solrNetworkIDField + ":"
                    + networkAcronym + " :: " + e.getMessage());
        }
    }

    /******************* Auxiliares ********** */
    private void error() {
        this.stop();
    }

    private void logError(String message) {
        logger.error(message);
    }

    private void logInfo(String message) {
        logger.info(message);
    }

    @Override
    public String toString() {
        return "UnIndexer[Delete by " + this.solrNetworkIDField + "]";
    }

}
