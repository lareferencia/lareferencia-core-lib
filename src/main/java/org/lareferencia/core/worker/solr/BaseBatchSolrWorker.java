
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

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.worker.IBatchWorker;
import org.lareferencia.core.worker.IPaginator;
import org.lareferencia.core.worker.IRunningContext;
import org.lareferencia.core.worker.WorkerRuntimeException;
import org.springframework.data.domain.Page;
import lombok.Getter;
import lombok.Setter;

/**
 * Base class for batch workers that interact with Solr in pages.
 * <p>
 * Combines Solr interaction capabilities with batch processing,
 * allowing pagination through large result sets from Solr.
 * </p>
 * 
 * @param <I> the type of items being processed
 * @param <C> the running context type
 * @author LA Referencia Team
 * @see BaseSolrWorker
 * @see IBatchWorker
 */
public abstract class BaseBatchSolrWorker<I, C extends IRunningContext> extends BaseSolrWorker<C>
        implements IBatchWorker<I, C> {

    private static Logger logger = LogManager.getLogger(BaseBatchSolrWorker.class);

    private static final int DEFAULT_PAGE_SIZE = 100;

    @Getter
    @Setter
    private int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Paginator for iterating through Solr results in pages.
     */
    @Setter
    protected IPaginator<I> paginator;

    /**
     * Total number of pages to process.
     */
    @Getter
    private int totalPages = 1;

    /**
     * Current page number being processed.
     */
    @Getter
    private int actualPage = 0;

    private boolean wasStopped = false;

    /**
     * Processes a single item from the current page.
     * 
     * @param item the item to process
     */
    @Override
    public abstract void processItem(I item);

    /**
     * Executes actions before processing each page.
     */
    @Override
    public abstract void prePage();

    /**
     * Executes actions after processing each page.
     */
    @Override
    public abstract void postPage();

    /**
     * Cleanup actions after all processing completes.
     */
    @Override
    protected abstract void postRun();

    /**
     * Initialization actions before processing starts.
     */
    @Override
    protected abstract void preRun();

    /**
     * Creates a batch Solr worker with the specified Solr URL.
     * 
     * @param solrURL the Solr server URL
     */
    public BaseBatchSolrWorker(String solrURL) {
        super(solrURL);
    }

    /**
     * Executes the batch processing workflow across all pages.
     */
    @Override
    public synchronized void run() {
        logger.info("WORKER: " + getName() + " :: START processing: " + runningContext.toString());

        preRun();

        if (paginator != null) {

            totalPages = paginator.getTotalPages();

            for (actualPage = 1; actualPage <= totalPages && !wasStopped; actualPage++) {

                logger.info("WORKER: " + getName() + " :: Processing page: " + actualPage + " of " + totalPages);

                try {

                    prePage();

                    Page<I> page = paginator.nextPage();
                    List<I> items = page.getContent();

                    for (I item : items) {

                        if (wasStopped)
                            break;

                        try {
                            processItem(item);
                        } catch (Exception e) {
                            throw new WorkerRuntimeException(
                                    "Runtime error processing in item: " + item.toString() + " : " + e.getMessage());
                        }

                    }

                    if (!wasStopped) { // if wasnt stopped in the middle of the page
                        postPage();
                    }

                } catch (Exception e) {
                    this.stop();

                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t,
                            new WorkerRuntimeException("BaseBatchSolrWorker runtime error processing in page: "
                                    + actualPage + " : " + e.getMessage()));
                }

            }

            if (!wasStopped) {
                postRun();
            }
        }
        logger.info("WORKER: " + getName() + " :: END processing total of " + totalPages + " pages: "
                + runningContext.toString());
    }

    @Override
    public void stop() {
        wasStopped = true;
        super.stop();
        try {
            this.solrRollback();
            logger.info("WORKER: " + getName() + " :: STOP and Rollback SOLR: " + runningContext.toString());
        } catch (Exception e) {
            logger.error("WORKER: " + getName() + "Issues when commiting to SOLR: " + runningContext.toString() + ": "
                    + e.getMessage());
        }
    }

    @Override
    public double getCompletionRate() {
        return (totalPages == 0) ? 1d : Double.valueOf(actualPage) / totalPages;
    }

}
