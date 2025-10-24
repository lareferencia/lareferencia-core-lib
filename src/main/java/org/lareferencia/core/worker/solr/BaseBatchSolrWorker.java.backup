
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

//TODO verify if this is working as excepted
/**
 * This class can be used to process solr pagging processing
 * 
 * @author pgraca
 *
 * @param <I>
 * @param <C>
 */
public abstract class BaseBatchSolrWorker<I, C extends IRunningContext> extends BaseSolrWorker<C>
        implements IBatchWorker<I, C> {

    private static Logger logger = LogManager.getLogger(BaseBatchSolrWorker.class);

    private static final int DEFAULT_PAGE_SIZE = 100;

    @Getter
    @Setter
    private int pageSize = DEFAULT_PAGE_SIZE;

    @Setter
    protected IPaginator<I> paginator;

    @Getter
    private int totalPages = 1;

    @Getter
    private int actualPage = 0;

    private boolean wasStopped = false;

    @Override
    public abstract void processItem(I item);

    @Override
    public abstract void prePage();

    @Override
    public abstract void postPage();

    @Override
    protected abstract void postRun();

    @Override
    protected abstract void preRun();

    public BaseBatchSolrWorker(String solrURL) {
        super(solrURL);
    }

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
