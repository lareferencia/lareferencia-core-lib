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

package org.lareferencia.core.worker;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.List;

public abstract class BaseBatchWorker<I,C extends IRunningContext> extends BaseWorker<C> implements IBatchWorker<I,C> {
	
	private static Logger logger = LogManager.getLogger(BaseBatchWorker.class);
	
	
	@Autowired
	private PlatformTransactionManager transactionManager;

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
	
	public BaseBatchWorker(C context) {
		super(context);
	}
	
	public BaseBatchWorker() {
		super();
	}
	
	
	
	/**
	 * En caso de que varios threads quieran correr una misma instancia de este worker se asegura la exclusión
	 */
	@Override
	@Transactional(propagation = Propagation.NOT_SUPPORTED) // we're going to handle transactions manually
	public synchronized void run() {
		
		
		DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
		definition.setIsolationLevel(TransactionDefinition.ISOLATION_DEFAULT);

		logger.info("WORKER: "+ getName() +" :: START processing: " + runningContext.toString());
		
		preRun();
		
		if (paginator != null) {
			
			totalPages = paginator.getTotalPages();
			
			for (actualPage = paginator.getStartingPage(); actualPage <= totalPages && !wasStopped; actualPage++) {

				TransactionStatus transactionStatus = null;
				
				logger.info("WORKER: "+ getName() +" :: Processing page: " + actualPage + " of " + totalPages);
				
				try {
				
					transactionStatus = transactionManager.getTransaction(definition);
					
					prePage();
					
					Page<I> page = paginator.nextPage();				
					List<I> items = page.getContent();
	
	
					for (I item : items) {
						
						if ( wasStopped ) break; // detiene el ciclo si fue detenida
						
						try {
							processItem(item);
						} catch (Exception e) {
							throw new WorkerRuntimeException( "Runtime error processing in item: " + item.toString() + " : " + e.getClass().toString() + "::" + e.getMessage()  );
						}
	
					}
					
					if (!wasStopped) { // if wasnt stopped in the middle of the page
						postPage();
						transactionManager.commit(transactionStatus);
					} else
						transactionManager.rollback(transactionStatus);
					

				
				} catch (Exception e) {
					logger.error(e);
					this.stop();
					transactionManager.rollback(transactionStatus);

					Thread t = Thread.currentThread(); 
					t.getUncaughtExceptionHandler().uncaughtException(t, new WorkerRuntimeException( "BatchWorker runtime error processing in page: " + actualPage + " : "+ e.getMessage() ) );						
				}
			
			}
			
			if (!wasStopped)
				postRun();

		}
		logger.info("WORKER: "+ getName() +" :: END processing " + runningContext.toString());
	}
		
	protected abstract void postRun();
	protected abstract void preRun();

	@Override
	public void stop() {
		wasStopped = true;
		super.stop();
	}
	
	@Override
	public double getCompletionRate() {
		return (totalPages==0) ? 1d : Double.valueOf(actualPage)/totalPages;
	}
	

}
