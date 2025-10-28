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

/**
 * Interface for workers that process data in batches.
 * <p>
 * Batch workers iterate through pages of items, processing each item
 * and tracking completion progress. Supports pre/post page hooks.
 * </p>
 * 
 * @param <I> the type of items being processed
 * @param <C> the running context type
 * @author LA Referencia Team
 * @see IWorker
 * @see IPaginator
 */
public interface IBatchWorker<I,C extends IRunningContext> extends IWorker<C> {
	
	/**
	 * Processes a single item from the batch.
	 * 
	 * @param item the item to process
	 */
	void processItem(I item);
	
	/**
	 * Hook called before processing each page of items.
	 */
	void prePage();
	
	/**
	 * Hook called after processing each page of items.
	 */
	void postPage();
	
	/**
	 * Gets the completion rate as a percentage.
	 * 
	 * @return completion rate from 0.0 to 1.0
	 */
	double getCompletionRate();
	
	/**
	 * Sets the paginator for iterating through items.
	 * 
	 * @param paginator the paginator to use
	 */
	void setPaginator(IPaginator<I> paginator);
	
}
