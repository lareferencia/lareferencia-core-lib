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

import org.springframework.data.domain.Page;

/**
 * Interface for pagination of data sets in batch processing.
 * <p>
 * Provides methods to navigate through pages of data, enabling efficient
 * processing of large datasets in manageable chunks.
 * </p>
 * 
 * @param <T> the type of elements being paginated
 * @author LA Referencia Team
 */
public interface IPaginator<T> {

	/**
	 * Gets the starting page number for pagination.
	 * 
	 * @return the starting page number (typically 1 or 0)
	 */
	public int getStartingPage();
	
	/**
	 * Gets the total number of pages available.
	 * 
	 * @return the total page count
	 */
	public int getTotalPages();
	
	/**
	 * Retrieves the next page of data.
	 * 
	 * @return a Page containing the next set of elements
	 */
	public Page<T> nextPage();
	
	/**
	 * Sets the number of elements per page.
	 * 
	 * @param size the page size
	 */
	public void setPageSize(int size);
}
