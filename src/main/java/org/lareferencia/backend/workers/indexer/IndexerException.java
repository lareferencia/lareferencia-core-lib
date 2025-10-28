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

/**
 * Exception thrown during indexing operations.
 * 
 * @author LA Referencia Team
 */
public class IndexerException extends Exception {

	private static final long serialVersionUID = -5913401095836497654L;

	/**
	 * Creates a new indexer exception.
	 */
	public IndexerException() {
		super();
	}

	/**
	 * Creates a new indexer exception with a message.
	 * 
	 * @param message the error message
	 */
	public IndexerException(String message) {
		super(message);
	}

	/**
	 * Creates a new indexer exception with a message and cause.
	 * 
	 * @param message the error message
	 * @param cause the underlying cause
	 */
	public IndexerException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Creates a new indexer exception with a cause.
	 * 
	 * @param cause the underlying cause
	 */
	public IndexerException(Throwable cause) {
		super(cause);
	}
}
