
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
 * Exception thrown when an error occurs during pagination operations.
 * Indicates failures in page-based data iteration or retrieval.
 */
public class PaginatorException extends RuntimeException {

	/**
	 * Constructs a new paginator exception with the specified detail message.
	 * 
	 * @param string the detail message describing the pagination error
	 */
	public PaginatorException(String string) {
		super(string);
	}

	/**
	 * Constructs a new paginator exception with the specified detail message and
	 * cause.
	 * 
	 * @param string the detail message describing the pagination error
	 * @param cause  the underlying cause of the exception
	 */
	public PaginatorException(String string, Throwable cause) {
		super(string, cause);
	}

}
