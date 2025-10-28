
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

package org.lareferencia.core.metadata;

/**
 * Exception thrown when an error occurs in metadata record store operations.
 * This exception is used to indicate failures in metadata storage, retrieval, or manipulation.
 */
public class MetadataRecordStoreException extends Exception {

	/**
	 * Constructs a new metadata record store exception with no detail message.
	 */
	public MetadataRecordStoreException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Constructs a new metadata record store exception with the specified detail message.
	 * 
	 * @param arg0 the detail message describing the error
	 */
	public MetadataRecordStoreException(String arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Constructs a new metadata record store exception with the specified cause.
	 * 
	 * @param cause the cause of the exception
	 */
	public MetadataRecordStoreException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Constructs a new metadata record store exception with the specified detail message and cause.
	 * 
	 * @param message the detail message describing the error
	 * @param cause the cause of the exception
	 */
	public MetadataRecordStoreException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Constructs a new metadata record store exception with the specified detail message, cause,
	 * suppression enablement, and writable stack trace settings.
	 * 
	 * @param message the detail message describing the error
	 * @param cause the cause of the exception
	 * @param enableSuppression whether suppression is enabled or disabled
	 * @param writableStackTrace whether the stack trace should be writable
	 */
	public MetadataRecordStoreException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
