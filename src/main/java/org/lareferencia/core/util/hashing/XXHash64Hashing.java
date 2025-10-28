
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

package org.lareferencia.core.util.hashing;

import net.openhft.hashing.LongHashFunction;

/**
 * Hashing implementation using the XXHash64 algorithm.
 * Provides high-performance non-cryptographic hashing for metadata strings
 * using the OpenHFT xxHash library.
 */
public class XXHash64Hashing implements IHashingHelper {

	/**
	 * Constructs a new XXHash64Hashing instance.
	 */
	public XXHash64Hashing() {
		super();
	}

	/**
	 * Calculates a hexadecimal hash string for the given metadata using XXHash64.
	 *
	 * @param metadata the metadata string to hash
	 * @return a 16-character hexadecimal string representing the hash value
	 */
	@Override
	public String calculateHash(String metadata) {
	   	return String.format("%016X", LongHashFunction.xx().hashBytes(  metadata.toString().getBytes() )) ;  

	}

	/**
	 * Calculates a numeric hash value for the given metadata using XXHash64.
	 *
	 * @param metadata the metadata string to hash
	 * @return a Long value representing the hash
	 */
	public static Long calculateHashLong(String metadata) {
	   	return LongHashFunction.xx().hashBytes(  metadata.toString().getBytes() );  
	}

}
