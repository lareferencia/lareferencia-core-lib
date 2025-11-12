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

package org.lareferencia.core.worker.harvesting;

import java.util.List;
import java.util.Map;

/**
 * Interface for OAI-PMH harvester implementations.
 * 
 * @author LA Referencia Team
 */
public interface IHarvester extends IHarvestingEventSource {

	/**
	 * Performs a harvest operation from an OAI-PMH endpoint.
	 * 
	 * @param originURL the base URL of the OAI-PMH repository
	 * @param set the set spec to harvest (or null for all sets)
	 * @param metadataPrefix the metadata format to request
	 * @param metadataStoreSchema the schema for storing metadata
	 * @param from the earliest datestamp for selective harvesting
	 * @param until the latest datestamp for selective harvesting
	 * @param resumptionToken token for resuming incomplete harvests
	 * @param maxRetries maximum number of retry attempts on errors
	 */
	public void harvest(String originURL, String set, String metadataPrefix, String metadataStoreSchema, String from, String until, String resumptionToken, int maxRetries);
	
	/**
	 * Retrieves identify information from the OAI-PMH repository.
	 * 
	 * @param originURL the base URL of the OAI-PMH repository
	 * @return map of identify response fields
	 */
	public Map<String, String> identify(String originURL);
	
	/**
	 * Lists all sets available in the OAI-PMH repository.
	 * 
	 * @param uri the base URL of the OAI-PMH repository
	 * @return list of set specs
	 */
	public List<String> listSets(String uri);

	/**
	 * Stops the current harvesting operation.
	 */
	public void stop();

	/**
	 * Resets the harvester to initial state.
	 */
	public void reset();

}
