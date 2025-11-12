
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

import org.lareferencia.core.domain.OAIMetadata;
import org.lareferencia.core.repository.jpa.OAIMetadataRepository;
import org.lareferencia.core.util.hashing.IHashingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * SQL-based implementation of the metadata store interface.
 */
public class MetadataStoreSQLImpl implements IMetadataStore {
	
	/**
	 * Constructs a new SQL-based metadata store.
	 */
	public MetadataStoreSQLImpl() {
		// Default constructor
	}

	@Autowired
	OAIMetadataRepository mdRepository;
	
	@Autowired 
	IHashingHelper hashing;

	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public synchronized String storeAndReturnHash(SnapshotMetadata snapshotMetadata, String metadata) {
		
		String hash = hashing.calculateHash(metadata);
		
		if ( !mdRepository.checkIfExists(hash) ) {
			OAIMetadata md = new OAIMetadata(metadata, hash);
			mdRepository.saveAndFlush(md);
		}
			
		return hash;
	}

	@Override
	public String getMetadata(SnapshotMetadata snapshotMetadata, String hash) throws MetadataRecordStoreException {
		
		String metadata = mdRepository.getMetadata(hash);
		
		if ( metadata != null )
			return metadata;
		else throw new MetadataRecordStoreException("Metadata with hash " + hash + " not found in store.");
	}

	@Override
	public Boolean cleanAndOptimizeStore() {
		mdRepository.deleteOrphanMetadataEntries();
		return true;
	}


}
