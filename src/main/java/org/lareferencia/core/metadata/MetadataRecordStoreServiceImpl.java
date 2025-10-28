
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

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.domain.NetworkSnapshot;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.domain.SnapshotIndexStatus;
import org.lareferencia.backend.domain.SnapshotStatus;
import org.lareferencia.backend.repositories.jpa.NetworkRepository;
import org.lareferencia.backend.repositories.jpa.NetworkSnapshotRepository;
import org.lareferencia.backend.repositories.jpa.OAIRecordRepository;
import org.lareferencia.backend.services.SnapshotLogService;
import org.lareferencia.core.util.hashing.IHashingHelper;
import org.lareferencia.core.worker.IPaginator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import lombok.Getter;

/**
 * Implementation of the metadata record store service for managing network snapshots and OAI records.
 * Provides operations for creating, retrieving, updating, and deleting snapshots and their associated records.
 * Manages the storage and retrieval of metadata using both SQL database and file system storage.
 */
public class MetadataRecordStoreServiceImpl implements IMetadataRecordStoreService {
	
	private static final Logger logger = LogManager.getLogger(MetadataRecordStoreServiceImpl.class);
	private static final int MAX_IDENTIFIER_LENGTH = 255 ;

	ConcurrentHashMap<Long, NetworkSnapshot> snapshotMap = new ConcurrentHashMap<>();
	
	private NetworkSnapshot getSnapshot(Long snapshotId) throws MetadataRecordStoreException {
		
		NetworkSnapshot snapshot = snapshotMap.get(snapshotId);
		
		if ( snapshot == null ) {
			Optional<NetworkSnapshot> optSnapshot = snapshotRepository.findById(snapshotId);
			if ( optSnapshot.isPresent() )
				snapshot = optSnapshot.get();
			else
				throw new MetadataRecordStoreException("Snapshot: " + snapshotId + " not found.");
				
			snapshotMap.put(snapshotId, snapshot);
		}
		
		return snapshot;	
	}
	
	private void putSnapshot(NetworkSnapshot snapshot) {
		snapshotMap.put(snapshot.getId(), snapshot);
	}

	private void deleteSnapshot(NetworkSnapshot snapshot) {
		snapshotMap.remove(snapshot.getId());
	}


	@Autowired 
	IHashingHelper hashing;

	@Autowired
	NetworkRepository networkRepository;

	@Autowired
	NetworkSnapshotRepository snapshotRepository;

	@Autowired
	OAIRecordRepository recordRepository;
	
	@Autowired
	IMetadataStore metadataStore;
	
	@Autowired
	private SnapshotLogService snapshotLogService;

	/**
	 * Constructs a new metadata record store service implementation.
	 * Dependencies are injected via Spring's autowiring mechanism.
	 */
	public MetadataRecordStoreServiceImpl() {
		// Default constructor for Spring dependency injection
	}
	
	@Override
	public Long createSnapshot(Network network) {

		NetworkSnapshot snapshot = new NetworkSnapshot();
		snapshot.setNetwork(network);
		snapshot.setStartTime(LocalDateTime.now());
		snapshotRepository.save(snapshot);

		// save in cache
		putSnapshot(snapshot);
		
		return snapshot.getId();
	};
	
	@Override
	public void saveSnapshot(Long snapshotId) {

		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			snapshotRepository.save(snapshot);
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::saveSnapshot::"+e.getMessage());
			
		}
	};
	
	@Override
	public void deleteSnapshot(Long snapshotId) {

		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			
			// delete log entries
			snapshotLogService.deleteSnapshotLog(snapshotId);
			
			// delete records
			recordRepository.deleteBySnapshotID(snapshotId);
			
			// delete snapshot
			snapshotRepository.deleteBySnapshotID(snapshotId);

			// delete from internal map
			deleteSnapshot(snapshot);
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::deleteSnapshot::"+e.getMessage());
			
		}
	};
	
	/**
	 * Cleans snapshot data by deleting log entries and records.
	 * For valid snapshots, marks them as deleted; for failed snapshots, removes them entirely.
	 * 
	 * @param snapshotId the ID of the snapshot to clean
	 */
	@Override
	public void cleanSnapshotData(Long snapshotId) {

		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			
			// delete log entries
			snapshotLogService.deleteSnapshotLog(snapshotId);
			
			// delete records
			recordRepository.deleteBySnapshotID(snapshotId);
			
			if ( snapshot.getStatus().equals(SnapshotStatus.VALID) || snapshot.getStatus().equals(SnapshotStatus.HARVESTING_FINISHED_VALID) ) {
				// marks as cleaned
				snapshot.setDeleted(true);
				// save		
				snapshotRepository.save(snapshot);
			}	
			else // if is a failed snapshot then delete
				snapshotRepository.delete(snapshot);
				
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::cleanSnapshot::"+e.getMessage());
			
		}
	};

	public List<Long> listSnapshotsIds(Long networkId, boolean includeDeleted) {
		
		if ( includeDeleted )
			return snapshotRepository.findAllIdsByNetworkId(networkId);		
		else
			return snapshotRepository.findNonDeletedIdsByNetworkId(networkId);
	}

	
	
	@Override
	public SnapshotStatus getSnapshotStatus(Long snapshotId) {
		
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			return snapshot.getStatus();
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::getSnapshotStatus::"+e.getMessage());
			return null;
		}
	
	};
	
	@Override
	public SnapshotIndexStatus getSnapshotIndexStatus(Long snapshotId) {
		
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			return snapshot.getIndexStatus();
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::getSnapshotStatus::"+e.getMessage());
			return null;
		}
	
	};

	@Override
	public Long getPreviousSnapshotId(Long snapshotId) {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			return snapshot.getPreviousSnapshotId();

		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::getPreviousSnapshotId::"+e.getMessage());
			return null;
		}
	}

	/**
	 * Sets the ID of the previous snapshot for incremental harvest tracking.
	 * 
	 * @param snapshotId the ID of the current snapshot
	 * @param previousSnapshotId the ID of the previous snapshot to reference
	 */
	@Override
	public void setPreviousSnapshotId(Long snapshotId, Long previousSnapshotId) {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			snapshot.setPreviousSnapshotId(previousSnapshotId);
			snapshotRepository.save(snapshot);

		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::setPreviousSnapshotId::"+e.getMessage());
		}
	}

	/**
	 * Optimizes the metadata store by cleaning unused data and performing optimization operations.
	 */
	@Override
	public void optimizeStore() {
		metadataStore.cleanAndOptimizeStore();
	}

	/**
	 * Resets validation counts for a snapshot, preparing it for re-validation.
	 * Sets valid and transformed sizes to zero, status to HARVESTING_FINISHED_VALID,
	 * and index status to UNKNOWN.
	 * 
	 * @param snapshotId the ID of the snapshot to reset
	 * @throws MetadataRecordStoreException if the snapshot cannot be accessed
	 */
	@Override
	public void resetSnapshotValidationCounts(Long snapshotId) throws MetadataRecordStoreException {
		NetworkSnapshot snapshot = getSnapshot(snapshotId);
		snapshot.setValidSize(0);
		snapshot.setTransformedSize(0);
		snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
		snapshot.setIndexStatus(SnapshotIndexStatus.UNKNOWN);
		snapshotRepository.save(snapshot);
	}



	@Override
	public Long findLastGoodKnownSnapshot(Network network) {
		
		if ( network == null || network.getId() == null )
			return null;
		
		NetworkSnapshot snapshot = snapshotRepository.findLastGoodKnowByNetworkID(network.getId()); 
		
		if ( snapshot != null )
			return snapshot.getId();
		else 
			return null;
	};

	@Override
	public Long findLastHarvestingSnapshot(Network network) {
		
		if ( network == null || network.getId() == null )
			return null;
		
		NetworkSnapshot snapshot = snapshotRepository.findLastHarvestedByNetworkID(network.getId());
		
		if ( snapshot != null )
			return snapshot.getId();
		else 
			return null;
	};
	
	
	/**************************** Records 
	 * @throws MetadataRecordStoreException ********************************************/

	
	/**
	 * Creates a new metadata record in the specified snapshot.
	 * Stores the metadata content, creates the record entity, and associates it with the snapshot.
	 * 
	 * @param snapshotId the ID of the snapshot where the record will be created
	 * @param metadata the OAI record metadata to store
	 * @return the created OAI record entity
	 * @throws MetadataRecordStoreException if the snapshot cannot be accessed or record creation fails
	 */
	@Override
	public OAIRecord createRecord(Long snapshotId, OAIRecordMetadata metadata) throws MetadataRecordStoreException {

		NetworkSnapshot snapshot = getSnapshot(snapshotId);

		String hash = metadataStore.storeAndReturnHash(metadata.toString());
		
		OAIRecord record = new OAIRecord(snapshot);
		record.setDatestamp( metadata.getDatestamp() );

		record.setIdentifier( StringUtils.substring(metadata.getIdentifier(), 0, MAX_IDENTIFIER_LENGTH-1) );

		record.setOriginalMetadataHash( hash );
		
		snapshot.incrementSize();
		
		recordRepository.save(record);
		//snapshotRepository.save(snapshot);
		
		return record;

	};

	@Override
	public OAIRecord createDeletedRecord(Long snapshotId, String identifier, LocalDateTime dateStamp) throws MetadataRecordStoreException {

		NetworkSnapshot snapshot = getSnapshot(snapshotId);

		OAIRecord record = new OAIRecord(snapshot);
		record.setDatestamp( dateStamp );
		record.setIdentifier( identifier );
		record.setStatus( RecordStatus.DELETED );
		recordRepository.save(record);

		return record;
	};


	@Override
	public void copyNotDeletedRecordsFromSnapshot(Long previousSnapshotId, Long snapshotId) {

		snapshotRepository.copyNotDeletedRecordsFromSnapshot(previousSnapshotId, snapshotId);

		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);

			RecordStatus[] statuses = { RecordStatus.VALID, RecordStatus.INVALID, RecordStatus.UNTESTED };

			Long size = recordRepository.countBySnapshotAndStatusIn(snapshot, statuses);
			snapshot.setSize(size.intValue());

			Long validSize = recordRepository.countBySnapshotAndStatusIn(snapshot, new RecordStatus[] { RecordStatus.VALID });
			snapshot.setValidSize(validSize.intValue());

			Long transformedSize = recordRepository.countBySnapshotAndTransformed(snapshot, true);
			snapshot.setTransformedSize(transformedSize.intValue());

		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::copyNotDeletedRecordsFromSnapshot::"+e.getMessage());

		}

	}



	@Override
	public OAIRecord updateRecordStatus(OAIRecord record, RecordStatus status, Boolean wasTransformed) {
		
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(record.getSnapshotId());
		

			switch (status) {
		
			case VALID: // case new status valid 
				
				// if old status was invalid then increment valid size
				if ( record.getStatus().equals(RecordStatus.INVALID) || record.getStatus().equals(RecordStatus.UNTESTED)  )
					snapshot.incrementValidSize(); 
				break;
			
			case INVALID: 
				
				// if old status was valid then decrement valid size
				if ( record.getStatus().equals(RecordStatus.VALID) )
					snapshot.decrementValidSize();
				break;
			
			case DELETED:
				
				// if old status was valid then decrement valid size
				if ( record.getStatus().equals(RecordStatus.VALID) )
					snapshot.decrementValidSize();
				break;
				
			case UNTESTED:
				
				if ( record.getStatus().equals(RecordStatus.VALID) )
					snapshot.decrementValidSize(); 
				break;
			}
			
			
			if (wasTransformed != null ) { // only if transformed != null
				
				if ( wasTransformed && !record.getTransformed()) 
					snapshot.incrementTransformedSize();
				 
				if ( !wasTransformed && record.getTransformed()) 
					snapshot.decrementTransformedSize();
								
				record.setTransformed(wasTransformed);
			}	
			
			// set status
			record.setStatus(status);
			recordRepository.save(record);
		
		} catch (MetadataRecordStoreException e) {
			logger.debug("Error updating record status record: " + record.getPublishedMetadataHash() + " " + record.getIdentifier() + " snapshot: " + record.getSnapshotId() );
		}
		
		return record;
	}
	
	
	@Override
	public void updateSnapshotStatus(Long snapshotId, SnapshotStatus status)  {
		
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(status);
			//snapshotRepository.save(snapshot);
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::updateSnapshotStatus::"+e.getMessage());
		}
	
	};
	
	
	@Override
	public void updateSnapshotIndexStatus(Long snapshotId, SnapshotIndexStatus status)  {
		
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			snapshot.setIndexStatus(status);
			//snapshotRepository.save(snapshot);
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::updateSnapshotStatus::"+e.getMessage());
		}
	
	};

	
	
	@Override
	public void updateSnapshotStartDatestamp(Long snapshotId, LocalDateTime datestamp)  {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			snapshot.setStartTime(datestamp);
			//snapshotRepository.save(snapshot);
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::updateSnapshotStartDateStamp::"+e.getMessage());
		}		
	}
	
	
	@Override
	public void updateSnapshotEndDatestamp(Long snapshotId, LocalDateTime datestamp)  {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			snapshot.setEndTime(datestamp);
			//snapshotRepository.save(snapshot);
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::updateSnapshotEndDateStamp::"+e.getMessage());
		}		
	}
	
	
	@Override
	public void updateSnapshotLastIncrementalDatestamp(Long snapshotId, LocalDateTime datestamp)  {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			snapshot.setLastIncrementalTime(datestamp);
			//snapshotRepository.save(snapshot);
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::updateSnapshotEndDateStamp::"+e.getMessage());
		}		
	}
	
	
	@Override
	public LocalDateTime getSnapshotStartDatestamp(Long snapshotId)  {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			return snapshot.getStartTime();
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::getSnapshotStartDateStamp::"+e.getMessage());
			return null;
		}		
	}
	
	@Override
	public LocalDateTime getSnapshotEndDatestamp(Long snapshotId)  {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			return snapshot.getEndTime();
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::getSnapshotEndDateStamp::"+e.getMessage());
			return null;
		}		
	}
	
	@Override
	public LocalDateTime getSnapshotLastIncrementalDatestamp(Long snapshotId)  {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			return snapshot.getLastIncrementalTime();
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::getSnapshotLastIncrementalDateStamp::"+e.getMessage());
			return null;
		}		
	}

	
	@Override
	public Integer getSnapshotSize(Long snapshotId) {
		NetworkSnapshot snapshot;
		try {
			snapshot = getSnapshot(snapshotId);
			return snapshot.getSize();
			
		} catch (MetadataRecordStoreException e) {
			logger.error("MetadataRecordStore::getSnapshotLastIncrementalDateStamp::"+e.getMessage());
			return null;
		}		
	}

	/////////////////////////////// Metadata //////////////////////////////////////////////////
	
	
	@Override
	public OAIRecord updateOriginalMetadata(OAIRecord record, OAIRecordMetadata metadata) {

		String metadataStr = metadata.toString();
		String newHash = hashing.calculateHash(metadataStr);
		
		// if new metadata hash is new the update, if not do nothing
		if ( newHash != record.getOriginalMetadataHash() ) {
			logger.debug( "Updating metadata record: " + record.getId() );
			record.setOriginalMetadataHash( metadataStore.storeAndReturnHash(metadataStr) );
			updateRecordStatus(record, RecordStatus.UNTESTED, false);
			record.setDatestamp( metadata.getDatestamp() );
		} else
			logger.debug( "Trying to update metadata record: " + record.getId() + " w/ same hash - skipping update" );
		
		recordRepository.save(record);
		return record;
	};
	
	
	@Override
	public OAIRecord updatePublishedMetadata(OAIRecord record, OAIRecordMetadata metadata) {

		String metadataStr = metadata.toString();
		String newHash = hashing.calculateHash(metadataStr);
		
		// if new metadata hash is new the update, if not do nothing
		if ( ! newHash.equals( record.getPublishedMetadataHash() ) ) {
			logger.debug( "Updating metadata record: " + record.getId() );
			record.setPublishedMetadataHash( metadataStore.storeAndReturnHash(metadataStr) );
			record.setDatestamp( metadata.getDatestamp() );
		} else
			logger.debug( "Trying to update metadata record: " + record.getId() + " w/ same hash - skipping update" );

		recordRepository.save(record);
		return record;
	};
	

	@Override
	public OAIRecordMetadata getOriginalMetadata(OAIRecord record) throws OAIRecordMetadataParseException, MetadataRecordStoreException {
				
		String xmlString = metadataStore.getMetadata( record.getOriginalMetadataHash() );
		
		if ( xmlString != null && xmlString.length() > 0) {
			OAIRecordMetadata metadata = new OAIRecordMetadata(record.getIdentifier(), xmlString);
			metadata.setDatestamp( record.getDatestamp() );
			return metadata;
		}
		
		throw new OAIRecordMetadataParseException("Error parsing record XML metadata - null o empty original metadata: " + record.getId() + " :: " + record.getIdentifier());
	}

	@Override
	public OAIRecordMetadata getPublishedMetadata(OAIRecord record) throws OAIRecordMetadataParseException, MetadataRecordStoreException {
		
		String xmlString = metadataStore.getMetadata( record.getPublishedMetadataHash() );
		
		if ( xmlString == null || xmlString.trim().length() == 0) 
			xmlString = metadataStore.getMetadata( record.getOriginalMetadataHash() );
		
		if ( xmlString != null && xmlString.length() > 0) { 
			OAIRecordMetadata metadata = new OAIRecordMetadata(record.getIdentifier(), xmlString);
			metadata.setDatestamp( record.getDatestamp() );
			return metadata;
		}
		
		throw new OAIRecordMetadataParseException("Error parsing record XML metadata null o empty metadata" + record.getId() + " :: " + record.getIdentifier());
	}


	@Override
	public OAIRecord findRecordByIdentifier(Long snapshotId, String oaiIdentifier) {
		return recordRepository.findOneBySnapshotIdAndIdentifier(snapshotId, oaiIdentifier);
	}
	
	@Override
	public OAIRecord findRecordByRecordId(Long recordId) {
		return recordRepository.findById(recordId).get();
	}

	@Override
	public IPaginator<OAIRecord> getUntestedRecordsPaginator(Long snapshotId) {

		return new RecordPaginator(snapshotId, RecordStatus.UNTESTED);
	}

	@Override
	public IPaginator<OAIRecord> getNotDeletedRecordsPaginator(Long snapshotId) {
		return new RecordPaginator(snapshotId, RecordStatus.DELETED, true);
	}

	@Override
	public IPaginator<OAIRecord> getDeletedRecordsPaginator(Long snapshotId) {
		return new RecordPaginator(snapshotId, RecordStatus.DELETED, false);
	}
	
	@Override
	public IPaginator<OAIRecord> getNotInvalidRecordsPaginator(Long snapshotId) {
		return new RecordPaginator(snapshotId, RecordStatus.INVALID, true);
	}

	@Override
	public IPaginator<OAIRecord> getUpdatedRecordsPaginator(Long snapshotId) throws MetadataRecordStoreException {
		NetworkSnapshot snapshot = getSnapshot(snapshotId);
		return new RecordPaginator(snapshotId, snapshot.getLastIncrementalTime());
	}

	
	@Override
	public IPaginator<OAIRecord> getValidRecordsPaginator(Long snapshotId) {
		return new RecordPaginator(snapshotId, RecordStatus.VALID);
	} 
	
	@Override
	public IPaginator<String> getRecordIdentifiersPaginator(Long snapshotId, RecordStatus status) {
		return new RecordIdentifierPaginator(snapshotId, status);
	}

	/**
	 * Paginator implementation for iterating through OAI records in a snapshot.
	 * Provides page-by-page access to records with configurable page size and filtering by status.
	 */
	public class RecordPaginator implements IPaginator<OAIRecord> {

		// implements dummy starting page
		@Override
		public int getStartingPage() { return 1; }

		private static final int DEFAULT_PAGE_SIZE = 1000;

		@Getter
		private int pageSize = DEFAULT_PAGE_SIZE;

		// private IMetadataRecordRepository recordRepository;

		private int totalPages = 0;
		private Long lastRecordID = -1L;
		private Long snapshotID;
		private RecordStatus status;
		private boolean negateStatus = false;

		private LocalDateTime from;

		/**
		 * Creates a new record paginator for iterating through metadata records.
		 * <p>
		 * This paginated request always asks for the first page restricted to records
		 * with an ID greater than the last one processed.
		 * </p>
		 *
		 * @param snapshotID the snapshot ID to filter records
		 * @param status the record status to filter by
		 * @param negateStatus if true, negates the status filter
		 * @param from the starting date/time for filtering records
		 */
		public RecordPaginator(Long snapshotID, RecordStatus status,
				boolean negateStatus, LocalDateTime from) {

			logger.debug("RecordPaginator::snapshotID: " + snapshotID + " status: " + status + " negateStatus: " + negateStatus + " from: " + from);

			this.negateStatus = negateStatus;
			this.lastRecordID = -1L;
			this.snapshotID = snapshotID;
			this.status = status;
			this.from = from;

			obtainPage();
		}

		@Override
		public void setPageSize(int size) {
			// only if size is different from current change it
			if ( size != this.pageSize ) {
				this.pageSize = size;
				obtainPage();
			}
		}

		private Page<OAIRecord> obtainPage() {

			Page<OAIRecord> page = null;

			if (status == null) { // caso sin status

				if (from == null) { // no date and no status
					logger.debug("In case of no date and no status");
					logger.debug("RecordPaginator::obtainPage::snapshotID: " + snapshotID + " lastRecordID: " + lastRecordID + " pageSize: " + pageSize);
					//page = recordRepository.findBySnapshotIdOptimizedOrderByIdAsc(snapshotID, lastRecordID, PageRequest.of(0, pageSize));
					page = recordRepository.findBySnapshotIdAndIdGreaterThanOrderByIdAsc(snapshotID, lastRecordID, PageRequest.of(0, pageSize));
				}
				else { // date no status
					logger.debug("In case of date and no status");
					logger.debug("RecordPaginator::obtainPage::snapshotID: " + snapshotID + " from: " + from + " lastRecordID: " + lastRecordID + " pageSize: " + pageSize);
					//page = recordRepository.findBySnapshotIdAndDateOptimizedOrderByIdAsc(snapshotID, from, lastRecordID, PageRequest.of(0, pageSize));
					page = recordRepository.findBySnapshotIdAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(snapshotID, from, lastRecordID, PageRequest.of(0, pageSize));
				}

			} else { // caso con status

				if (from == null) { // no date and status
					logger.debug("In case of no date and status");
					logger.debug("RecordPaginator::obtainPage::snapshotID: " + snapshotID + " status: " + status + " lastRecordID: " + lastRecordID + " pageSize: " + pageSize);

					//page = recordRepository.findBySnapshotIdAndStatusOptimizedOrderByIdAsc(snapshotID, status, negateStatus, lastRecordID, PageRequest.of(0, pageSize));
					if ( negateStatus )
						page = recordRepository.findBySnapshotIdAndStatusNotAndIdGreaterThanOrderByIdAsc(snapshotID, status, lastRecordID, PageRequest.of(0, pageSize));
					else
						page = recordRepository.findBySnapshotIdAndStatusAndIdGreaterThanOrderByIdAsc(snapshotID, status, lastRecordID, PageRequest.of(0, pageSize));

				}
				else { // date and status
					logger.debug("In case of date and status");
					logger.debug("RecordPaginator::obtainPage::snapshotID: " + snapshotID + " status: " + status + " from: " + from + " lastRecordID: " + lastRecordID + " pageSize: " + pageSize);
					//page = recordRepository.findBySnapshotIdAndStatusAndDateOptimizedOrderByIdAsc(snapshotID, status, negateStatus, from, lastRecordID, PageRequest.of(0, pageSize));
					if ( negateStatus )
						page = recordRepository.findBySnapshotIdAndStatusNotAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(snapshotID, status, from, lastRecordID, PageRequest.of(0, pageSize));
					else
						page = recordRepository.findBySnapshotIdAndStatusAndDatestampGreaterThanEqualAndIdGreaterThanOrderByIdAsc(snapshotID, status, from, lastRecordID, PageRequest.of(0, pageSize));
				}

			}

			this.totalPages = page.getTotalPages();
			return page;

		}

		/**
		 * Constructs a record paginator for records with a specific status.
		 * 
		 * @param snapshotID the snapshot ID to paginate records from
		 * @param status the record status to filter by
		 * @param negateStatus whether to negate the status filter
		 */
		public RecordPaginator(Long snapshotID, RecordStatus status,
				boolean negateStatus) {
			this(snapshotID, status, negateStatus, null);
		}

		/**
		 * Constructs a record paginator for records with a specific status.
		 * 
		 * @param snapshotID the snapshot ID to paginate records from
		 * @param status the record status to filter by
		 */
		public RecordPaginator(Long snapshotID, RecordStatus status) {
			this(snapshotID, status, false);
		}

		/**
		 * Constructs a record paginator for all records in a snapshot.
		 * 
		 * @param snapshotID the snapshot ID to paginate records from
		 */
		public RecordPaginator(Long snapshotID) {
			this(snapshotID, null, false, null);
		}

		/**
		 * Constructs a record paginator for records created after a specific date.
		 * 
		 * @param snapshotID the snapshot ID to paginate records from
		 * @param from the date to filter records from
		 */
		public RecordPaginator(Long snapshotID, LocalDateTime from) {
			this(snapshotID, null, false, from);
		}

		/**
		 * Constructs an empty record paginator with no snapshot or status filter.
		 */
		public RecordPaginator() {

			this.totalPages = 0;
			this.status = null;

		}

		public int getTotalPages() {
			return totalPages;
		}

		public Page<OAIRecord> nextPage() {
			Page<OAIRecord> page = obtainPage();
			List<OAIRecord> records = page.getContent();
			lastRecordID = records.get(records.size() - 1).getId();
			return page;

		}

	}

	/**
	 * Paginator implementation for iterating through record identifiers in a snapshot.
	 * Provides page-by-page access to record identifiers with configurable page size and status filtering.
	 */
	public class RecordIdentifierPaginator implements IPaginator<String> {

		private static final int DEFAULT_PAGE_SIZE = 1000;

		// implements dummy starting page
		@Override
		public int getStartingPage() { return 1; }

		@Getter
		private int pageSize = DEFAULT_PAGE_SIZE;

		private int totalPages = 0;
		private int lastPageNumber = 0;
		private Long snapshotID;
		private RecordStatus status;

		@Override
		public void setPageSize(int size) {
			this.pageSize = size;
			this.lastPageNumber = 0;
			obtainPage();
		}

		private Page<String> obtainPage() {

			Page<String> page = recordRepository.listIdentifiersBySnapshotAndStatus(snapshotID, status, PageRequest.of(lastPageNumber, pageSize) );
			this.totalPages = page.getTotalPages();
			return page;

		}

		/**
		 * Constructs a record identifier paginator for records with a specific status.
		 * 
		 * @param snapshotId the snapshot ID to paginate identifiers from
		 * @param status the record status to filter by
		 */
		public RecordIdentifierPaginator(Long snapshotId, RecordStatus status) {
			this.snapshotID = snapshotId;
			this.status = status;
			this.totalPages = 0;
			this.lastPageNumber = 0;
			
			obtainPage();
		}

		public int getTotalPages() {
			return totalPages;
		}

		public Page<String> nextPage() {
			
			Page<String> page = obtainPage();
			lastPageNumber++;
			
			return page;
			
		}

	}


	











}
