
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

import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.domain.SnapshotIndexStatus;
import org.lareferencia.backend.domain.SnapshotStatus;
import org.lareferencia.core.worker.IPaginator;


public interface IMetadataRecordStoreService {
	
	public Long createSnapshot(Network network);
	public void saveSnapshot(Long snapshotId);
	public void resetSnapshotValidationCounts(Long snapshot) throws MetadataRecordStoreException;
	public void cleanSnapshotData(Long snapshotId);
	public void deleteSnapshot(Long snapshotId);
	public List<Long> listSnapshotsIds(Long networkId, boolean includeDeleted);

	
	public void updateSnapshotStatus(Long snapshot, SnapshotStatus status);
	public void updateSnapshotIndexStatus(Long snapshot, SnapshotIndexStatus status);
	public void updateSnapshotStartDatestamp(Long snapshot, LocalDateTime datestamp);
	public void updateSnapshotLastIncrementalDatestamp(Long snapshot, LocalDateTime datestamp);
	public void updateSnapshotEndDatestamp(Long snapshot, LocalDateTime datestamp);
	
	public SnapshotStatus getSnapshotStatus(Long snapshot);
	public SnapshotIndexStatus getSnapshotIndexStatus(Long snapshot);
	public LocalDateTime getSnapshotStartDatestamp(Long snapshot);
	public LocalDateTime getSnapshotEndDatestamp(Long snapshot);
	public LocalDateTime getSnapshotLastIncrementalDatestamp(Long snapshot);
	public Integer getSnapshotSize(Long snapshot);


	//public void updateSnapshot(Long snapshot);
	
	public Long findLastGoodKnownSnapshot(Network network);
	public Long findLastHarvestingSnapshot(Network network);

	public OAIRecord findRecordByIdentifier(Long snapshot, String oaiIdentifier);
	public OAIRecord findRecordByRecordId(Long recordId);

	public OAIRecord createRecord(Long snapshot, OAIRecordMetadata metadata) throws MetadataRecordStoreException;
	public OAIRecord createDeletedRecord(Long snapshotId, String identifier, LocalDateTime dateStamp) throws MetadataRecordStoreException;


	public OAIRecord updateRecordStatus(OAIRecord record, RecordStatus status, Boolean wasTransformed);

	public OAIRecord updateOriginalMetadata(OAIRecord record, OAIRecordMetadata metadata);
	public OAIRecordMetadata getOriginalMetadata(OAIRecord record) throws OAIRecordMetadataParseException, MetadataRecordStoreException;
	
	public OAIRecord updatePublishedMetadata(OAIRecord record, OAIRecordMetadata metadata);
	public OAIRecordMetadata getPublishedMetadata(OAIRecord record) throws OAIRecordMetadataParseException, MetadataRecordStoreException;
	
	public IPaginator<OAIRecord> getUntestedRecordsPaginator(Long snapshot);
	public IPaginator<OAIRecord> getNotDeletedRecordsPaginator(Long snapshot);
	public IPaginator<OAIRecord> getNotInvalidRecordsPaginator(Long snapshot);
	public IPaginator<OAIRecord> getValidRecordsPaginator(Long snapshot);
	public IPaginator<OAIRecord> getUpdatedRecordsPaginator(Long snapshot) throws MetadataRecordStoreException;
	
	public IPaginator<String> getRecordIdentifiersPaginator(Long snapshot, RecordStatus status);

	void copyNotDeletedRecordsFromSnapshot(Long previousSnapshotId, Long snapshotId);

}
