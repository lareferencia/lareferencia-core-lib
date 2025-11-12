/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.domain.SnapshotStatus;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.repository.jpa.NetworkSnapshotRepository;
import org.lareferencia.core.repository.jpa.OAIRecordRepository;
import org.lareferencia.core.service.management.SnapshotLogService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Implementación SQL del store de snapshots.
 * 
 * RESPONSABILIDADES:
 * - Gestión de lifecycle de snapshots (crear, guardar, eliminar)
 * - Gestión de estados y timestamps
 * - Queries sobre snapshots
 * - Caché en memoria de snapshots para performance
 * 
 * SEPARACIÓN CON RECORD STORE:
 * - Este store NO gestiona records, solo metadata de snapshots
 * - Delegación a RecordStore para operaciones de records
 * - Mantiene contadores sincronizados (size, validSize, transformedSize)
 * 
 * THREAD SAFETY:
 * - ConcurrentHashMap para caché de snapshots
 * - Métodos synchronized en operaciones críticas
 */
public class SnapshotStoreSQLImpl implements ISnapshotStore {
	
	private static final Logger logger = LogManager.getLogger(SnapshotStoreSQLImpl.class);
	
	// Caché de snapshots en memoria para performance
	private ConcurrentHashMap<Long, NetworkSnapshot> snapshotCache = new ConcurrentHashMap<>();
	
	@Autowired
	private NetworkSnapshotRepository snapshotRepository;
	
	@Autowired
	private OAIRecordRepository recordRepository;
	
	@Autowired
	private SnapshotLogService snapshotLogService;
	
	// ============================================================================
	// CACHE MANAGEMENT (privado)
	// ============================================================================
	
	/**
	 * Obtiene un snapshot desde el caché o la BD.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el snapshot
	 * @throws MetadataRecordStoreException si el snapshot no existe
	 */
	private NetworkSnapshot getSnapshot(Long snapshotId) throws MetadataRecordStoreException {
		NetworkSnapshot snapshot = snapshotCache.get(snapshotId);
		
		if (snapshot == null) {
			Optional<NetworkSnapshot> optSnapshot = snapshotRepository.findById(snapshotId);
			if (optSnapshot.isPresent()) {
				snapshot = optSnapshot.get();
				snapshotCache.put(snapshotId, snapshot);
			} else {
				throw new MetadataRecordStoreException("Snapshot: " + snapshotId + " not found.");
			}
		}
		
		return snapshot;
	}
	
	/**
	 * Guarda un snapshot en el caché.
	 */
	private void putSnapshot(NetworkSnapshot snapshot) {
		snapshotCache.put(snapshot.getId(), snapshot);
	}
	
	/**
	 * Elimina un snapshot del caché.
	 */
	private void removeFromCache(Long snapshotId) {
		snapshotCache.remove(snapshotId);
	}
	
	// ============================================================================
	// SNAPSHOT LIFECYCLE
	// ============================================================================
	
	@Override
	public Long createSnapshot(Network network) {
		NetworkSnapshot snapshot = new NetworkSnapshot();
		snapshot.setNetwork(network);
		snapshot.setStartTime(LocalDateTime.now());
		snapshotRepository.save(snapshot);
		
		// Guardar en caché
		putSnapshot(snapshot);
		
		logger.info("SNAPSHOT STORE: Created snapshot {} for network {}", 
		           snapshot.getId(), network.getId());
		
		return snapshot.getId();
	}
	
	@Override
	public void saveSnapshot(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshotRepository.save(snapshot);
			logger.debug("SNAPSHOT STORE: Saved snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error saving snapshot {}: {}", snapshotId, e.getMessage());
		}
	}
	
	@Override
	public void deleteSnapshot(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			
			// Eliminar logs
			snapshotLogService.deleteSnapshotLog(snapshotId);
			
			// Eliminar records (delegado al RecordStore en el futuro)
			recordRepository.deleteBySnapshotID(snapshotId);
			
			// Eliminar snapshot
			snapshotRepository.deleteBySnapshotID(snapshotId);
			
			// Eliminar del caché
			removeFromCache(snapshotId);
			
			logger.info("SNAPSHOT STORE: Deleted snapshot {}", snapshotId);
			
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error deleting snapshot {}: {}", snapshotId, e.getMessage());
		}
	}
	
	@Override
	public void cleanSnapshotData(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			
			// Eliminar logs
			snapshotLogService.deleteSnapshotLog(snapshotId);
			
			// Eliminar records (delegado al RecordStore en el futuro)
			recordRepository.deleteBySnapshotID(snapshotId);
			
			// Para snapshots válidos: marcar como deleted
			if (snapshot.getStatus().equals(SnapshotStatus.VALID) || 
			    snapshot.getStatus().equals(SnapshotStatus.HARVESTING_FINISHED_VALID)) {
				snapshot.setDeleted(true);
				snapshotRepository.save(snapshot);
				logger.info("SNAPSHOT STORE: Cleaned and marked as deleted snapshot {}", snapshotId);
			} else {
				// Para snapshots fallidos: eliminar completamente
				snapshotRepository.delete(snapshot);
				removeFromCache(snapshotId);
				logger.info("SNAPSHOT STORE: Cleaned and removed failed snapshot {}", snapshotId);
			}
			
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error cleaning snapshot {}: {}", snapshotId, e.getMessage());
		}
	}
	
	// ============================================================================
	// SNAPSHOT QUERIES
	// ============================================================================
	
	@Override
	public List<Long> listSnapshotsIds(Long networkId, boolean includeDeleted) {
		if (includeDeleted) {
			return snapshotRepository.findAllIdsByNetworkId(networkId);
		} else {
			return snapshotRepository.findNonDeletedIdsByNetworkId(networkId);
		}
	}
	
	@Override
	public Long findLastGoodKnownSnapshot(Network network) {
		if (network == null || network.getId() == null) {
			return null;
		}
		
		NetworkSnapshot snapshot = snapshotRepository.findLastGoodKnowByNetworkID(network.getId());
		return snapshot != null ? snapshot.getId() : null;
	}
	
	@Override
	public Long findLastHarvestingSnapshot(Network network) {
		if (network == null || network.getId() == null) {
			return null;
		}
		
		NetworkSnapshot snapshot = snapshotRepository.findLastHarvestedByNetworkID(network.getId());
		return snapshot != null ? snapshot.getId() : null;
	}
	
	@Override
	public Long getPreviousSnapshotId(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getPreviousSnapshotId();
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting previous snapshot ID for {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	@Override
	public void setPreviousSnapshotId(Long snapshotId, Long previousSnapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setPreviousSnapshotId(previousSnapshotId);
			snapshotRepository.save(snapshot);
			logger.debug("SNAPSHOT STORE: Set previous snapshot {} for snapshot {}", 
			           previousSnapshotId, snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error setting previous snapshot ID for {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	// ============================================================================
	// SNAPSHOT METADATA
	// ============================================================================
	
	@Override
	public SnapshotMetadata getSnapshotMetadata(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			
			SnapshotMetadata metadata = new SnapshotMetadata(snapshotId);
			
			// Información básica
			metadata.setSize(snapshot.getSize() != null ? snapshot.getSize().longValue() : 0L);
			metadata.setValidSize(snapshot.getValidSize() != null ? snapshot.getValidSize().longValue() : 0L);
			metadata.setTransformedSize(snapshot.getTransformedSize() != null ? snapshot.getTransformedSize().longValue() : 0L);
			
			// Timestamp de creación
			if (snapshot.getStartTime() != null) {
				metadata.setCreatedAt(snapshot.getStartTime().toInstant(java.time.ZoneOffset.UTC).toEpochMilli());
			}
			
			// Información de la red
			if (snapshot.getNetwork() != null) {
				metadata.setOrigin(snapshot.getNetwork().getOriginURL());
				metadata.setNetworkAcronym(snapshot.getNetwork().getAcronym());
				metadata.setMetadataPrefix(snapshot.getNetwork().getMetadataPrefix());

				metadata.setNetwork(snapshot.getNetwork());
			}
			
		    // Populate rule definitions from the associated validator
			if (snapshot.getNetwork().getValidator() != null) {
				snapshot.getNetwork().getValidator().getRules().forEach(rule -> {
					SnapshotMetadata.RuleDefinition ruleDef = new SnapshotMetadata.RuleDefinition(
							rule.getId(),
							rule.getName(),
							rule.getDescription(),
							rule.getQuantifier().name(),
							rule.getMandatory()
					);
					metadata.getRuleDefinitions().put(rule.getId(), ruleDef);
				});
			}
			
			return metadata;
			
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting snapshot metadata for {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	@Override
	public Validator getValidator(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getNetwork().getValidator();
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting validator for snapshot {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	// ============================================================================
	// SNAPSHOT STATUS
	// ============================================================================
	
	@Override
	public SnapshotStatus getSnapshotStatus(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getStatus();
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting status for snapshot {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	@Override
	public void updateSnapshotStatus(Long snapshotId, SnapshotStatus status) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(status);
			// No guardamos inmediatamente - se guarda en saveSnapshot()
			logger.debug("SNAPSHOT STORE: Updated status to {} for snapshot {}", status, snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error updating status for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	@Override
	public SnapshotIndexStatus getSnapshotIndexStatus(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getIndexStatus();
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting index status for snapshot {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	@Override
	public void updateSnapshotIndexStatus(Long snapshotId, SnapshotIndexStatus status) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setIndexStatus(status);
			// No guardamos inmediatamente - se guarda en saveSnapshot()
			logger.debug("SNAPSHOT STORE: Updated index status to {} for snapshot {}", status, snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error updating index status for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	// ============================================================================
	// SNAPSHOT TIMESTAMPS
	// ============================================================================
	
	@Override
	public LocalDateTime getSnapshotStartDatestamp(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getStartTime();
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting start datestamp for snapshot {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	@Override
	public void updateSnapshotStartDatestamp(Long snapshotId, LocalDateTime datestamp) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStartTime(datestamp);
			logger.debug("SNAPSHOT STORE: Updated start datestamp for snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error updating start datestamp for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	@Override
	public LocalDateTime getSnapshotEndDatestamp(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getEndTime();
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting end datestamp for snapshot {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	@Override
	public void updateSnapshotEndDatestamp(Long snapshotId, LocalDateTime datestamp) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setEndTime(datestamp);
			logger.debug("SNAPSHOT STORE: Updated end datestamp for snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error updating end datestamp for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	@Override
	public LocalDateTime getSnapshotLastIncrementalDatestamp(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getLastIncrementalTime();
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting last incremental datestamp for snapshot {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	@Override
	public void updateSnapshotLastIncrementalDatestamp(Long snapshotId, LocalDateTime datestamp) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setLastIncrementalTime(datestamp);
			logger.debug("SNAPSHOT STORE: Updated last incremental datestamp for snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error updating last incremental datestamp for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	// ============================================================================
	// SNAPSHOT COUNTERS
	// ============================================================================
	
	@Override
	public Integer getSnapshotSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getSize();
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting size for snapshot {}: {}", 
			           snapshotId, e.getMessage());
			return null;
		}
	}
	
	@Override
	public synchronized void incrementSnapshotSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.incrementSize();
			logger.trace("SNAPSHOT STORE: Incremented size for snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error incrementing size for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	@Override
	public synchronized void incrementValidSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.incrementValidSize();
			logger.trace("SNAPSHOT STORE: Incremented valid size for snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error incrementing valid size for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	@Override
	public synchronized void decrementValidSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.decrementValidSize();
			logger.trace("SNAPSHOT STORE: Decremented valid size for snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error decrementing valid size for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	@Override
	public synchronized void incrementTransformedSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.incrementTransformedSize();
			logger.trace("SNAPSHOT STORE: Incremented transformed size for snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error incrementing transformed size for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	@Override
	public synchronized void decrementTransformedSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.decrementTransformedSize();
			logger.trace("SNAPSHOT STORE: Decremented transformed size for snapshot {}", snapshotId);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error decrementing transformed size for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
	
	@Override
	public void resetSnapshotValidationCounts(Long snapshotId) throws MetadataRecordStoreException {
		NetworkSnapshot snapshot = getSnapshot(snapshotId);
		snapshot.setValidSize(0);
		snapshot.setTransformedSize(0);
		snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
		snapshot.setIndexStatus(SnapshotIndexStatus.UNKNOWN);
		snapshotRepository.save(snapshot);
		logger.info("SNAPSHOT STORE: Reset validation counts for snapshot {}", snapshotId);
	}
	
	@Override
	public void updateSnapshotCounts(Long snapshotId, Integer size, Integer validSize, Integer transformedSize) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setSize(size);
			snapshot.setValidSize(validSize);
			snapshot.setTransformedSize(transformedSize);
			logger.debug("SNAPSHOT STORE: Updated counts for snapshot {}: size={}, valid={}, transformed={}", 
			           snapshotId, size, validSize, transformedSize);
		} catch (MetadataRecordStoreException e) {
			logger.error("SNAPSHOT STORE: Error updating counts for snapshot {}: {}", 
			           snapshotId, e.getMessage());
		}
	}
}
