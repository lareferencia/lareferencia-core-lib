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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.persistence.EntityManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.domain.SnapshotStatus;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.repository.jpa.NetworkSnapshotRepository;
import org.lareferencia.core.repository.jpa.OAIRecordRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementación SQL del store de snapshots.
 * 
 * RESPONSABILIDADES:
 * - Gestión de lifecycle de snapshots (crear, guardar, eliminar)
 * - Gestión de estados y timestamps
 * - Queries sobre snapshots
 * 
 * ARQUITECTURA REFACTORIZADA (v5.0):
 * - NetworkSnapshot (SQL) es la ÚNICA fuente de verdad
 * - SnapshotMetadata es un DTO construido on-demand desde NetworkSnapshot
 * - Sin caché manual: confiamos en JPA level-1 cache + @Transactional
 * - Dirty checking automático de JPA para persistencia
 * 
 * THREAD SAFETY:
 * - @Transactional maneja concurrencia
 * - Métodos synchronized solo donde sea estrictamente necesario
 */
@Transactional
public class SnapshotStoreSQLImpl implements ISnapshotStore {

	private static final Logger logger = LogManager.getLogger(SnapshotStoreSQLImpl.class);
	private static final int AUTOFLUSH_THRESHOLD = 100; // Flush cada 100 updates

	@Autowired
	private NetworkSnapshotRepository snapshotRepository;

	@Autowired
	private EntityManager entityManager;

	// Contador de updates para autoflush por snapshot
	private final Map<Long, Integer> updateCounters = new ConcurrentHashMap<>();

	// Cache concurrente de snapshots activos (en proceso de harvesting/validación)
	private final ConcurrentHashMap<Long, NetworkSnapshot> snapshotCache = new ConcurrentHashMap<>();

	// ============================================================================
	// PRIVATE HELPERS
	// ============================================================================

	/**
	 * Obtiene un snapshot dado su ID.
	 * Usa cache-first: primero busca en cache, si no está, carga de BD y cachea.
	 * 
	 * @param snapshotId ID del snapshot a obtener
	 * @return NetworkSnapshot con los datos del snapshot
	 * @throws SnapshotStoreException si el snapshot no existe
	 */
	private NetworkSnapshot getSnapshot(Long snapshotId) throws SnapshotStoreException {
		// Primero intentar desde cache
		NetworkSnapshot cached = snapshotCache.get(snapshotId);
		if (cached != null) {
			return cached;
		}

		// Si no está en cache, cargar de BD
		Optional<NetworkSnapshot> optSnapshot = snapshotRepository.findById(snapshotId);
		if (optSnapshot.isPresent()) {
			return optSnapshot.get();
		} else {
			throw new SnapshotStoreException("Snapshot: " + snapshotId + " not found.");
		}
	}

	/**
	 * Añade un snapshot al cache.
	 * 
	 * @param snapshot el snapshot a cachear
	 */
	private void cacheSnapshot(NetworkSnapshot snapshot) {
		snapshotCache.put(snapshot.getId(), snapshot);
		logger.debug("SNAPSHOT STORE: Cached snapshot {}", snapshot.getId());
	}

	/**
	 * Remueve un snapshot del cache.
	 * 
	 * @param snapshotId el ID del snapshot a remover
	 */
	private void uncacheSnapshot(Long snapshotId) {
		snapshotCache.remove(snapshotId);
		logger.debug("SNAPSHOT STORE: Uncached snapshot {}", snapshotId);
	}

	/**
	 * Incrementa el contador de updates para autoflush.
	 * Hace flush si se alcanza el threshold.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	private void trackUpdateAndAutoFlush(Long snapshotId) {
		int count = updateCounters.merge(snapshotId, 1, Integer::sum);
		if (count >= AUTOFLUSH_THRESHOLD) {
			entityManager.flush();
			updateCounters.put(snapshotId, 0); // Reset contador
			logger.trace("SNAPSHOT STORE: Autoflush triggered for snapshot {} after {} updates",
					snapshotId, count);
		}
	}

	/**
	 * Limpia el contador de updates después de finalizar operación.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	private void clearUpdateCounter(Long snapshotId) {
		updateCounters.remove(snapshotId);
	}

	// ============================================================================
	// SNAPSHOT LIFECYCLE
	// ============================================================================

	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public Long createSnapshot(Network network) {
		NetworkSnapshot snapshot = new NetworkSnapshot();
		snapshot.setNetwork(network);
		snapshot.setStartTime(LocalDateTime.now());
		snapshotRepository.saveAndFlush(snapshot);

		// Añadir al cache para uso posterior
		cacheSnapshot(snapshot);

		logger.debug("SNAPSHOT STORE: Created snapshot {} for network {}",
				snapshot.getId(), network.getId());

		return snapshot.getId();
	}

	@Override
	public void deleteSnapshot(Long snapshotId) {

		// Eliminar snapshot
		snapshotRepository.deleteBySnapshotID(snapshotId);

		logger.debug("SNAPSHOT STORE: Deleted snapshot {}", snapshotId);
	}

	@Override
	public void cleanSnapshotData(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);

			// Para snapshots válidos: marcar como deleted
			if (snapshot.getStatus().equals(SnapshotStatus.VALID) ||
					snapshot.getStatus().equals(SnapshotStatus.HARVESTING_FINISHED_VALID)) {
				snapshot.setDeleted(true);
				snapshotRepository.save(snapshot);
				logger.info("SNAPSHOT STORE: Cleaned and marked as deleted snapshot {}", snapshotId);
			} else {
				// Para snapshots fallidos: eliminar completamente
				snapshotRepository.delete(snapshot);
				logger.info("SNAPSHOT STORE: Cleaned and removed failed snapshot {}", snapshotId);
			}

		} catch (SnapshotStoreException e) {
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
	public List<ISnapshotStore.SnapshotSummary> listSnapshotsSummary(Long networkId, boolean includeDeleted) {
		List<Long> ids = listSnapshotsIds(networkId, includeDeleted);
		Long lgkId = findLastGoodKnownSnapshot(networkId);

		List<ISnapshotStore.SnapshotSummary> summaries = new java.util.ArrayList<>();
		for (Long id : ids) {
			try {
				NetworkSnapshot snapshot = getSnapshot(id);
				summaries.add(new ISnapshotStore.SnapshotSummary(
						snapshot.getId(),
						snapshot.getStatus() != null ? snapshot.getStatus().name() : "UNKNOWN",
						snapshot.getIndexStatus() != null ? snapshot.getIndexStatus().name() : "UNKNOWN",
						snapshot.getStartTime(),
						snapshot.getEndTime(),
						snapshot.getSize(),
						snapshot.getValidSize(),
						snapshot.getTransformedSize(),
						snapshot.isDeleted(),
						id.equals(lgkId)));
			} catch (SnapshotStoreException e) {
				logger.warn("Could not load snapshot {}: {}", id, e.getMessage());
			}
		}
		return summaries;
	}

	@Override
	public Long findLastGoodKnownSnapshot(Network network) {
		if (network == null) {
			return null;
		}
		return findLastGoodKnownSnapshot(network.getId());
	}

	@Override
	public Long findLastHarvestingSnapshot(Network network) {
		if (network == null) {
			return null;
		}
		return findLastHarvestingSnapshot(network.getId());
	}

	@Override
	public Long findLastGoodKnownSnapshot(Long networkId) {
		if (networkId == null) {
			return null;
		}
		NetworkSnapshot snapshot = snapshotRepository.findLastGoodKnowByNetworkID(networkId);
		return snapshot != null ? snapshot.getId() : null;
	}

	@Override
	public Long findLastHarvestingSnapshot(Long networkId) {
		if (networkId == null) {
			return null;
		}
		NetworkSnapshot snapshot = snapshotRepository.findLastHarvestedByNetworkID(networkId);
		return snapshot != null ? snapshot.getId() : null;
	}

	@Override
	public Long getPreviousSnapshotId(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getPreviousSnapshotId();
		} catch (SnapshotStoreException e) {
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
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error setting previous snapshot ID for {}: {}",
					snapshotId, e.getMessage());
		}
	}

	// ============================================================================
	// SNAPSHOT METADATA
	// ============================================================================

	/**
	 * Genera SnapshotMetadata desde NetworkSnapshot.
	 * 
	 * IMPORTANTE: NetworkSnapshot es la fuente de verdad.
	 * Este método construye el DTO SnapshotMetadata on-demand.
	 * 
	 * NOTA: Lee directamente desde BD para garantizar datos frescos.
	 * JPA level-1 cache puede hacer esto eficiente dentro de la transacción.
	 */
	@Override
	public SnapshotMetadata getSnapshotMetadata(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return new SnapshotMetadata(snapshot);
		} catch (SnapshotStoreException e) {
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
		} catch (SnapshotStoreException e) {
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
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting status for snapshot {}: {}",
					snapshotId, e.getMessage());
			return null;
		}
	}

	@Override
	public SnapshotIndexStatus getSnapshotIndexStatus(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getIndexStatus();
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting index status for snapshot {}: {}",
					snapshotId, e.getMessage());
			return null;
		}
	}

	// ============================================================================
	// SNAPSHOT TIMESTAMPS (GETTERS ONLY - Use batch methods for updates)
	// ============================================================================

	@Override
	public LocalDateTime getSnapshotStartDatestamp(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getStartTime();
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting start datestamp for snapshot {}: {}",
					snapshotId, e.getMessage());
			return null;
		}
	}

	@Override
	public LocalDateTime getSnapshotEndDatestamp(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getEndTime();
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting end datestamp for snapshot {}: {}",
					snapshotId, e.getMessage());
			return null;
		}
	}

	@Override
	public LocalDateTime getSnapshotLastIncrementalDatestamp(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getLastIncrementalTime();
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting last incremental datestamp for snapshot {}: {}",
					snapshotId, e.getMessage());
			return null;
		}
	}

	@Override
	@Transactional
	public void updateSnapshotLastIncrementalDatestamp(Long snapshotId, LocalDateTime datestamp) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setLastIncrementalTime(datestamp);
			logger.debug("SNAPSHOT STORE: Updated last incremental datestamp for snapshot {} to {}",
					snapshotId, datestamp);
		} catch (SnapshotStoreException e) {
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
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting size for snapshot {}: {}",
					snapshotId, e.getMessage());
			return null;
		}
	}

	@Override
	public Integer getSnapshotValidSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getValidSize();
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting valid size for snapshot {}: {}",
					snapshotId, e.getMessage());
			return null;
		}
	}

	@Override
	public Integer getSnapshotTransformedSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			return snapshot.getTransformedSize();
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error getting transformed size for snapshot {}: {}",
					snapshotId, e.getMessage());
			return null;
		}
	}

	@Override
	public void incrementSnapshotSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.incrementSize();
			trackUpdateAndAutoFlush(snapshotId);
			logger.trace("SNAPSHOT STORE: Incremented size for snapshot {}", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error incrementing size for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	public void incrementSnapshotSizeBy(Long snapshotId, int count) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setSize(snapshot.getSize() + count);
			// Solo modifica en cache, la persistencia se hace en updateHarvesting
			logger.debug("SNAPSHOT STORE: Incremented size by {} for snapshot {} (total: {})",
					count, snapshotId, snapshot.getSize());
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error incrementing size by {} for snapshot {}: {}",
					count, snapshotId, e.getMessage());
		}
	}

	@Override
	public void incrementValidSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.incrementValidSize();
			trackUpdateAndAutoFlush(snapshotId);
			logger.trace("SNAPSHOT STORE: Incremented valid size for snapshot {}", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error incrementing valid size for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	public void decrementValidSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.decrementValidSize();
			trackUpdateAndAutoFlush(snapshotId);
			logger.trace("SNAPSHOT STORE: Decremented valid size for snapshot {}", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error decrementing valid size for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	public void incrementTransformedSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.incrementTransformedSize();
			trackUpdateAndAutoFlush(snapshotId);
			logger.trace("SNAPSHOT STORE: Incremented transformed size for snapshot {}", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error incrementing transformed size for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	public void decrementTransformedSize(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.decrementTransformedSize();
			trackUpdateAndAutoFlush(snapshotId);
			logger.trace("SNAPSHOT STORE: Decremented transformed size for snapshot {}", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error decrementing transformed size for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	@Transactional
	public void resetSnapshotValidationCounts(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setValidSize(0);
			snapshot.setTransformedSize(0);
			snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
			snapshot.setIndexStatus(SnapshotIndexStatus.UNKNOWN);
			logger.info("SNAPSHOT STORE: Reset validation counts for snapshot {}", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error resetting validation counts for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	// @Override
	// public void updateSnapshotCounts(Long snapshotId, Integer size, Integer
	// validSize, Integer transformedSize) {
	// try {
	// NetworkSnapshot snapshot = getSnapshot(snapshotId);
	// snapshot.setSize(size);
	// snapshot.setValidSize(validSize);
	// snapshot.setTransformedSize(transformedSize);
	// logger.debug("SNAPSHOT STORE: Updated counts for snapshot {}: size={},
	// valid={}, transformed={}",
	// snapshotId, size, validSize, transformedSize);
	// } catch (SnapshotStoreException e) {
	// logger.error("SNAPSHOT STORE: Error updating counts for snapshot {}: {}",
	// snapshotId, e.getMessage());
	// }
	// }

	// ============================================================================
	// BATCH UPDATE METHODS - Simplified API (Fase 2)
	// ============================================================================

	@Override
	public void startHarvesting(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(SnapshotStatus.HARVESTING);
			snapshot.setStartTime(LocalDateTime.now());
			snapshotRepository.saveAndFlush(snapshot);
			logger.info("SNAPSHOT STORE: Started harvesting for snapshot {}", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error starting harvesting for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	public void updateHarvesting(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(SnapshotStatus.HARVESTING);
			snapshot.setEndTime(LocalDateTime.now());
			snapshotRepository.saveAndFlush(snapshot);
			logger.debug("SNAPSHOT STORE: Updated harvesting status for snapshot {}", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error updating harvesting for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	public void finishHarvesting(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
			snapshot.setEndTime(LocalDateTime.now());
			snapshotRepository.saveAndFlush(snapshot);
			logger.info("SNAPSHOT STORE: Finished harvesting for snapshot {}", snapshotId);
			clearUpdateCounter(snapshotId);
			uncacheSnapshot(snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error finishing harvesting for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	@Transactional
	public void startValidation(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(SnapshotStatus.VALID); // No hay estado VALIDATING, usa VALID directamente
			logger.info("SNAPSHOT STORE: Started validation for snapshot {}", snapshotId);
			// JPA dirty checking persiste automáticamente al final de la transacción
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error starting validation for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	@Transactional
	public void finishValidation(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(SnapshotStatus.VALID);
			snapshot.setEndTime(LocalDateTime.now());
			logger.info("SNAPSHOT STORE: Finished validation for snapshot {}", snapshotId);
			// JPA dirty checking persiste automáticamente al final de la transacción
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error finishing validation for snapshot {}: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	@Transactional
	public void markAsIndexed(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setIndexStatus(SnapshotIndexStatus.INDEXED);
			logger.info("SNAPSHOT STORE: Marked snapshot {} as indexed", snapshotId);
			// JPA dirty checking persiste automáticamente al final de la transacción
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error marking snapshot {} as indexed: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	public void markAsFailed(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(SnapshotStatus.HARVESTING_FINISHED_ERROR);
			snapshot.setEndTime(LocalDateTime.now());
			snapshotRepository.saveAndFlush(snapshot);
			logger.info("SNAPSHOT STORE: Marked snapshot {} as failed", snapshotId);
			clearUpdateCounter(snapshotId);
			uncacheSnapshot(snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error marking snapshot {} as failed: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	@Transactional
	public void markAsDeleted(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setDeleted(true);
			logger.info("SNAPSHOT STORE: Marked snapshot {} as deleted", snapshotId);
			// JPA dirty checking persiste automáticamente al final de la transacción
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error marking snapshot {} as deleted: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	@Transactional
	public void markAsRetrying(Long snapshotId) {
		try {
			NetworkSnapshot snapshot = getSnapshot(snapshotId);
			snapshot.setStatus(SnapshotStatus.RETRYING);
			logger.info("SNAPSHOT STORE: Marked snapshot {} as retrying", snapshotId);
		} catch (SnapshotStoreException e) {
			logger.error("SNAPSHOT STORE: Error marking snapshot {} as retrying: {}",
					snapshotId, e.getMessage());
		}
	}

	@Override
	public void flush(Long snapshotId) {
		// Forzar flush del EntityManager
		// Con @Transactional, JPA dirty checking ya persistió los cambios
		// Este método es principalmente para documentar puntos de flush explícitos
		snapshotRepository.flush();

		if (snapshotId != null) {
			logger.debug("SNAPSHOT STORE: Flushed snapshot {}", snapshotId);
		} else {
			logger.debug("SNAPSHOT STORE: Flushed all pending changes");
		}
	}
}
