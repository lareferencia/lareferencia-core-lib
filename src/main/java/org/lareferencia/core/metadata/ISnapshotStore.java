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

import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.domain.SnapshotStatus;
import org.lareferencia.core.domain.Validator;

/**
 * Interface for snapshot storage operations.
 * 
 * SEPARATION OF CONCERNS:
 * - ISnapshotStore: Gestiona metadata de snapshots (SQL)
 * - IRecordStore: Gestionará records y su metadata (Parquet/SQL híbrido)
 * 
 * RESPONSABILIDADES:
 * - Crear/eliminar/actualizar snapshots
 * - Gestionar estados y timestamps
 * - Queries sobre snapshots (último válido, último harvesting, etc.)
 * - Metadata de snapshots (tamaños, contadores, etc.)
 * 
 * IMPLEMENTACIONES:
 * - SnapshotStoreSQLImpl: Implementación SQL actual (migrada desde MetadataRecordStoreServiceImpl)
 * - Futura: SnapshotStoreParquetImpl si se necesita
 */
public interface ISnapshotStore {
	
	// ============================================================================
	// SNAPSHOT LIFECYCLE
	// ============================================================================
	
	/**
	 * Crea un nuevo snapshot para la red especificada.
	 * 
	 * @param network la red para la cual crear el snapshot
	 * @return el ID del snapshot creado
	 */
	Long createSnapshot(Network network);
	
	/**
	 * Persiste los cambios del snapshot en el store.
	 * 
	 * @param snapshotId el ID del snapshot a guardar
	 */
	void saveSnapshot(Long snapshotId);
	
	/**
	 * Elimina completamente un snapshot y sus datos asociados.
	 * Incluye: logs, records (delegado a RecordStore), y metadata del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot a eliminar
	 */
	void deleteSnapshot(Long snapshotId);
	
	/**
	 * Limpia los datos del snapshot pero mantiene su registro.
	 * Para snapshots válidos: los marca como deleted
	 * Para snapshots fallidos: los elimina completamente
	 * 
	 * @param snapshotId el ID del snapshot a limpiar
	 */
	void cleanSnapshotData(Long snapshotId);
	
	// ============================================================================
	// SNAPSHOT QUERIES
	// ============================================================================
	
	/**
	 * Lista todos los IDs de snapshots para una red.
	 * 
	 * @param networkId el ID de la red
	 * @param includeDeleted si se deben incluir snapshots eliminados
	 * @return lista de IDs de snapshots
	 */
	List<Long> listSnapshotsIds(Long networkId, boolean includeDeleted);
	
	/**
	 * Encuentra el ID del último snapshot válido de la red.
	 * 
	 * @param network la red
	 * @return el ID del snapshot, o null si no existe
	 */
	Long findLastGoodKnownSnapshot(Network network);
	
	/**
	 * Encuentra el ID del último snapshot en proceso de harvesting.
	 * 
	 * @param network la red
	 * @return el ID del snapshot, o null si no existe
	 */
	Long findLastHarvestingSnapshot(Network network);
	
	/**
	 * Obtiene el ID del snapshot anterior (para tracking incremental).
	 * 
	 * @param snapshotId el ID del snapshot actual
	 * @return el ID del snapshot anterior, o null si no existe
	 */
	Long getPreviousSnapshotId(Long snapshotId);
	
	/**
	 * Establece la referencia al snapshot anterior.
	 * 
	 * @param snapshotId el ID del snapshot actual
	 * @param previousSnapshotId el ID del snapshot anterior
	 */
	void setPreviousSnapshotId(Long snapshotId, Long previousSnapshotId);
	
	// ============================================================================
	// SNAPSHOT METADATA
	// ============================================================================
	
	/**
	 * Obtiene los metadatos completos del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return los metadatos del snapshot
	 */
	SnapshotMetadata getSnapshotMetadata(Long snapshotId);
	
	/**
	 * Obtiene el validador asociado al snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el validador
	 */
	Validator getValidator(Long snapshotId);
	
	// ============================================================================
	// SNAPSHOT STATUS
	// ============================================================================
	
	/**
	 * Obtiene el estado actual del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el estado del snapshot
	 */
	SnapshotStatus getSnapshotStatus(Long snapshotId);
	
	/**
	 * Actualiza el estado del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @param status el nuevo estado
	 */
	void updateSnapshotStatus(Long snapshotId, SnapshotStatus status);
	
	/**
	 * Obtiene el estado de indexación del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el estado de indexación
	 */
	SnapshotIndexStatus getSnapshotIndexStatus(Long snapshotId);
	
	/**
	 * Actualiza el estado de indexación del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @param status el nuevo estado de indexación
	 */
	void updateSnapshotIndexStatus(Long snapshotId, SnapshotIndexStatus status);
	
	// ============================================================================
	// SNAPSHOT TIMESTAMPS
	// ============================================================================
	
	/**
	 * Obtiene el timestamp de inicio del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el timestamp de inicio
	 */
	LocalDateTime getSnapshotStartDatestamp(Long snapshotId);
	
	/**
	 * Actualiza el timestamp de inicio del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @param datestamp el timestamp de inicio
	 */
	void updateSnapshotStartDatestamp(Long snapshotId, LocalDateTime datestamp);
	
	/**
	 * Obtiene el timestamp de fin del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el timestamp de fin
	 */
	LocalDateTime getSnapshotEndDatestamp(Long snapshotId);
	
	/**
	 * Actualiza el timestamp de fin del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @param datestamp el timestamp de fin
	 */
	void updateSnapshotEndDatestamp(Long snapshotId, LocalDateTime datestamp);
	
	/**
	 * Obtiene el timestamp del último harvesting incremental.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el timestamp del último incremental
	 */
	LocalDateTime getSnapshotLastIncrementalDatestamp(Long snapshotId);
	
	/**
	 * Actualiza el timestamp del último harvesting incremental.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @param datestamp el timestamp del último incremental
	 */
	void updateSnapshotLastIncrementalDatestamp(Long snapshotId, LocalDateTime datestamp);
	
	// ============================================================================
	// SNAPSHOT COUNTERS
	// ============================================================================
	
	/**
	 * Obtiene el tamaño total del snapshot (número de records).
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el número de records
	 */
	Integer getSnapshotSize(Long snapshotId);
	
	/**
	 * Incrementa el contador de tamaño del snapshot.
	 * Llamado cuando se crea un nuevo record.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void incrementSnapshotSize(Long snapshotId);
	
	/**
	 * Incrementa el contador de records válidos.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void incrementValidSize(Long snapshotId);
	
	/**
	 * Decrementa el contador de records válidos.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void decrementValidSize(Long snapshotId);
	
	/**
	 * Incrementa el contador de records transformados.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void incrementTransformedSize(Long snapshotId);
	
	/**
	 * Decrementa el contador de records transformados.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void decrementTransformedSize(Long snapshotId);
	
	/**
	 * Reinicia los contadores de validación del snapshot.
	 * Pone validSize y transformedSize a 0.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @throws MetadataRecordStoreException si falla la operación
	 */
	void resetSnapshotValidationCounts(Long snapshotId) throws MetadataRecordStoreException;
	
	/**
	 * Actualiza los contadores de tamaño del snapshot basándose en conteo real.
	 * Usado después de operaciones bulk como copyNotDeletedRecordsFromSnapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @param size el tamaño total
	 * @param validSize el número de records válidos
	 * @param transformedSize el número de records transformados
	 */
	void updateSnapshotCounts(Long snapshotId, Integer size, Integer validSize, Integer transformedSize);
}
