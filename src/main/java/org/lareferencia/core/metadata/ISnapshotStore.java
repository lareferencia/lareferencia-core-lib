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
	// SNAPSHOT STATUS - USE BATCH METHODS INSTEAD (startHarvesting, finishValidation, etc)
	// ============================================================================
	
	/**
	 * Obtiene el estado actual del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el estado del snapshot
	 */
	SnapshotStatus getSnapshotStatus(Long snapshotId);
	
	/**
	 * Obtiene el estado de indexación del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el estado de indexación
	 */
	SnapshotIndexStatus getSnapshotIndexStatus(Long snapshotId);
	
	// ============================================================================
	// SNAPSHOT TIMESTAMPS - USE BATCH METHODS FOR STATE CHANGES
	// ============================================================================
	
	/**
	 * Obtiene el timestamp de inicio del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el timestamp de inicio
	 */
	LocalDateTime getSnapshotStartDatestamp(Long snapshotId);
	
	/**
	 * Obtiene el timestamp de fin del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el timestamp de fin
	 */
	LocalDateTime getSnapshotEndDatestamp(Long snapshotId);
	
	/**
	 * Obtiene el timestamp del último harvesting incremental.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el timestamp del último incremental
	 */
	LocalDateTime getSnapshotLastIncrementalDatestamp(Long snapshotId);
	
	/**
	 * Actualiza el timestamp del último harvesting incremental.
	 * Usado durante harvesting incremental para trackear el progreso.
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
	 * Obtiene el tamaño válido actual del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el número de records válidos
	 */
	Integer getSnapshotValidSize(Long snapshotId);
	
	/**
	 * Obtiene el tamaño transformado actual del snapshot.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return el número de records transformados
	 */
	Integer getSnapshotTransformedSize(Long snapshotId);
	
	/**
	 * Incrementa el contador de tamaño del snapshot.
	 * Llamado cuando se crea un nuevo record.
	 * 
	 * @deprecated Usar {@link #updateCounters(Long, CountersUpdate)} en su lugar.
	 * Ejemplo: updateCounters(id, CountersUpdate.incrementHarvested())
	 * 
	 * @param snapshotId el ID del snapshot
	 */
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
	void resetSnapshotValidationCounts(Long snapshotId);
	
	/**
	 * Fuerza la persistencia de cambios pendientes a la BD.
	 * 
	 * CUÁNDO USAR:
	 * - Al final de cada fase (harvesting, validation, indexing)
	 * - Antes de leer datos que acabas de escribir desde otro componente
	 * - Para garantizar durabilidad en puntos críticos
	 * 
	 * NOTA: Con @Transactional, esto fuerza el flush del EntityManager.
	 * Los cambios se persisten pero la transacción sigue activa.
	 * 
	 * @param snapshotId el ID del snapshot (puede ser null para flush general)
	 */
	void flush(Long snapshotId);
	
	// ============================================================================
	// BATCH UPDATE METHODS - Simplified API (Fase 2)
	// ============================================================================
	
	/**
	 * Inicia la fase de harvesting.
	 * Actualiza: status = HARVESTING, startTime = now()
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void startHarvesting(Long snapshotId);
	
	/**
	 * Actualiza el estado de harvesting sin finalizar.
	 * Actualiza: status = HARVESTING, endTime = now()
	 * Útil para marcar checkpoints durante el harvesting.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void updateHarvesting(Long snapshotId);
	
	/**
	 * Finaliza la fase de harvesting exitosamente.
	 * Actualiza: status = HARVESTED, endTime = now()
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void finishHarvesting(Long snapshotId);
	
	/**
	 * Inicia la fase de validación.
	 * Actualiza: status = VALIDATING
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void startValidation(Long snapshotId);
	
	/**
	 * Finaliza la fase de validación exitosamente.
	 * Actualiza: status = VALID, endTime = now()
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void finishValidation(Long snapshotId);
	
	/**
	 * Marca el snapshot como indexado.
	 * Actualiza: indexStatus = INDEXED
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void markAsIndexed(Long snapshotId);
	
	/**
	 * Marca el snapshot como fallido.
	 * Actualiza: status = FAILED, endTime = now()
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void markAsFailed(Long snapshotId);
	
	/**
	 * Marca el snapshot en estado de reintento.
	 * Actualiza: status = RETRYING
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void markAsRetrying(Long snapshotId);
	
	/**
	 * Marca el snapshot como eliminado.
	 * Actualiza: status = DELETED, endTime = now()
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	void markAsDeleted(Long snapshotId);
}
