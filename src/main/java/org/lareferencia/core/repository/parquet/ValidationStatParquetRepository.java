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

package org.lareferencia.core.repository.parquet;

import org.lareferencia.core.metadata.RecordStatus;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * REPOSITORIO DE VALIDACIÓN PARQUET - FACHADA
 * 
 * Esta clase actúa como fachada que mantiene la API pública existente
 * y delega a componentes especializados:
 * 
 * - ValidationSnapshotLifecycleManager: Escritura y ciclo de vida de snapshots
 * - ValidationStatsQueryService: Consultas y cálculo de estadísticas
 * 
 * ARQUITECTURA DE 3 CAPAS:
 * ========================
 * 
 * LAYER 1 - METADATA (JSON):
 * - metadata.json: Datos de snapshot guardados UNA SOLA VEZ
 * - Consultas ultra-rápidas (<1ms) para estadísticas agregadas
 * 
 * LAYER 2 - RECORDS (PARQUET):
 * - 1 fila por RECORD (no por occurrence)
 * - Arrays: validRuleIds[], invalidRuleIds[]
 * - Paginación CORRECTA: 20 filas = 20 records
 * - Reducción ~88% de espacio vs fact table
 * 
 * LAYER 3 - RULE FACTS (PARQUET):
 * - 1 fila por (record_id, rule_id) con array de occurrences
 * - Análisis detallado por regla específica
 * 
 * COMPATIBILIDAD:
 * - API externa 100% compatible con versión anterior
 * - Delegación transparente a componentes internos
 */
@Repository
public class ValidationStatParquetRepository {

    @Autowired
    private ValidationSnapshotLifecycleManager lifecycleManager;

    @Autowired
    private ValidationStatsQueryService queryService;

    // ========================================
    // LIFECYCLE OPERATIONS (delegados a LifecycleManager)
    // ========================================

    /**
     * Inicializa un snapshot para escritura.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @throws IOException si hay error
     */
    public void initializeSnapshot(SnapshotMetadata snapshotMetadata) throws IOException {
        lifecycleManager.initializeSnapshot(snapshotMetadata);
    }

    /**
     * Guarda un record y sus facts (escritura incremental).
     * 
     * @param snapshotMetadata metadata del snapshot
     * @param record           datos del record
     * @throws IOException si hay error
     */
    public void saveRecordAndFacts(SnapshotMetadata snapshotMetadata, RecordValidation record) throws IOException {
        lifecycleManager.saveRecordAndFacts(snapshotMetadata, record);
    }

    /**
     * Finaliza un snapshot, cierra writers y persiste metadata final.
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void finalizeSnapshot(Long snapshotId) throws IOException {
        lifecycleManager.finalizeSnapshot(snapshotId);
    }

    /**
     * Fuerza el flush de los buffers de escritura.
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void flush(Long snapshotId) throws IOException {
        lifecycleManager.flush(snapshotId);
    }

    /**
     * Elimina datos de validación de un snapshot.
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void deleteSnapshot(Long snapshotId) throws IOException {
        lifecycleManager.deleteSnapshot(snapshotId);
    }

    /**
     * Elimina solo archivos Parquet de validación.
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void deleteParquetForSnapshot(Long snapshotId) throws IOException {
        lifecycleManager.deleteParquetForSnapshot(snapshotId);
    }

    // ========================================
    // QUERY OPERATIONS (delegados a QueryService)
    // ========================================

    /**
     * Construye estadísticas iterando sobre records con filtros.
     * 
     * @param metadata metadata del snapshot
     * @param fq       filtros opcionales
     * @return estadísticas calculadas
     * @throws IOException si hay error
     */
    public SnapshotValidationStats buildStats(SnapshotMetadata metadata, List<String> fq) throws IOException {
        return queryService.buildStats(metadata, fq);
    }

    /**
     * Obtiene estadísticas de validación de un snapshot.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @return estadísticas o null
     * @throws IOException si hay error
     */
    public SnapshotValidationStats getSnapshotValidationStats(SnapshotMetadata snapshotMetadata) throws IOException {
        return queryService.getSnapshotValidationStats(snapshotMetadata);
    }

    /**
     * Calcula las ocurrencias para una regla específica.
     * 
     * @param snapshotId ID del snapshot
     * @param ruleId     ID de la regla
     * @param fq         filtros opcionales
     * @return mapa con "valid" e "invalid"
     * @throws IOException si hay error
     */
    public Map<String, Map<String, Integer>> calculateRuleOccurrences(Long snapshotId, Integer ruleId, List<String> fq)
            throws IOException {
        return queryService.calculateRuleOccurrences(snapshotId, ruleId, fq);
    }

    /**
     * Consulta observaciones con paginación y filtrado.
     * 
     * @param snapshotId ID del snapshot
     * @param fq         filtros opcionales
     * @param offset     registros a saltar
     * @param limit      límite total
     * @return mapa con "records" y "totalFiltered"
     * @throws IOException si hay error
     */
    public Map<String, Object> queryObservationsWithPagination(Long snapshotId, List<String> fq, int offset, int limit)
            throws IOException {
        return queryService.queryObservationsWithPagination(snapshotId, fq, offset, limit);
    }

    /**
     * Obtiene lista de records por estado.
     * 
     * @param snapshotId ID del snapshot
     * @param status     estado a filtrar
     * @return lista de RecordValidation
     * @throws IOException si hay error
     */
    public List<RecordValidation> getRecordValidationListBySnapshotAndStatus(Long snapshotId, RecordStatus status)
            throws IOException {
        return queryService.getRecordValidationListBySnapshotAndStatus(snapshotId, status);
    }

    /**
     * Obtiene un record por snapshot e identifier.
     * 
     * @param snapshotId ID del snapshot
     * @param identifier identificador del record
     * @return RecordValidation o null
     */
    public RecordValidation getRecordValidationBySnapshotAndIdentifier(Long snapshotId, String identifier) {
        return queryService.getRecordValidationBySnapshotAndIdentifier(snapshotId, identifier);
    }

    /**
     * Retorna iterator completo para streaming.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @return iterator lazy
     * @throws IOException si hay error
     */
    public Iterator<RecordValidation> getIterator(SnapshotMetadata snapshotMetadata) throws IOException {
        return queryService.getIterator(snapshotMetadata);
    }

    /**
     * Retorna iterator ligero para streaming sin ruleFacts.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @param status           estado a filtrar
     * @return iterator lazy
     * @throws IOException si hay error
     */
    public Iterator<RecordValidation> getLightweightIterator(SnapshotMetadata snapshotMetadata, RecordStatus status)
            throws IOException {
        return queryService.getLightweightIterator(snapshotMetadata, status);
    }
}
