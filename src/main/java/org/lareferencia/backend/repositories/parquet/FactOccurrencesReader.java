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

package org.lareferencia.backend.repositories.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.lareferencia.backend.domain.parquet.FactOccurrence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * FACT TABLE READER: Lectura optimizada de tabla de hechos con predicate pushdown.
 * 
 * CARACTERÍSTICAS:
 * - Predicate pushdown para filtros a nivel columnar
 * - Row group pruning para skip de bloques irrelevantes
 * - Streaming de registros para evitar carga masiva en memoria
 * - Soporte para filtros complejos (AND, OR, IN, RANGE)
 * - Proyección de columnas para lectura selectiva
 * 
 * OPTIMIZACIONES:
 * - Lee solo row groups que pasan los filtros
 * - Usa dictionary encoding para filtros en strings
 * - Bloom filters cuando disponibles
 * - Estadísticas min/max para rangos numéricos
 */
public final class FactOccurrencesReader implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(FactOccurrencesReader.class);

    private final ParquetReader<Group> reader;
    private final String filePath;
    private long recordsRead = 0;

    /**
     * Constructor privado - usar builder estático
     */
    private FactOccurrencesReader(ParquetReader<Group> reader, String filePath) {
        this.reader = reader;
        this.filePath = filePath;
    }

    /**
     * BUILDER: Crea reader sin filtros (full scan)
     * 
     * @param inputPath ruta al archivo Parquet
     * @param hadoopConf configuración Hadoop
     * @return reader listo para usar
     * @throws IOException si falla la apertura
     */
    public static FactOccurrencesReader newReader(String inputPath, Configuration hadoopConf) 
            throws IOException {
        return newReader(inputPath, hadoopConf, null);
    }

    /**
     * BUILDER: Crea reader con filtros optimizados (predicate pushdown)
     * 
     * VENTAJAS:
     * - Parquet skip row groups que no cumplen filtros
     * - Reduce I/O dramáticamente en queries selectivos
     * - Usa estadísticas min/max de columnas
     * 
     * @param inputPath ruta al archivo Parquet
     * @param hadoopConf configuración Hadoop
     * @param filter predicado de filtrado (null = sin filtro)
     * @return reader listo para usar
     * @throws IOException si falla la apertura
     */
    public static FactOccurrencesReader newReader(String inputPath, 
                                                  Configuration hadoopConf,
                                                  FilterPredicate filter) throws IOException {
        Path path = new Path(inputPath);
        
        logger.debug("FACT READER: Opening file {}", inputPath);
        if (filter != null) {
            logger.debug("FACT READER: With filter predicate: {}", filter);
        }
        
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader.Builder<Group> builder = ParquetReader.builder(readSupport, path)
            .withConf(hadoopConf);
        
        // Aplicar filtro si existe
        if (filter != null) {
            builder = builder.withFilter(FilterCompat.get(filter));
        }
        
        ParquetReader<Group> parquetReader = builder.build();
        
        return new FactOccurrencesReader(parquetReader, inputPath);
    }

    /**
     * STREAMING READ: Procesa registros uno por uno sin cargar en memoria
     * 
     * Patrón recomendado para archivos grandes o queries que no necesitan
     * materializar resultados completos.
     * 
     * @param processor función que procesa cada fila
     * @return número de filas procesadas
     * @throws IOException si falla la lectura
     */
    public long stream(Consumer<FactOccurrence> processor) throws IOException {
        long count = 0;
        Group group;
        
        while ((group = reader.read()) != null) {
            FactOccurrence fact = fromGroup(group);
            processor.accept(fact);
            count++;
            recordsRead++;
        }
        
        logger.debug("FACT READER: Streamed {} rows from {}", count, filePath);
        return count;
    }

    /**
     * BATCH READ: Lee todas las filas en memoria
     * 
     * WARNING: Solo usar para resultsets pequeños o con filtros selectivos
     * Para archivos grandes, preferir stream() o readWithLimit()
     * 
     * @return lista de todas las filas
     * @throws IOException si falla la lectura
     */
    public List<FactOccurrence> readAll() throws IOException {
        List<FactOccurrence> results = new ArrayList<>();
        stream(results::add);
        return results;
    }

    /**
     * PAGINATED READ: Lee con offset y limit para paginación
     * 
     * IMPORTANTE: En Parquet no hay skip eficiente, offset requiere leer y descartar.
     * Para grandes offsets, considerar particionamiento o índices secundarios.
     * 
     * @param offset número de filas a skip
     * @param limit máximo de filas a retornar
     * @return lista de filas paginadas
     * @throws IOException si falla la lectura
     */
    public List<FactOccurrence> readWithLimit(int offset, int limit) throws IOException {
        List<FactOccurrence> results = new ArrayList<>();
        long skipped = 0;
        long collected = 0;
        
        Group group;
        while ((group = reader.read()) != null) {
            recordsRead++;
            
            if (skipped < offset) {
                skipped++;
                continue;
            }
            
            if (collected >= limit) {
                break;
            }
            
            FactOccurrence fact = fromGroup(group);
            results.add(fact);
            collected++;
        }
        
        logger.debug("FACT READER: Read {} rows (skipped {}, limit {}) from {}", 
                   collected, skipped, limit, filePath);
        
        return results;
    }

    /**
     * COUNT ONLY: Cuenta filas sin deserializar objetos completos
     * 
     * Más eficiente que readAll().size() porque no construye objetos.
     * 
     * @return número de filas que pasan los filtros
     * @throws IOException si falla la lectura
     */
    public long count() throws IOException {
        long count = 0;
        
        while (reader.read() != null) {
            count++;
            recordsRead++;
        }
        
        logger.debug("FACT READER: Counted {} rows in {}", count, filePath);
        return count;
    }

    /**
     * AGGREGATION HELPER: Procesa filas para agregación sin materializar
     * 
     * Patrón visitor para cálculo de estadísticas sin overhead de objetos.
     * 
     * @param aggregator función que acumula estadísticas desde Groups
     * @return número de filas agregadas
     * @throws IOException si falla la lectura
     */
    public long aggregateFromGroups(Consumer<Group> aggregator) throws IOException {
        long count = 0;
        Group group;
        
        while ((group = reader.read()) != null) {
            aggregator.accept(group);
            count++;
            recordsRead++;
        }
        
        logger.debug("FACT READER: Aggregated {} rows from {}", count, filePath);
        return count;
    }

    /**
     * CONVERSION: Parquet Group → FactOccurrence
     * 
     * Maneja nullability correcta para campos opcionales.
     * 
     * @param group fila Parquet
     * @return objeto Java
     */
    private FactOccurrence fromGroup(Group group) {
        return FactOccurrence.builder()
            .id(group.getString("id", 0))
            .identifier(group.getString("identifier", 0))
            .snapshotId(group.getLong("snapshot_id", 0))
            .origin(group.getString("origin", 0))
            .network(getOptionalString(group, "network"))
            .repository(getOptionalString(group, "repository"))
            .institution(getOptionalString(group, "institution"))
            .ruleId(group.getInteger("rule_id", 0))
            .value(getOptionalString(group, "value"))
            .isValid(group.getBoolean("is_valid", 0))
            .recordIsValid(group.getBoolean("record_is_valid", 0))
            .isTransformed(group.getBoolean("is_transformed", 0))
            .metadataPrefix(getOptionalString(group, "metadata_prefix"))
            .setSpec(getOptionalString(group, "set_spec"))
            .build();
    }

    /**
     * HELPER: Extrae string opcional (null-safe)
     */
    private String getOptionalString(Group group, String fieldName) {
        int fieldIndex = group.getType().getFieldIndex(fieldName);
        int repetitionCount = group.getFieldRepetitionCount(fieldIndex);
        
        if (repetitionCount == 0) {
            return null; // Campo no presente
        }
        
        return group.getString(fieldIndex, 0);
    }

    /**
     * Retorna número de registros leídos hasta el momento
     */
    public long getRecordsRead() {
        return recordsRead;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            logger.debug("FACT READER: Closing reader. Total rows read: {}", recordsRead);
            reader.close();
        }
    }

    // ==================== FILTER BUILDERS ====================

    /**
     * FILTER BUILDER: Crea filtro para snapshot_id
     */
    public static FilterPredicate snapshotIdEquals(long snapshotId) {
        return FilterApi.eq(FilterApi.longColumn("snapshot_id"), snapshotId);
    }

    /**
     * FILTER BUILDER: Crea filtro para is_valid
     */
    public static FilterPredicate isValidEquals(boolean isValid) {
        return FilterApi.eq(FilterApi.booleanColumn("is_valid"), isValid);
    }

    /**
     * FILTER BUILDER: Crea filtro para record_is_valid (estado de validación del record completo)
     */
    public static FilterPredicate recordIsValidEquals(boolean recordIsValid) {
        return FilterApi.eq(FilterApi.booleanColumn("record_is_valid"), recordIsValid);
    }

    /**
     * FILTER BUILDER: Crea filtro para is_transformed
     */
    public static FilterPredicate isTransformedEquals(boolean isTransformed) {
        return FilterApi.eq(FilterApi.booleanColumn("is_transformed"), isTransformed);
    }

    /**
     * FILTER BUILDER: Crea filtro para rule_id
     */
    public static FilterPredicate ruleIdEquals(int ruleId) {
        return FilterApi.eq(FilterApi.intColumn("rule_id"), ruleId);
    }

    /**
     * FILTER BUILDER: Crea filtro IN para múltiples rule_ids
     */
    public static FilterPredicate ruleIdIn(List<Integer> ruleIds) {
        if (ruleIds == null || ruleIds.isEmpty()) {
            return null;
        }
        
        FilterPredicate filter = ruleIdEquals(ruleIds.get(0));
        for (int i = 1; i < ruleIds.size(); i++) {
            filter = FilterApi.or(filter, ruleIdEquals(ruleIds.get(i)));
        }
        return filter;
    }

    /**
     * FILTER BUILDER: Crea filtro para network (string)
     */
    public static FilterPredicate networkEquals(String network) {
        return FilterApi.eq(
            FilterApi.binaryColumn("network"), 
            Binary.fromString(network)
        );
    }

    /**
     * FILTER BUILDER: Crea filtro para identifier (string)
     */
    public static FilterPredicate identifierEquals(String identifier) {
        return FilterApi.eq(
            FilterApi.binaryColumn("identifier"),
            Binary.fromString(identifier)
        );
    }

    /**
     * FILTER BUILDER: Combina múltiples filtros con AND
     */
    public static FilterPredicate and(FilterPredicate... predicates) {
        if (predicates == null || predicates.length == 0) {
            return null;
        }
        
        FilterPredicate result = predicates[0];
        for (int i = 1; i < predicates.length; i++) {
            if (predicates[i] != null) {
                result = FilterApi.and(result, predicates[i]);
            }
        }
        return result;
    }

    /**
     * FILTER BUILDER: Combina múltiples filtros con OR
     */
    public static FilterPredicate or(FilterPredicate... predicates) {
        if (predicates == null || predicates.length == 0) {
            return null;
        }
        
        FilterPredicate result = predicates[0];
        for (int i = 1; i < predicates.length; i++) {
            if (predicates[i] != null) {
                result = FilterApi.or(result, predicates[i]);
            }
        }
        return result;
    }
}
