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

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Cache LRU en memoria para RecordValidation de snapshots.
 * 
 * ESTRATEGIA:
 * - Primer acceso (cache-miss): Carga TODOS los records del snapshot desde Parquet
 * - Accesos siguientes (cache-hit): Retorna lista en memoria
 * - Las funciones de consulta siempre iteran sobre memoria aplicando filtros on-the-fly
 * 
 * CARACTERÍSTICAS:
 * - LRU (Least Recently Used): Evict automático cuando excede tamaño máximo
 * - TTL: Entradas expiran después de N minutos de inactividad
 * - Thread-safe: ReadWriteLock para lecturas concurrentes, escrituras exclusivas
 * - Invalidación explícita: Al escribir nueva validación sobre un snapshot
 * 
 * CONFIGURACIÓN:
 * - parquet.validation.cache-max-snapshots: Número máximo de snapshots en cache (default: 5, min: 5)
 * - parquet.validation.cache-ttl-minutes: Tiempo de vida en minutos (default: 30)
 */
@Component
public class SnapshotRecordsCache {

    private static final Logger logger = LogManager.getLogger(SnapshotRecordsCache.class);

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    @Value("${parquet.validation.cache-max-snapshots:5}")
    private int maxSnapshots;

    @Value("${parquet.validation.cache-ttl-minutes:30}")
    private int ttlMinutes;

    private Configuration hadoopConf;

    /**
     * Entrada de cache con timestamp para TTL
     */
    private static class CacheEntry {
        final List<RecordValidation> records;
        volatile long lastAccessTime;

        CacheEntry(List<RecordValidation> records) {
            this.records = records;
            this.lastAccessTime = System.currentTimeMillis();
        }

        void touch() {
            this.lastAccessTime = System.currentTimeMillis();
        }

        boolean isExpired(long ttlMillis) {
            return (System.currentTimeMillis() - lastAccessTime) > ttlMillis;
        }
    }

    /**
     * Cache LRU usando LinkedHashMap con accessOrder=true
     * Al acceder a una entrada, se mueve al final (más reciente)
     * La entrada más antigua está al principio para eviction
     */
    private LinkedHashMap<Long, CacheEntry> cache;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long ttlMillis;

    @PostConstruct
    public void init() {
        // Asegurar mínimo de 5 snapshots
        if (maxSnapshots < 5) {
            logger.warn("CACHE: cache-max-snapshots={} is below minimum, using 5", maxSnapshots);
            maxSnapshots = 5;
        }

        ttlMillis = ttlMinutes * 60L * 1000L;

        // Crear LinkedHashMap con accessOrder=true para LRU
        // El tercer parámetro true = accessOrder (vs insertionOrder)
        cache = new LinkedHashMap<Long, CacheEntry>(maxSnapshots + 1, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, CacheEntry> eldest) {
                boolean shouldRemove = size() > maxSnapshots;
                if (shouldRemove) {
                    logger.debug("CACHE LRU EVICTION: Removing snapshot {} (cache size exceeded {})", 
                               eldest.getKey(), maxSnapshots);
                }
                return shouldRemove;
            }
        };

        // Configurar Hadoop
        hadoopConf = new Configuration();
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        logger.info("SNAPSHOT RECORDS CACHE: Initialized | maxSnapshots={} | ttlMinutes={}", 
                   maxSnapshots, ttlMinutes);
    }

    @PreDestroy
    public void cleanup() {
        lock.writeLock().lock();
        try {
            int size = cache.size();
            cache.clear();
            logger.info("SNAPSHOT RECORDS CACHE: Cleared {} entries on shutdown", size);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Obtiene los records de un snapshot desde cache o los carga desde Parquet.
     * 
     * FLUJO:
     * 1. Read-lock: Verificar si está en cache y no expirado
     * 2. Si cache-hit válido: Retornar lista (inmutable view)
     * 3. Si cache-miss o expirado: Write-lock, cargar desde Parquet, cachear
     * 
     * @param snapshotId ID del snapshot
     * @param metadata   Metadata del snapshot (requerido para paths)
     * @return Lista inmutable de RecordValidation (nunca null, puede estar vacía)
     * @throws IOException si hay error leyendo Parquet
     */
    public List<RecordValidation> getRecords(Long snapshotId, SnapshotMetadata metadata) throws IOException {
        // Primero intentar read-lock (permitir lecturas concurrentes)
        lock.readLock().lock();
        try {
            CacheEntry entry = cache.get(snapshotId);
            if (entry != null && !entry.isExpired(ttlMillis)) {
                entry.touch();
                logger.debug("CACHE HIT: snapshot={}, records={}", snapshotId, entry.records.size());
                return Collections.unmodifiableList(entry.records);
            }
        } finally {
            lock.readLock().unlock();
        }

        // Cache miss o expirado - necesitamos write-lock para cargar
        lock.writeLock().lock();
        try {
            // Double-check: otro thread pudo haber cargado mientras esperábamos el lock
            CacheEntry entry = cache.get(snapshotId);
            if (entry != null && !entry.isExpired(ttlMillis)) {
                entry.touch();
                logger.debug("CACHE HIT (after lock upgrade): snapshot={}, records={}", 
                           snapshotId, entry.records.size());
                return Collections.unmodifiableList(entry.records);
            }

            // Limpiar entrada expirada si existe
            if (entry != null) {
                cache.remove(snapshotId);
                logger.debug("CACHE EXPIRED: Removed stale entry for snapshot {}", snapshotId);
            }

            // Cargar desde Parquet
            logger.debug("CACHE MISS: Loading snapshot {} from Parquet...", snapshotId);
            long startTime = System.currentTimeMillis();

            List<RecordValidation> records = loadFromParquet(metadata);

            long loadTime = System.currentTimeMillis() - startTime;
            logger.debug("CACHE LOADED: snapshot={}, records={}, time={}ms", 
                       snapshotId, records.size(), loadTime);

            // Guardar en cache
            cache.put(snapshotId, new CacheEntry(records));

            return Collections.unmodifiableList(records);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Carga todos los records de un snapshot desde archivos Parquet.
     */
    private List<RecordValidation> loadFromParquet(SnapshotMetadata metadata) throws IOException {
        List<RecordValidation> records = new ArrayList<>();

        try (ValidationRecordManager manager = ValidationRecordManager.forReading(basePath, metadata, hadoopConf)) {
            for (RecordValidation record : manager) {
                records.add(record);
            }
        }

        return records;
    }

    /**
     * Invalida (elimina) una entrada del cache.
     * 
     * CUÁNDO LLAMAR:
     * - Al escribir nueva validación sobre un snapshot (writeRecordsAndFacts)
     * - Al eliminar archivos de un snapshot (cleanupSnapshotFiles, deleteSnapshot)
     * - Al finalizar un snapshot (finalizeSnapshot)
     * 
     * @param snapshotId ID del snapshot a invalidar
     */
    public void invalidate(Long snapshotId) {
        lock.writeLock().lock();
        try {
            CacheEntry removed = cache.remove(snapshotId);
            if (removed != null) {
                logger.debug("CACHE INVALIDATED: snapshot={}, had {} records", 
                           snapshotId, removed.records.size());
            } else {
                logger.debug("CACHE INVALIDATE: snapshot={} was not in cache", snapshotId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Invalida todas las entradas del cache.
     * Útil para limpieza general o troubleshooting.
     */
    public void invalidateAll() {
        lock.writeLock().lock();
        try {
            int size = cache.size();
            cache.clear();
            logger.info("CACHE INVALIDATED ALL: Cleared {} entries", size);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Verifica si un snapshot está en cache (y no expirado).
     * Útil para diagnóstico y tests.
     */
    public boolean isCached(Long snapshotId) {
        lock.readLock().lock();
        try {
            CacheEntry entry = cache.get(snapshotId);
            return entry != null && !entry.isExpired(ttlMillis);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retorna el número de snapshots actualmente en cache.
     */
    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retorna estadísticas del cache para monitoreo.
     */
    public Map<String, Object> getStats() {
        lock.readLock().lock();
        try {
            Map<String, Object> stats = new LinkedHashMap<>();
            stats.put("size", cache.size());
            stats.put("maxSnapshots", maxSnapshots);
            stats.put("ttlMinutes", ttlMinutes);
            
            long totalRecords = 0;
            for (CacheEntry entry : cache.values()) {
                totalRecords += entry.records.size();
            }
            stats.put("totalRecordsInCache", totalRecords);
            stats.put("snapshotIds", new ArrayList<>(cache.keySet()));
            
            return stats;
        } finally {
            lock.readLock().unlock();
        }
    }
}
