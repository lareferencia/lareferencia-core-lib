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

package org.lareferencia.core.worker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.parquet.OAIRecord;
import org.lareferencia.backend.repositories.parquet.OAIRecordParquetRepository;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Iterator;

/**
 * Worker base para procesar secuencialmente los OAIRecords almacenados en Parquet.
 *
 * Simplificación: no hay concepto de batch ni transacciones. El worker hace:
 * - preRun() para inicializar (debe setear this.snapshotId)
 * - itera sobre todos los records con el iterator de {@link OAIRecordParquetRepository}
 * - llama a processItem(record) por cada record
 * - postRun() al finalizar
 *
 * Este diseño evita la complejidad transaccional porque Parquet es almacenamiento en archivos.
 */
public abstract class OAIRecordParquetWorker extends BaseWorker<NetworkRunningContext> {

    private static final Logger logger = LogManager.getLogger(OAIRecordParquetWorker.class);

    @Autowired
    protected OAIRecordParquetRepository oaiRecordRepository;


    /**
     * Metadata del snapshot a procesar. Debe ser seteado por la implementación en preRun().
     */
    protected SnapshotMetadata snapshotMetadata;

    private volatile boolean wasStopped = false;

    private long totalRecordsProcessed = 0L;

    public OAIRecordParquetWorker() {
        super();
    }

    public OAIRecordParquetWorker(NetworkRunningContext context) {
        super(context);
    }

    @Override
    public synchronized void run() {
        logger.info("OAI PARQUET WORKER: {} :: START processing: {}", getName(), runningContext);

        // Inicialización por la subclase
        preRun();

        if (snapshotMetadata == null) {
            logger.error("OAI PARQUET WORKER: {} :: snapshotMetadata not set in preRun()", getName());
            return;
        }

        try {
            Iterable<OAIRecord> iterable = oaiRecordRepository.iterateRecords(snapshotMetadata);
            Iterator<OAIRecord> it = iterable.iterator();

            while (it.hasNext() && !wasStopped) {
                OAIRecord record = it.next();
                try {
                    processItem(record);
                    totalRecordsProcessed++;
                } catch (Exception e) {
                    // Registrar y continuar o detener según la implementación
                    logger.error("OAI PARQUET WORKER: {} :: Error processing record {}: {}", getName(), record.getIdentifier(), e.getMessage(), e);
                    // Por defecto, detener la ejecución
                    this.stop();
                    break;
                }
            }

            if (!wasStopped) {
                postRun();
                logger.info("OAI PARQUET WORKER: {} :: COMPLETED ({} records)", getName(), totalRecordsProcessed);
            } else {
                logger.info("OAI PARQUET WORKER: {} :: STOPPED ({} records)", getName(), totalRecordsProcessed);
            }

        } catch (IOException e) {
            logger.error("OAI PARQUET WORKER: {} :: Failed reading Parquet records for snapshot {}: {}", 
                        getName(), snapshotMetadata.getSnapshotId(), e.getMessage(), e);
        }
    }

    /**
     * Inicialización antes de comenzar. Debe setear this.snapshotMetadata.
     */
    protected abstract void preRun();

    /**
     * Procesa un record individual leído de Parquet.
     */
    protected abstract void processItem(OAIRecord record) throws Exception;

    /**
     * Finalización después de leer todos los records.
     */
    protected abstract void postRun();

    @Override
    public void stop() {
        this.wasStopped = true;
        super.stop();
    }

    public double getCompletionRate() {
        // No tenemos el total estimado por defecto
        return 0d;
    }
}
