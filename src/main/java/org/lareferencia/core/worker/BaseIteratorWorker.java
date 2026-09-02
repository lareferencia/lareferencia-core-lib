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
import lombok.Getter;
import lombok.Setter;

import java.text.NumberFormat;
import java.util.Iterator;

/**
 * Worker base para procesar secuencialmente registros obtenidos por un
 * {@link Iterator} proporcionado por la implementación concreta.
 *
 * El worker hace:
 * - preRun() para inicializar (debe setear this.snapshotId)
 * - itera sobre los records proporcionados
 * - llama a processItem(record) por cada record
 * - postRun() al finalizar
 *
 */
public abstract class BaseIteratorWorker<I, C extends IRunningContext> extends BaseWorker<C>
        implements IIteratorWorker<I, C> {

    private static final Logger logger = LogManager.getLogger(BaseIteratorWorker.class);

    private volatile boolean wasStopped = false;

    private Integer totalRecords = 0;
    private Integer currentRecordIndex = 0;

    @Getter
    @Setter
    private Integer pageSize = 1000;

    private Iterator<I> recordIterator;

    NumberFormat percentageFormat = NumberFormat.getPercentInstance();

    public BaseIteratorWorker() {
        super();
    }

    public BaseIteratorWorker(C context) {
        super(context);
    }

    @Override
    public void run() {

        preRun();

        if (recordIterator == null) {
            throw new IllegalStateException("Iterator Worker: " + getName() +
                    " :: recordIterator not set before run()");
        }

        prePage();

        while (recordIterator.hasNext()) {
            I record = recordIterator.next();

            processItem(record);

            currentRecordIndex += 1;

            if (currentRecordIndex % pageSize == 0) {
                logger.debug("Iterator Worker: {} " + percentageFormat.format(this.getCompletionRate()));

                postPage();

                // Check for stop signal and break if set
                if (wasStopped)
                    break;

                prePage();
            }
        }

        postRun();
    }

    /**
     * Inicialización antes de comenzar. Debe setear this.snapshotMetadata.
     */
    protected abstract void preRun();

    /**
     * Procesamiento antes de leer un lote de records.
     */
    public abstract void prePage();

    /**
     * Procesamiento después de leer un lote de records.
     */
    public abstract void postPage();

    /**
     * Procesa un registro individual del iterador.
     */
    public abstract void processItem(I record);

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
        if (totalRecords == 0)
            return 0.0;
        else
            return (currentRecordIndex.doubleValue() / totalRecords.doubleValue());
    }

    @Override
    public void setIterator(Iterator<I> iterator, Integer totalRecords) {
        this.recordIterator = iterator;
        this.totalRecords = totalRecords;
    }

    @Override
    public Long getCurrentRecordUniqueID(Long snapshotId) {
        if (currentRecordIndex == null) {
            return null;
        }
        // Calcula ID único combinando snapshotId + sequence
        // snapshotId en bits 27-62 (37 bits = 137 billones de snapshots)
        // currentRecordIndex en bits 0-26 (27 bits = 134 millones máximo)

        return (snapshotId << 27) | (currentRecordIndex & 0x7FFFFFFFL);
    }
}
