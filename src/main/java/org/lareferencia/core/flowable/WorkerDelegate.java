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

package org.lareferencia.core.flowable;

import org.flowable.engine.delegate.BpmnError;
import org.lareferencia.core.worker.IWorker;
import org.springframework.context.ApplicationContext;

/**
 * Generic worker delegate that loads workers dynamically by bean name.
 * <p>
 * This class is NOT a Spring bean - it's instantiated by WorkerDelegateFactory
 * to provide dynamic worker loading without creating dedicated delegate classes
 * for each worker type.
 * </p>
 * 
 * <p>
 * Usage in BPMN (via factory):
 * </p>
 * 
 * <pre>
 * &lt;serviceTask id="harvestingTask"
 *              flowable:delegateExpression="${workerDelegateFactory.create('harvestingWorker')}"/&gt;
 * </pre>
 * 
 * @author LA Referencia Team
 */
public class WorkerDelegate extends BaseWorkerDelegate<IWorker<?>> {

    private final String workerBeanName;
    private final ApplicationContext applicationContext;

    /**
     * Creates a new WorkerDelegate instance.
     * 
     * @param workerBeanName     the Spring bean name of the worker to execute
     * @param applicationContext the Spring application context for bean lookup
     */
    public WorkerDelegate(String workerBeanName, ApplicationContext applicationContext) {
        this.workerBeanName = workerBeanName;
        this.applicationContext = applicationContext;
    }

    @Override
    protected IWorker<?> getWorker() {
        try {
            return applicationContext.getBean(workerBeanName, IWorker.class);
        } catch (Exception e) {
            String message = String.format(
                    "Failed to load worker bean '%s': %s. " +
                            "Verify the bean exists and implements IWorker.",
                    workerBeanName, e.getMessage());
            throw new BpmnError("WORKER_BEAN_NOT_FOUND", message);
        }
    }

    @Override
    public String toString() {
        return "WorkerDelegate[worker=" + workerBeanName + "]";
    }
}
