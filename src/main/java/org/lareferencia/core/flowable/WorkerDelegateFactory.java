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

import org.flowable.engine.RuntimeService;
import org.flowable.engine.delegate.JavaDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Factory for creating generic worker delegates dynamically.
 *
 * Usage in BPMN:
 * <serviceTask id="task"
 *          flowable:delegateExpression="${workerDelegateFactory.create('workerBeanName')}"/>
 *
 * Each call to {@link #create(String)} produces a new delegate instance,
 * ensuring thread-safety across concurrent process executions.
 */
@Component("workerDelegateFactory")
public class WorkerDelegateFactory {

    @Autowired
    private ApplicationContext applicationContext;

    // Autowire required beans directly
    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private IWorkerContextFactory contextFactory;

    @Autowired
    private WorkflowService workflowService;

    /**
     * Creates a JavaDelegate instance that will execute the specified worker.
     *
     * @param workerBeanName the Spring bean name of the worker to execute
     * @return a JavaDelegate that wraps the worker
     */
    public JavaDelegate create(String workerBeanName) {
        // Create a new instance for each process - thread-safe
        WorkerDelegate delegate = new WorkerDelegate(workerBeanName, applicationContext);

        // Manually inject dependencies expected by BaseWorkerDelegate
        delegate.runtimeService = this.runtimeService;
        delegate.contextFactory = this.contextFactory;
        delegate.workflowService = this.workflowService;

        return delegate;
    }
}
