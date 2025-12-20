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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flowable.engine.RuntimeService;
import org.flowable.engine.delegate.BpmnError;
import org.flowable.engine.delegate.DelegateExecution;
import org.flowable.engine.delegate.JavaDelegate;
import org.lareferencia.core.worker.IRunningContext;
import org.lareferencia.core.worker.IWorker;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Abstract base class for Flowable delegates that execute workers.
 * <p>
 * This class provides common functionality for wrapping workers as Flowable
 * service tasks, including context setup, progress reporting, and error
 * handling.
 * Subclasses only need to implement {@link #getWorker()} to provide the
 * specific
 * worker instance.
 * </p>
 * 
 * <p>
 * Usage in BPMN:
 * </p>
 * 
 * <pre>
 * &lt;serviceTask id="harvestingTask" 
 *              flowable:delegateExpression="${harvestingDelegate}"/&gt;
 * </pre>
 * 
 * @param <W> the worker type this delegate executes
 * @author LA Referencia Team
 */
public abstract class BaseWorkerDelegate<W extends IWorker<?>> implements JavaDelegate {

    private static final Logger logger = LogManager.getLogger(BaseWorkerDelegate.class);

    @Autowired
    protected RuntimeService runtimeService;

    @Autowired
    protected IWorkerContextFactory contextFactory;

    @Autowired
    protected WorkflowService workflowService;

    /**
     * Returns the worker instance to execute.
     * Subclasses should typically get this from ApplicationContext.
     * 
     * @return the worker instance
     */
    protected abstract W getWorker();

    @Override
    @SuppressWarnings("unchecked")
    public void execute(DelegateExecution execution) {
        logger.info("FLOWABLE DELEGATE: Starting execution for process instance: {}",
                execution.getProcessInstanceId());

        W worker = getWorker();

        if (worker == null) {
            throw new BpmnError("WORKER_ERROR", "Worker instance is null");
        }

        // Create running context from process variables
        IRunningContext context = contextFactory.createContext(execution.getVariables());

        // Configure worker
        ((IWorker<IRunningContext>) worker).setRunningContext(context);

        Boolean incremental = (Boolean) execution.getVariable("incremental");
        if (incremental != null) {
            worker.setIncremental(incremental);
        }

        logger.info("FLOWABLE DELEGATE: Executing worker {} with context {}",
                worker.getName(), context.getId());

        // Subscribe to termination and status events
        String processInstanceId = execution.getProcessInstanceId();
        workflowService.subscribeToTermination(processInstanceId, () -> worker.stop());
        workflowService.subscribeToStatus(processInstanceId, () -> worker.getStatus());

        try {
            // Execute the worker
            worker.run();

            // Set completion variables
            execution.setVariable("workerSuccess", true);

            logger.info("FLOWABLE DELEGATE: Worker {} completed successfully", worker.getName());

        } catch (Exception e) {
            logger.error("FLOWABLE DELEGATE: Worker {} failed: {}", worker.getName(), e.getMessage(), e);

            execution.setVariable("workerSuccess", false);
            execution.setVariable("workerError", e.getMessage());

            throw new BpmnError("WORKER_ERROR",
                    "Worker " + worker.getName() + " failed: " + e.getMessage());
        } finally {
            // Always unsubscribe from events
            workflowService.unsubscribeFromTermination(processInstanceId);
            workflowService.unsubscribeFromStatus(processInstanceId);
        }
    }
}
