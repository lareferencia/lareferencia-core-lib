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

package org.lareferencia.core.flowable.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flowable.common.engine.api.delegate.event.FlowableEngineEventType;
import org.flowable.common.engine.api.delegate.event.FlowableEvent;
import org.flowable.common.engine.api.delegate.event.FlowableEventListener;
import org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl;
import org.flowable.engine.impl.persistence.entity.ExecutionEntity;
import org.lareferencia.core.flowable.WorkflowService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Listens for Flowable process lifecycle events and notifies WorkflowService
 * when processes complete. This replaces polling-based cleanup.
 * 
 * @author LA Referencia Team
 */
@Component
@ConditionalOnProperty(name = "workflow.engine", havingValue = "flowable")
public class ProcessCompletionListener implements FlowableEventListener {

    private static final Logger logger = LogManager.getLogger(ProcessCompletionListener.class);

    @Autowired
    @Lazy // Avoid circular dependency
    private WorkflowService workflowService;

    @Override
    public void onEvent(FlowableEvent event) {
        FlowableEngineEventType eventType = (FlowableEngineEventType) event.getType();

        switch (eventType) {
            case PROCESS_COMPLETED:
                handleProcessCompleted(event);
                break;
            case PROCESS_CANCELLED:
                handleProcessCancelled(event);
                break;
            default:
                // Ignore other events
                break;
        }
    }

    private void handleProcessCompleted(FlowableEvent event) {
        handleProcessTermination(event, "completed");
    }

    private void handleProcessCancelled(FlowableEvent event) {
        handleProcessTermination(event, "cancelled");
    }

    private void handleProcessTermination(FlowableEvent event, String terminationType) {
        try {
            if (event instanceof FlowableEntityEventImpl) {
                FlowableEntityEventImpl entityEvent = (FlowableEntityEventImpl) event;
                Object entity = entityEvent.getEntity();

                if (entity instanceof ExecutionEntity) {
                    ExecutionEntity execution = (ExecutionEntity) entity;
                    String processInstanceId = execution.getProcessInstanceId();

                    logger.info("Process {} event received for: {}", terminationType, processInstanceId);
                    workflowService.onProcessCompleted(processInstanceId);
                }
            }
        } catch (Exception e) {
            logger.error("Error handling process {} event: {}", terminationType, e.getMessage(), e);
        }
    }

    @Override
    public boolean isFailOnException() {
        // Don't fail the process if listener throws exception
        return false;
    }

    @Override
    public boolean isFireOnTransactionLifecycleEvent() {
        return false;
    }

    @Override
    public String getOnTransaction() {
        return null;
    }
}
