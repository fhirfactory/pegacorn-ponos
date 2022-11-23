/*
 * Copyright (c) 2021 Mark A. Hunter
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices;

import net.fhirfactory.pegacorn.core.interfaces.tasks.PetasosTaskGridClientInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.TaskCompletionSummaryType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.TaskFulfillmentType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.datatypes.TaskExecutionControl;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.datatypes.TaskOutcomeStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayloadSet;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices.common.TaskGridClientServicesManagerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TaskGridClientServicesManagerBeta extends TaskGridClientServicesManagerBase implements PetasosTaskGridClientInterface {
    private static final Logger LOG = LoggerFactory.getLogger(TaskGridClientServicesManagerBeta.class);

    //
    // Constructor(s)
    //

    public TaskGridClientServicesManagerBeta(){
        super();
    }

    //
    // Getters (and Setters)
    //

    @Override
    protected Logger getLogger(){
        return(LOG);
    }

    //
    // Implemented Interface Stubs
    //


    @Override
    public TaskExecutionControl notifyTaskStart(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail) {
        TaskExecutionControl taskExecutionControl = updateTaskStatusToStart(participantName, taskId, taskFulfillmentDetail);
        return (taskExecutionControl);
    }

    @Override
    public TaskExecutionControl notifyTaskFinish(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String taskStatusReason) {
        TaskExecutionControl taskExecutionControl = updateTaskStatusToFinish(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome, taskStatusReason);
        return(taskExecutionControl);
    }

    @Override
    public TaskExecutionControl notifyTaskCancellation(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String taskStatusReason) {
        TaskExecutionControl taskExecutionControl = updateTaskStatusToCancelled(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome, taskStatusReason);
        return(taskExecutionControl);
    }

    @Override
    public TaskExecutionControl notifyTaskFailure(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String taskStatusReason) {
        TaskExecutionControl taskExecutionControl = updateTaskStatusToFailed(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome, taskStatusReason);
        return(taskExecutionControl);
    }

    @Override
    public TaskExecutionControl notifyTaskFinalisation(String participantName, TaskIdType taskId, TaskCompletionSummaryType completionSummary) {
        TaskExecutionControl taskExecutionControl = updateTaskStatusToFinalised(participantName, taskId, completionSummary);
        return(taskExecutionControl);
    }

}
