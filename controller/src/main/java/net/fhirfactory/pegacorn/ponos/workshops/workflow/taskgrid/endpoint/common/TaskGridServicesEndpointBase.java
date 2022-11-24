/*
 * Copyright (c) 2022 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.endpoint.common;

import net.fhirfactory.pegacorn.core.interfaces.tasks.PetasosTaskGridClientInterface;
import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointTopologyTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.TaskCompletionSummaryType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.TaskFulfillmentType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.datatypes.TaskExecutionControl;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.datatypes.TaskOutcomeStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayloadSet;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.edge.jgroups.JGroupsIntegrationPointSummary;
import net.fhirfactory.pegacorn.internals.fhir.r4.resources.endpoint.valuesets.EndpointPayloadTypeEnum;
import net.fhirfactory.pegacorn.petasos.endpoints.technologies.jgroups.JGroupsIntegrationPointBase;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices.TaskGridClientServicesManagerAlpha;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices.TaskGridClientServicesManagerBeta;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices.common.TaskGridClientServicesManagerBase;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.routing.InterSubsystemTaskingServices;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import java.time.Instant;

public abstract class TaskGridServicesEndpointBase extends JGroupsIntegrationPointBase {

    private static final Long MAXIMUM_ACTIVITY_DURATION = 3000L;

    @Inject
    private TaskGridClientServicesManagerBeta taskServicesManagerBeta;

    @Inject
    private TaskGridClientServicesManagerAlpha taskServicesManagerAlpha;

    //
    // Instance Methods
    //

    protected TaskGridClientServicesManagerBase getTaskGridClientServicesManager(){
        if(taskServicesManagerAlpha.isBusy()){
            if(taskServicesManagerBeta.isBusy()){
                Long alphaActivityDuration = Instant.now().getEpochSecond() - taskServicesManagerAlpha.getBusyStartTime().getEpochSecond();
                if(alphaActivityDuration > MAXIMUM_ACTIVITY_DURATION){
                    return(taskServicesManagerAlpha);
                } else {
                    return(taskServicesManagerBeta);
                }
            } else {
                return(taskServicesManagerBeta);
            }
        }
        return(taskServicesManagerAlpha);
    }

    //
    // Business Methods
    //

    public PetasosActionableTask registerExternallyTriggeredTask(String participantName, PetasosActionableTask actionableTask , JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".registerExternallyTriggeredTask(): Entry, participantName->{}, actionableTask->{}, integrationPoint->{}", participantName, actionableTask, requesterEndpointSummary);
        PetasosActionableTask task = getTaskGridClientServicesManager().registerExternallyTriggeredTask(participantName, actionableTask);
        getLogger().debug(".registerExternallyTriggeredTask(): Exit, task->{}", task);
        return(task);
    }

    public TaskIdType queueTask(PetasosActionableTask actionableTask, JGroupsIntegrationPointSummary integrationPoint) {
        getLogger().debug(".queueTask(): Entry, actionableTask->{}, integrationPoint->{}", actionableTask, integrationPoint);
        TaskIdType taskId = getTaskGridClientServicesManager().queueTask(actionableTask);
        getLogger().debug(".queueTask(): Exit, taskId->{}", taskId);
        return(taskId);
    }

    public PetasosActionableTask getNextPendingTask(String participantName, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".getNextPendingTask(): Entry, participantName->{}, requesterEndpointSummary->{}", participantName, requesterEndpointSummary);
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getNextPendingTask(): Exit, participantName is empty");
            return(null);
        }
        PetasosActionableTask nextPendingTask = getTaskGridClientServicesManager().getNextPendingTask(participantName);
        getLogger().debug(".getNextPendingTask(): Exit, task->{}", nextPendingTask);
        return(nextPendingTask);
    }

    public TaskExecutionControl notifyTaskStart(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskStart(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}", participantName, taskId, taskFulfillmentDetail);
        TaskExecutionControl executionControl = getTaskGridClientServicesManager().updateTaskStatusToStart(participantName, taskId, taskFulfillmentDetail);
        getLogger().debug(".notifyTaskStart(): Exit");
        return(executionControl);
    }

    public TaskExecutionControl notifyTaskFinish(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String statusReason, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskFinish(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome );
        TaskExecutionControl executionControl = getTaskGridClientServicesManager().updateTaskStatusToFinish(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome, statusReason);
        getLogger().debug(".notifyTaskFinish(): Exit");
        return(executionControl);
    }

    public TaskExecutionControl notifyTaskCancellation(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String statusReason, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskCancellation(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome );
        TaskExecutionControl executionControl = getTaskGridClientServicesManager().updateTaskStatusToCancelled(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome, statusReason);
        getLogger().debug(".notifyTaskCancellation(): Exit");
        return(executionControl);
    }

    public TaskExecutionControl notifyTaskFailure(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String failureDescription, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskFailure(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}, failureDescription->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome , failureDescription);
        TaskExecutionControl executionControl = getTaskGridClientServicesManager().updateTaskStatusToFailed(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome, failureDescription);
        getLogger().debug(".notifyTaskFailure(): Exit");
        return(executionControl);
    }

    public TaskExecutionControl notifyTaskFinalisation(String participantName, TaskIdType taskId, TaskCompletionSummaryType completionSummary, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskFinalisation(): Entry, participantName->{}, taskId->{}, completionSummary->{}", participantName, taskId, completionSummary);
        TaskExecutionControl executionControl = getTaskGridClientServicesManager().updateTaskStatusToFinalised(participantName, taskId, completionSummary);
        getLogger().debug(".notifyTaskFinalisation(): Exit");
        return(executionControl);
    }

    //
    // JGroups Endpoint Housekeeping
    //

    @Override
    protected String specifySubsystemParticipantName() {
        return (getProcessingPlant().getSubsystemParticipantName());
    }

    @Override
    protected String specifyJGroupsClusterName() {
        return (getComponentNameUtilities().getPetasosTaskServicesGroupName());
    }

    @Override
    protected String specifyJGroupsStackFileName() {
        return (getProcessingPlant().getMeAsASoftwareComponent().getPetasosTaskingStackConfigFile());
    }

    @Override
    protected PetasosEndpointTopologyTypeEnum specifyIPCType() {
        return (PetasosEndpointTopologyTypeEnum.JGROUPS_INTEGRATION_POINT);
    }

    @Override
    protected EndpointPayloadTypeEnum specifyPetasosEndpointPayloadType() {
        return (EndpointPayloadTypeEnum.ENDPOINT_PAYLOAD_INTERNAL_TASKS);
    }

    @Override
    protected void addIntegrationPointToJGroupsIntegrationPointSet() {
        getJgroupsIPSet().setPetasosTaskServicesEndpoint(getJGroupsIntegrationPoint());
    }

    @Override
    protected void doIntegrationPointBusinessFunctionCheck(JGroupsIntegrationPointSummary integrationPointSummary, boolean isRemoved, boolean isAdded) {

    }

    @Override
    protected void executePostConstructActivities() {

    }

    //
    // Task Grid Client Interface Stubs
    //


}
