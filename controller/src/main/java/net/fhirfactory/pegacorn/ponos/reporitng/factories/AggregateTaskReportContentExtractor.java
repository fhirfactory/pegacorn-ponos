/*
 * Copyright (c) 2022 Mark A. Hunter
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
package net.fhirfactory.pegacorn.ponos.reporitng.factories;

import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointTopologyTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.ActionableTaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayload;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWProcessingOutcomeEnum;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.base.IPCTopologyEndpoint;
import net.fhirfactory.pegacorn.core.model.topology.nodes.WorkUnitProcessorSoftwareComponent;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class AggregateTaskReportContentExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateTaskReportContentExtractor.class);


    public String getIngresName(PetasosActionableTask actionableTask){
        getLogger().debug(".getIngresName(): Entry, actionableTask->{}", actionableTask);

        String ingresEndpointParticipantName = null;
        String ingresName = null;

        try {
            if (actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor() instanceof WorkUnitProcessorSoftwareComponent) {
                WorkUnitProcessorSoftwareComponent ingresWUP = (WorkUnitProcessorSoftwareComponent) actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor();
                ingresEndpointParticipantName = getEndpointParticipantName(ingresWUP, true);
            }

            if (StringUtils.isEmpty(ingresEndpointParticipantName)) {
                ingresName = actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getParticipantName();
            } else {
                ingresName = ingresEndpointParticipantName;
            }
        } catch(Exception ex){
            getLogger().warn(".getIngresName(): Error Extracting Ingres Name, message->{}, stackTrace->{}", ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex));
            ingresName = "Unavailable";
        }

        getLogger().debug(".getIngresName(): Exit, ingresName->{}", ingresName);
        return(ingresName);
    }

    public String getEgressName(PetasosActionableTask actionableTask){
        getLogger().debug(".getEgressName(): Entry, actionableTask->{}", actionableTask);

        String egressEndpointParticipantName = null;
        String egressName = null;

        try {
            if (actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor() instanceof WorkUnitProcessorSoftwareComponent) {
                WorkUnitProcessorSoftwareComponent egressWUP = (WorkUnitProcessorSoftwareComponent) actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor();
                egressEndpointParticipantName = getEndpointParticipantName(egressWUP, false);
            }

            if (StringUtils.isEmpty(egressEndpointParticipantName)) {
                egressName = actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getParticipantName();
            } else {
                egressName = egressEndpointParticipantName;
            }
        } catch(Exception ex){
            getLogger().warn(".getEgressName(): Error Extracting Egress Name, message->{}, stackTrace->{}", ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex));
            egressName = "Unavailable";
        }

        getLogger().debug(".getIngresName(): Exit, egressName->{}", egressName);
        return(egressName);
    }

    public String getIngresContent(PetasosActionableTask actionableTask){
        getLogger().debug(".getContent(): Entry, actionableTask->{}", actionableTask);

        String taskContent = null;
        try {
            if (actionableTask.hasTaskWorkItem()) {
                if (actionableTask.getTaskWorkItem().hasIngresContent()) {
                    taskContent = actionableTask.getTaskWorkItem().getIngresContent().getPayload();
                }
            }
            if (taskContent == null) {
                taskContent = "Unavailable";
            }
        } catch (Exception ex){
            getLogger().warn(".getOutcomeContent(): Error Extracting Task Content, message->{}, stackTrace->{}", ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex));
            taskContent = "Unavailable(due to an Exception)";
        }

        getLogger().debug(".getContent(): Exit, taskContent->{}", taskContent);
        return(taskContent);
    }

    public String getOutcomeContent(PetasosActionableTask actionableTask){
        getLogger().debug(".getOutcomeContent(): Entry, actionableTask->{}", actionableTask);

        String outcomeContent = null;
        try {
            if (actionableTask.hasTaskWorkItem()) {
                if (actionableTask.getTaskWorkItem().hasEgressContent()) {
                    UoWPayload lastTaskEgresOutcome = null;
                    for (UoWPayload lastUOWPayload : actionableTask.getTaskWorkItem().getEgressContent().getPayloadElements()) {
                        lastTaskEgresOutcome = lastUOWPayload;
                        break;
                    }
                    if(lastTaskEgresOutcome != null){
                        outcomeContent = lastTaskEgresOutcome.getPayload();
                    }
                }
            }
            if (outcomeContent == null) {
                outcomeContent = "Unavailable";
            }
        } catch(Exception ex){
            getLogger().warn(".getOutcomeContent(): Error Extracting Outcome Content, message->{}, stackTrace->{}", ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex));
            outcomeContent = "Unavailable(due to an Exception)";
        }

        getLogger().debug(".getOutcomeContent(): Exit, outcomeContent->{}", outcomeContent);
        return(outcomeContent);
    }

    public boolean getOutcomeStatus(PetasosActionableTask actionableTask){
        getLogger().debug(".getOutcomeStatus(): Entry, actionableTask->{}", actionableTask);

        boolean badOutcome = false;

        try {
            ActionableTaskOutcomeStatusEnum outcomeStatus = actionableTask.getTaskOutcomeStatus().getOutcomeStatus();

            if (outcomeStatus.equals(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_FINALISED) || outcomeStatus.equals(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_FINISHED)) {
                badOutcome = false;
            } else {
                badOutcome = true;
            }
        } catch(Exception ex){
            getLogger().warn(".getOutcomeContent(): Error Extracting Outcome Status, message->{}, stackTrace->{}", ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex));
            badOutcome = true;
        }

        getLogger().debug(".getOutcomeStatusDetail(): Exit, badOutcome->{}", badOutcome);
        return(badOutcome);
    }

    public String getOutcomeStatusDetail(PetasosActionableTask actionableTask){
        getLogger().debug(".getOutcomeStatusDetail(): Entry, actionableTask->{}", actionableTask);

        String taskOutcomeStatus = null;

        try{
            ActionableTaskOutcomeStatusEnum outcomeStatus = null;
            if(actionableTask.hasTaskOutcomeStatus()) {
                if(actionableTask.getTaskOutcomeStatus().getOutcomeStatus() != null){
                    outcomeStatus = actionableTask.getTaskOutcomeStatus().getOutcomeStatus();
                }
            }

            if (outcomeStatus == null) {
                outcomeStatus = ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_UNKNOWN;
            }

            if(actionableTask.hasTaskWorkItem()) {
                if(actionableTask.getTaskWorkItem().hasProcessingOutcome()) {
                    UoWProcessingOutcomeEnum processingOutcome = actionableTask.getTaskWorkItem().getProcessingOutcome();
                    switch (processingOutcome) {
                        case UOW_OUTCOME_FILTERED:
                        case UOW_OUTCOME_DISCARD: {
                            taskOutcomeStatus = outcomeStatus.getDisplayName() + " (" + processingOutcome.getToken() + ")";
                            break;
                        }
                        default:
                            break;
                    }
                }
            }

            if(StringUtils.isEmpty(taskOutcomeStatus)) {
                taskOutcomeStatus = outcomeStatus.getDisplayName();
            }
        } catch(Exception ex){
            getLogger().warn(".getOutcomeContent(): Error Extracting Outcome Status, message->{}, stackTrace->{}", ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex));
            taskOutcomeStatus = "Unavailable(due to an Exception)";
        }

        getLogger().debug(".getOutcomeStatusDetail(): Exit, taskOutcomeStatus->{}", taskOutcomeStatus);
        return(taskOutcomeStatus);
    }

    public ComponentIdType getEndpointComponentId(PetasosActionableTask actionableTask){
        getLogger().debug(".getEndpointComponentId(): Entry, actionableTask->{}", actionableTask);

        ComponentIdType componentId = null;

        if(actionableTask != null){
            if(actionableTask.hasTaskFulfillment()){
                if(actionableTask.getTaskFulfillment().hasFulfillerWorkUnitProcessor()){
                    componentId = actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getComponentID();
                }
            }
        }

        getLogger().debug(".getEndpointComponentId(): Exit, componentId->{}", componentId);
        return(componentId);
    }

    public String getEndpointParticipantName(PetasosActionableTask actionableTask, boolean isIngres){
        getLogger().debug(".getEndpointParticipantName(): Entry, actionableTask->{}, isIngres->{}", actionableTask, isIngres);

        String participantName = null;
        if(actionableTask != null){
            if(actionableTask.hasTaskFulfillment()){
                if(actionableTask.getTaskFulfillment().hasFulfillerWorkUnitProcessor()){
                    WorkUnitProcessorSoftwareComponent wupSoftwareComponent = (WorkUnitProcessorSoftwareComponent)actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor();
                    participantName = getEndpointParticipantName(wupSoftwareComponent, isIngres);
                }
            }
        }

        getLogger().debug(".getEndpointParticipantName(): Exit, participantName->{}", participantName);
        return(participantName);
    }

    public String getEndpointParticipantName(WorkUnitProcessorSoftwareComponent endpointWUP, boolean isIngres){
        getLogger().debug(".getEndpointParticipantName(): Entry, endpointWUP->{}", endpointWUP);

        String participantName = null;
        if(isIngres){
            IPCTopologyEndpoint ingresEndpoint = endpointWUP.getIngresEndpoint();
            if(ingresEndpoint != null){
                if(ingresEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.MLLP_SERVER)){
                    participantName = ingresEndpoint.getParticipantName();
                } else if(ingresEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.HTTP_API_SERVER)){
                    participantName = ingresEndpoint.getParticipantName();
                } else if(ingresEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.FILE_SHARE_SOURCE)){
                    participantName = ingresEndpoint.getParticipantName();
                }
            }
        } else {
            IPCTopologyEndpoint egressEndpoint = endpointWUP.getEgressEndpoint();
            if(egressEndpoint != null){
                if(egressEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.MLLP_CLIENT)){
                    participantName= egressEndpoint.getParticipantName();
                } else if(egressEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.HTTP_API_CLIENT)){
                    participantName = egressEndpoint.getParticipantName();
                } else if(egressEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.FILE_SHARE_SINK)){
                    participantName = egressEndpoint.getParticipantName();
                }
            }
        }

        getLogger().debug(".getEndpointParticipantName(): Exit, participantName->{}", participantName);
        return(participantName);
    }

    //
    // Getters (And Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

}
