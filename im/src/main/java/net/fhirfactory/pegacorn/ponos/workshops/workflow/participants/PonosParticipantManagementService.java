/*
 * Copyright (c) 2020 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.participants;

import net.fhirfactory.pegacorn.core.interfaces.ui.resources.ParticipantUIServicesAPI;
import net.fhirfactory.pegacorn.core.interfaces.ui.resources.TaskUIServicesAPI;
import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipant;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipantControlStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.TaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.ui.resources.simple.PetasosParticipantESR;
import net.fhirfactory.pegacorn.core.model.ui.resources.summaries.PetasosParticipantSummary;
import net.fhirfactory.pegacorn.core.model.ui.resources.summaries.TaskSummary;
import net.fhirfactory.pegacorn.core.model.ui.resources.summaries.factories.ParticipantSummaryFactory;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.ParticipantCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.oam.ProcessingPlantPathwayReportProxy;
import net.fhirfactory.pegacorn.services.tasks.endpoint.PetasosParticipantServicesManagerEndpoint;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class PonosParticipantManagementService extends PetasosParticipantServicesManagerEndpoint implements ParticipantUIServicesAPI, TaskUIServicesAPI {
    private static final Logger LOG = LoggerFactory.getLogger(PonosParticipantManagementService.class);

    @Inject
    private ParticipantCacheServices petasosParticipantCache;

    @Inject
    private ProcessingPlantPathwayReportProxy processingPlantPathwayReportProxy;

    @Inject
    private ParticipantSummaryFactory participantSummaryFactory;


    public boolean isPetasosParticipantRegistered(PetasosParticipant localParticipantRegistration) {
        getLogger().debug(".isPetasosParticipantRegistered(): Entry, localParticipantRegistration->{}", localParticipantRegistration);
        boolean isRegistered = petasosParticipantCache.getParticipant(localParticipantRegistration.getParticipantId().getName()) != null;
        getLogger().debug(".isPetasosParticipantRegistered(): Exit, isRegistered->{}", isRegistered);
        return(isRegistered);
    }

    //
    // Business Methods
    //

    public Set<PetasosParticipant> getDownstreamSubscribersHandler(String sourceComponentName, String sourceParticipantName, String producerParticipantName){
        getLogger().debug(".getDownstreamSubscribersHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, producerParticipantName->{}", sourceComponentName, sourceParticipantName, producerParticipantName);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);

        getLogger().trace(".getDownstreamSubscribersHandler(): [Get Downstream Subscribers from Central Cache] Start");
        Set<PetasosParticipant> subscribers = getDownstreamSubscribers(producerParticipantName);
        getLogger().trace(".getDownstreamSubscribersHandler(): [Get Downstream Subscribers from Central Cache] Finish");

        getLogger().debug(".getDownstreamSubscribersHandler(): Exit, subscribers->{}", subscribers);
        return(subscribers);
    }

    @Override
    public Set<PetasosParticipant> getDownstreamSubscribers(String producerParticipantName) {
        getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): Entry, producerParticipantName->{}", producerParticipantName);
        Set<PetasosParticipant> subscriberSet = petasosParticipantCache.getDownstreamParticipantSet(producerParticipantName);
        if(getLogger().isInfoEnabled()){
            getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): subscriberSet->{}", subscriberSet);
        }
        processingPlantPathwayReportProxy.touchProcessingPlantSubscriptionSynchronisationInstant(producerParticipantName);
        getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): Exit");
        return(subscriberSet);
    }

    public PetasosParticipant synchroniseRegistrationHandler(String sourceComponentName, String sourceParticipantName, PetasosParticipant localParticipantRegistration){
        getLogger().debug(".synchroniseRegistrationHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, localParticipantRegistration->{}", sourceComponentName, sourceParticipantName, localParticipantRegistration);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);

        getLogger().trace(".synchroniseRegistrationHandler(): [Synchronise Registration with Central Cache] Start");
        PetasosParticipant centralRegistration = synchroniseRegistration(localParticipantRegistration);
        getLogger().trace(".synchroniseRegistrationHandler(): [Synchronise Registration with Central Cache] Finish");

        getLogger().debug(".synchroniseRegistrationHandler(): Exit, centralRegistration->{}", centralRegistration);
        return(centralRegistration);
    }

    @Override
    public PetasosParticipant synchroniseRegistration(PetasosParticipant participant) {
        getLogger().info(".synchroniseRegistration(): Entry, participant->{}", participant);
        if(participant == null) {
            getLogger().info(".synchroniseRegistration(): Exit, participant is null, returning null");
            return (null);
        }

        PetasosParticipant cachedParticipant = null;
        if(isPetasosParticipantRegistered(participant)){
            getLogger().trace(".synchroniseRegistration(): Already Registered, so updating");
            cachedParticipant = petasosParticipantCache.updateParticipantRegistration(participant);
            if(cachedParticipant.getUpdateInstant().isAfter(cachedParticipant.getReportingInstant())){
                processingPlantPathwayReportProxy.reportParticipantRegistration(SerializationUtils.clone(participant), true);
                cachedParticipant.setReportingInstant(Instant.now());
            }
        } else {
            getLogger().trace(".synchroniseRegistration(): Not presently registered, so registering");
            cachedParticipant = petasosParticipantCache.registerPetasosParticipant(participant);
            processingPlantPathwayReportProxy.reportParticipantRegistration(SerializationUtils.clone(participant), false);
            cachedParticipant.setReportingInstant(Instant.now());
        }

        getLogger().trace(".synchroniseRegistration(): [Synchronise Central Queue Status] Start");
        cachedParticipant.getTaskQueueStatus().setPendingTasksPersisted(false);
        cachedParticipant.getTaskQueueStatus().setPendingTasksOffloaded(false);
        getLogger().trace(".synchroniseRegistration(): [Synchronise Central Queue Status] Finish");

        getLogger().trace(".synchroniseRegistration(): [Synchronise Control Status] Start");
        if(!cachedParticipant.getControlStatus().equals(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED)) {
            cachedParticipant.setControlStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED);
        }
        getLogger().trace(".synchroniseRegistration(): [Synchronise Control Status] Finish");


        getLogger().info(".synchroniseRegistration(): Exit, cachedParticipant->{}", cachedParticipant);
        return(cachedParticipant);
    }

    public PetasosParticipant getParticipantRegistration(ComponentIdType componentId) {
        getLogger().debug(".getParticipantRegistration(): Entry, componentId->{}", componentId);
        if(componentId == null){
            getLogger().debug(".getParticipantRegistration(): Exit, componentId is null, returning null");
            return(null);
        }
        PetasosParticipant registration = petasosParticipantCache.deregisterPetasosParticipant(componentId);
        getLogger().debug(".getParticipantRegistration(): Exit, registration->{}", registration);
        return(registration);
    }

    public Set<PetasosParticipant> getAllRegistrations() {
        Set<PetasosParticipant> allRegistrations = petasosParticipantCache.getAllRegistrations();
        return (allRegistrations);
    }

    //
    // Participant Command API
    //

    public List<PetasosParticipantSummary> listSubsystemsHandler(String sourceComponentName, String sourceParticipantName){
        getLogger().debug(".listSubsystemsHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}", sourceComponentName, sourceParticipantName);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);
        List<PetasosParticipantSummary> response = listSubsystems();
        getLogger().debug(".listSubsystemsHandler(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public List<PetasosParticipantSummary> listSubsystems() {
        return null;
    }

    public List<PetasosParticipantSummary> listParticipantsHandler(String sourceComponentName, String sourceParticipantName, String subsystemName){
        getLogger().debug(".listParticipantsHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, subsystemName->{}", sourceComponentName, sourceParticipantName, subsystemName);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);
        List<PetasosParticipantSummary> response = listParticipants(subsystemName);
        getLogger().debug(".listParticipantsHandler(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public List<PetasosParticipantSummary> listParticipants(String subsystemName) {
        return null;
    }

    public PetasosParticipantSummary setControlStatusHandler(String sourceComponentName, String sourceParticipantName, String participantName, PetasosParticipantControlStatusEnum controlStatus){
        getLogger().debug(".setControlStatusHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantName->{}", sourceComponentName, sourceParticipantName, participantName);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);
        PetasosParticipantSummary response = setControlStatus(participantName, controlStatus);
        getLogger().debug(".setControlStatusHandler(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public PetasosParticipantSummary setControlStatus(String participantName, PetasosParticipantControlStatusEnum controlStatus) {
        getLogger().debug(".setControlStatus(): Entry, participantName->{}, controlStatus->{}", participantName, controlStatus);
        if(StringUtils.isEmpty(participantName)){
            return(null);
        }
        PetasosParticipant participant = petasosParticipantCache.getParticipant(participantName);
        PetasosParticipantSummary participantSummary = null;
        if(participant != null){
            participant.setControlStatus(controlStatus);
            participantSummary = participantSummaryFactory.newParticipantSummary(participant);
        }
        getLogger().debug(".setControlStatus(): Exit, participantSummary->{}", participantSummary);
        return(participantSummary);
    }

    public PetasosParticipantSummary getParticipantSummaryHandler(String sourceComponentName, String sourceParticipantName, String participantName){
        getLogger().debug(".getParticipantSummaryHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantName->{}", sourceComponentName, sourceParticipantName, participantName);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);
        PetasosParticipantSummary response = getParticipantSummary(participantName);
        getLogger().debug(".getParticipantSummaryHandler(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public PetasosParticipantSummary getParticipantSummary(String participantName) {
        return null;
    }

    public PetasosParticipantESR getParticipantESRHandler(String sourceComponentName, String sourceParticipantName, String participantName){
        getLogger().debug(".getParticipantESRHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantName->{}", sourceComponentName, sourceParticipantName, participantName);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);
        PetasosParticipantESR response = getParticipantESR(participantName);
        getLogger().debug(".getParticipantESRHandler(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public PetasosParticipantESR getParticipantESR(String participantName) {
        return null;
    }

    public List<TaskSummary> listTasks(String sourceComponentName, String sourceParticipantName, String participantName, TaskOutcomeStatusEnum status, boolean order, Instant startTime, Instant endTime){
        getLogger().debug(".listTasks(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantName->{}, status->{}, order->{}, startTime->{}, endTime->{}", sourceComponentName, sourceParticipantName, participantName, status, order, startTime, endTime);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);
        List<TaskSummary> response = listTasks(participantName, status, order, startTime, endTime);
        getLogger().debug(".listTasks(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public List<TaskSummary> listTasks(String participant, TaskOutcomeStatusEnum status, boolean order, Instant startTime, Instant endTime) {
        return null;
    }

    public TaskSummary redoTaskHandler(String sourceComponentName, String sourceParticipantName, String taskId){
        getLogger().debug(".redoTaskHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, taskId->{}", sourceComponentName, sourceParticipantName, taskId);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);
        TaskSummary response = initiateTaskRedo(taskId);
        getLogger().debug(".redoTaskHandler(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public TaskSummary initiateTaskRedo(String taskId) {
        return null;
    }

    public TaskSummary cancelTaskHandler(String sourceComponentName, String sourceParticipantName, String taskId){
        getLogger().debug(".cancelTaskHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, taskId->{}", sourceComponentName, sourceParticipantName, taskId);
        getMetricsAgent().incrementReceivedRPCRequestCount(sourceParticipantName);
        TaskSummary response = initiateTaskCancellation(taskId);
        getLogger().debug(".redoTaskHcancelTaskHandlerandler(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public TaskSummary initiateTaskCancellation(String taskId) {
        return null;
    }

    //
    // Getters (and Setters)
    //
    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    @Override
    protected void initialiseCacheSynchronisationDaemon() {

    }
}
