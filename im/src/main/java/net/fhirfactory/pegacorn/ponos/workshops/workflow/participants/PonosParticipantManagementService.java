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
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipantControlStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.participant.registration.PetasosParticipantRegistration;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.TaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.ui.resources.simple.PetasosParticipantESR;
import net.fhirfactory.pegacorn.core.model.ui.resources.summaries.PetasosParticipantSummary;
import net.fhirfactory.pegacorn.core.model.ui.resources.summaries.TaskSummary;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.ParticipantCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.oam.ProcessingPlantPathwayReportProxy;
import net.fhirfactory.pegacorn.services.tasks.endpoint.PetasosParticipantServicesManagerEndpoint;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class PonosParticipantManagementService extends PetasosParticipantServicesManagerEndpoint implements ParticipantUIServicesAPI, TaskUIServicesAPI {
    private static final Logger LOG = LoggerFactory.getLogger(PonosParticipantManagementService.class);

    @Inject
    private ParticipantCacheServices petasosParticipantCache;

    @Inject
    private ProcessingPlantPathwayReportProxy processingPlantPathwayReportProxy;


    public boolean isPetasosParticipantRegistered(PetasosParticipantRegistration localParticipantRegistration) {
        getLogger().debug(".isPetasosParticipantRegistered(): Entry, localParticipantRegistration->{}", localParticipantRegistration);
        boolean isRegistered = petasosParticipantCache.getParticipantRegistration(localParticipantRegistration.getLocalComponentId()) != null;
        getLogger().debug(".isPetasosParticipantRegistered(): Exit, isRegistered->{}", isRegistered);
        return(isRegistered);
    }

    //
    // Business Methods
    //

    public Set<PetasosParticipantRegistration> getDownstreamSubscribersHandler(String sourceComponentName, String sourceParticipantName, String producerParticipantName){
        getLogger().debug(".getDownstreamSubscribersHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, producerParticipantName->{}", sourceComponentName, sourceParticipantName, producerParticipantName);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".getDownstreamSubscribersHandler(): [Get Downstream Subscribers from Central Cache] Start");
        Set<PetasosParticipantRegistration> subscribers = getDownstreamSubscribers(producerParticipantName);
        getLogger().trace(".getDownstreamSubscribersHandler(): [Get Downstream Subscribers from Central Cache] Finish");

        getLogger().debug(".getDownstreamSubscribersHandler(): Exit, subscribers->{}", subscribers);
        return(subscribers);
    }

    @Override
    public Set<PetasosParticipantRegistration> getDownstreamSubscribers(String producerParticipantName) {
        getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): Entry, producerParticipantName->{}", producerParticipantName);
        Set<PetasosParticipantRegistration> subscriberSet = petasosParticipantCache.getDownstreamParticipantSet(producerParticipantName);
        if(getLogger().isInfoEnabled()){
            getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): subscriberSet->{}", subscriberSet);
        }
        processingPlantPathwayReportProxy.touchProcessingPlantSubscriptionSynchronisationInstant(producerParticipantName);
        getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): Exit");
        return(subscriberSet);
    }

    public PetasosParticipantRegistration synchroniseRegistrationHandler(String sourceComponentName, String sourceParticipantName, PetasosParticipantRegistration localParticipantRegistration){
        getLogger().debug(".getDownstreamSubscribersHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, localParticipantRegistration->{}", sourceComponentName, sourceParticipantName, localParticipantRegistration);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".getDownstreamSubscribersHandler(): [Synchronise Registration with Central Cache] Start");
        PetasosParticipantRegistration centralRegistration = synchroniseRegistration(localParticipantRegistration);
        getLogger().trace(".getDownstreamSubscribersHandler(): [Synchronise Registration with Central Cache] Finish");

        getLogger().debug(".getDownstreamSubscribersHandler(): Exit, centralRegistration->{}", centralRegistration);
        return(centralRegistration);
    }

    @Override
    public PetasosParticipantRegistration synchroniseRegistration(PetasosParticipantRegistration localParticipantRegistration) {
        getLogger().info(".registerPetasosParticipant(): Entry, localParticipantRegistration->{}", localParticipantRegistration);
        if(localParticipantRegistration == null) {
            getLogger().info(".registerPetasosParticipant(): Exit, localParticipantRegistration is null, returning null");
            return (null);
        }
        PetasosParticipantRegistration registration = null;
        if(isPetasosParticipantRegistered(localParticipantRegistration)){
            registration = petasosParticipantCache.updateParticipantRegistration(localParticipantRegistration);
            processingPlantPathwayReportProxy.reportParticipantRegistration(localParticipantRegistration, true);
        } else {
            registration = petasosParticipantCache.registerPetasosParticipant(localParticipantRegistration);
            processingPlantPathwayReportProxy.reportParticipantRegistration(localParticipantRegistration, false);
        }
        getLogger().info(".registerPetasosParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    public PetasosParticipantRegistration getParticipantRegistration(ComponentIdType componentId) {
        getLogger().debug(".getPetasosParticipantRegistration(): Entry, componentId->{}", componentId);
        if(componentId == null){
            getLogger().debug(".getPetasosParticipantRegistration(): Exit, componentId is null, returning null");
            return(null);
        }
        PetasosParticipantRegistration registration = petasosParticipantCache.deregisterPetasosParticipant(componentId);
        getLogger().debug(".getPetasosParticipantRegistration(): Exit, registration->{}", registration);
        return(registration);
    }

    public Set<PetasosParticipantRegistration> getParticipantRegistrationSetForParticipantName(String participantName) {
        getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Entry, participantName->{}", participantName);
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Exit, participantName is null, returning empty set");
            return(new HashSet<>());
        }
        Set<PetasosParticipantRegistration> downstreamTaskPerformers = petasosParticipantCache.getParticipantRegistrationSetForParticipantName(participantName);
        getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Exit");
        return (downstreamTaskPerformers);
    }

    public Set<PetasosParticipantRegistration> getAllRegistrations() {
        Set<PetasosParticipantRegistration> allRegistrations = petasosParticipantCache.getAllRegistrations();
        return (allRegistrations);
    }

    //
    // Participant Command API
    //

    public List<PetasosParticipantSummary> listSubsystemsHandler(String sourceComponentName, String sourceParticipantName){
        getLogger().debug(".listSubsystemsHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}", sourceComponentName, sourceParticipantName);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);
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
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);
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
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);
        PetasosParticipantSummary response = setControlStatus(participantName, controlStatus);
        getLogger().debug(".setControlStatusHandler(): Exit, response->{}", response);
        return(response);
    }

    @Override
    public PetasosParticipantSummary setControlStatus(String participantName, PetasosParticipantControlStatusEnum controlStatus) {
        return null;
    }

    public PetasosParticipantSummary getParticipantSummaryHandler(String sourceComponentName, String sourceParticipantName, String participantName){
        getLogger().debug(".getParticipantSummaryHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantName->{}", sourceComponentName, sourceParticipantName, participantName);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);
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
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);
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
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);
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
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);
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
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);
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
