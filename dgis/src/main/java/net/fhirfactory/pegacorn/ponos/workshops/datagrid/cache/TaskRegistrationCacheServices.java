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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache;

import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridElementKeyInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceServiceRegistrationType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.PonosDatagridTaskKey;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceResourceStatusEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelTypeDescriptor;
import net.fhirfactory.pegacorn.core.model.petasos.participant.id.PetasosParticipantId;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.collections.PetasosActionableTaskSet;
import net.fhirfactory.pegacorn.core.model.petasos.task.collections.PetasosTaskIdSet;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.valuesets.TaskExecutionCommandEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.valuesets.TaskStorageStatusEnum;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.core.PonosReplicatedCacheServices;
import net.fhirfactory.pegacorn.services.tasks.datatypes.PetasosActionableTaskRegistrationType;
import org.apache.commons.lang3.SerializationUtils;
import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class TaskRegistrationCacheServices {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRegistrationCacheServices.class);

    private boolean initialised;

    private static final String PONOS_TASK_PERSISTENCE_SERVICE = "Ponos-ActionableTask-Persistence-Service";

    private Cache<DatagridElementKeyInterface, PetasosActionableTask> taskCache;
    private Object taskCacheLock;

    private Cache<DatagridElementKeyInterface, PetasosActionableTaskRegistrationType> taskRegistrationCache;
    private Cache<DataParcelTypeDescriptor, DatagridPersistenceServiceRegistrationType> taskPersistenceServiceCache;

    private Cache<DatagridElementKeyInterface, Boolean> taskJourneyReportedMap;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private PonosReplicatedCacheServices replicatedCacheServices;

    //
    // Constructor(s)
    //

    public TaskRegistrationCacheServices(){
        super();
        this.initialised = false;
        this.taskCacheLock = new Object();
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise(){
        getLogger().debug(".initialise(): Entry");
        if(!initialised) {
            getLogger().info(".initialise(): Initialisation Start");

            getLogger().info(".initialise(): [Initialising Caches] Start");
            taskRegistrationCache = replicatedCacheServices.getCacheManager().createCache("ActionableTaskRegistrationCache", replicatedCacheServices.getCacheConfigurationBuild());
            getLogger().info(".initialise(): [Initialising Caches] End");

            getLogger().info(".initialise(): Initialisation Finish");
        } else {
            getLogger().debug(".initialise(): Nothing to do, already initialised");
        }
        getLogger().debug(".initialise(): Exit");
    }

    //
    // Business Methods
    //

    public PetasosActionableTaskRegistrationType registerPetasosActionableTask(PetasosActionableTask actionableTask) {
        getLogger().debug(".registerPetasosActionableTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask == null) {
            getLogger().debug(".registerPetasosActionableTask(): Exit, actionableTask is null");
            return null;
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(actionableTask.getTaskId());
        PetasosActionableTaskRegistrationType actionableTaskRegistration = null;
        boolean taskAlreadyRegistered = false;
        if(getTaskRegistrationCache().containsKey(entryKey)){
            actionableTaskRegistration = SerializationUtils.clone(getTaskRegistrationCache().get(actionableTask.getTaskId()));
            taskAlreadyRegistered = true;
        } else {
            actionableTaskRegistration = new PetasosActionableTaskRegistrationType();
            actionableTaskRegistration.setActionableTaskId(actionableTask.getTaskId());
            actionableTaskRegistration.setRegistrationInstant(Instant.now());
            actionableTaskRegistration.setCheckInstant(Instant.now());
        }
        actionableTaskRegistration.addPerformerParticipantId(actionableTask.getTaskFulfillment().getFulfiller().getParticipant().getParticipantId());
        actionableTaskRegistration.addPerformerComponentId(actionableTask.getTaskFulfillment().getFulfiller().getComponentId());
        actionableTaskRegistration.addPerformerTypes(actionableTask.getTaskPerformerTypes());
        if(!taskAlreadyRegistered){
            actionableTask.setRegistered(true);
            actionableTask.getTaskFulfillment().setRegistrationInstant(Instant.now());
            getTaskCache().put(entryKey, actionableTask);
            getTaskRegistrationCache().put(entryKey, actionableTaskRegistration);
            getTaskJourneyReportedMap().putIfAbsent(entryKey, false);

        } else {
            getTaskCache().replace(entryKey, actionableTask);
            getTaskRegistrationCache().replace(entryKey, actionableTaskRegistration);
        }
        getLogger().debug(".registerPetasosActionableTask(): Exit, actionableTaskRegistration->{}", actionableTaskRegistration);
        return(actionableTaskRegistration);
    }

    public PetasosActionableTaskRegistrationType updatePetasosActionableTask(PetasosActionableTask actionableTask) {
        getLogger().debug(".updatePetasosActionableTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask == null) {
            getLogger().debug(".updatePetasosActionableTask(): Exit, actionableTask is null");
            return null;
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(actionableTask.getTaskId());
        PetasosActionableTaskRegistrationType actionableTaskRegistration = null;
        synchronized (getTaskCacheLock()) {
            if (getTaskRegistrationCache().containsKey(entryKey)) {
                PetasosActionableTask registeredActionableTask = getTaskCache().get(entryKey);
                actionableTaskRegistration = SerializationUtils.clone(getTaskRegistrationCache().get(entryKey));
                registeredActionableTask.update(actionableTask);
                actionableTaskRegistration.setCheckInstant(Instant.now());
                actionableTaskRegistration.addPerformerTypes(actionableTask.getTaskPerformerTypes());
                actionableTaskRegistration.addPerformerParticipantId(actionableTask.getTaskFulfillment().getFulfiller().getParticipant().getParticipantId());
                actionableTaskRegistration.addPerformerComponentId(actionableTask.getTaskFulfillment().getFulfiller().getComponentId());
                getTaskRegistrationCache().replace(entryKey, actionableTaskRegistration);
            } else {
                actionableTaskRegistration = registerPetasosActionableTask(actionableTask);
            }
        }
        getLogger().debug(".updatePetasosActionableTask(): Exit, actionableTaskRegistration->{}", actionableTaskRegistration);
        return(actionableTaskRegistration);
    }

    public PetasosActionableTask getPetasosActionableTask(TaskIdType taskId) {
        getLogger().debug(".getPetasosActionableTask(): Entry, taskId->{}", taskId);
        if(taskId == null) {
            getLogger().debug(".getPetasosActionableTask(): Exit, taskId is null");
            return null;
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(taskId);
        if(getTaskCache().containsKey(entryKey)){
            PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
            getLogger().debug(".getPetasosActionableTask(): Exit, actionableTask->{}", actionableTask);
            return (actionableTask);
        }
        getLogger().debug(".getPetasosActionableTask(): Exit, PetasosActionableTask with taskId={} is not in the cache!", taskId);
        return(null);
    }

    public List<PetasosActionableTask> getPetasosActionableTasksForComponent(ComponentIdType componentId) {
        getLogger().debug(".getPetasosActionableTasksForComponent(): Entry, componentId->{}", componentId);
        if(componentId == null){
            getLogger().debug(".getPetasosActionableTasksForComponent(): Exit, componentId is null, returning empty list");
            return(new ArrayList<>());
        }
        List<PetasosActionableTask> activeActionableTasks = new ArrayList<>();
        synchronized (getTaskCacheLock()) {
            for (PetasosActionableTaskRegistrationType currentTaskRegistration : getTaskRegistrationCache().values()) {
                for (ComponentIdType currentComponentId : currentTaskRegistration.getFulfillerComponentIdSet()) {
                    if (currentComponentId.equals(componentId)) {
                        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(currentTaskRegistration.getActionableTaskId());
                        PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
                        activeActionableTasks.add(actionableTask);
                    }
                }
            }
        }
        getLogger().debug(".getPetasosActionableTasksForComponent(): Exit");
        return(activeActionableTasks);
    }

    public Boolean hasOffloadedPendingActionableTasks(PetasosParticipantId participantId){
        getLogger().debug(".hasOffloadedPendingActionableTasks(): Entry, participantId->{}", participantId);
        if(participantId == null){
            getLogger().debug(".hasOffloadedPendingActionableTasks(): Exit, participantId is null, returning empty list");
            return(false);
        }
        boolean foundOffloadedPendingTask = false;
        synchronized (getTaskCacheLock()) {
            for (PetasosActionableTaskRegistrationType currentTaskRegistration : getTaskRegistrationCache().values()) {
                for (PetasosParticipantId currentParticipant : currentTaskRegistration.getFulfillerParticipantIdSet()) {
                    if (currentParticipant.equals(participantId)) {
                        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(currentTaskRegistration.getActionableTaskId());
                        PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
                        boolean isNotLocallyBuffered = actionableTask.getTaskTraceability().getPersistenceStatus().getLocalStorageStatus().equals(TaskStorageStatusEnum.TASK_UNSAVED);
                        boolean isInRegisteredState = actionableTask.getTaskFulfillment().getStatus().equals(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_REGISTERED);
                        boolean isInWaitingExecutionState = actionableTask.getExecutionControl().getExecutionCommand().equals(TaskExecutionCommandEnum.TASK_COMMAND_WAIT);
                        if (isNotLocallyBuffered && isInRegisteredState && isInWaitingExecutionState) {
                            foundOffloadedPendingTask = true;
                            break;
                        }
                    }
                }
                if (foundOffloadedPendingTask) {
                    break;
                }
            }
        }
        getLogger().debug(".hasOffloadedPendingActionableTasks(): Exit, foundOffloadedPendingTask->{}", foundOffloadedPendingTask);
        return(foundOffloadedPendingTask);
    }

    public PetasosActionableTaskSet getAllPendingActionableTasks(PetasosParticipantId participantId) {
        getLogger().debug(".getPendingActionableTasks(): Entry, participantId->{}", participantId);
        if(participantId == null){
            getLogger().debug(".getPendingActionableTasks(): Exit, participantId is null, returning empty list");
            return(new PetasosActionableTaskSet());
        }
        PetasosActionableTaskSet waitingActionableTasks = new PetasosActionableTaskSet();
        synchronized (getTaskCacheLock()) {
            for (PetasosActionableTaskRegistrationType currentTaskRegistration : getTaskRegistrationCache().values()) {
                for (PetasosParticipantId currentParticipant : currentTaskRegistration.getFulfillerParticipantIdSet()) {
                    if (currentParticipant.equals(participantId)) {
                        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(currentTaskRegistration.getActionableTaskId());
                        PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
                        boolean isInRegisteredState = actionableTask.getTaskFulfillment().getStatus().equals(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_REGISTERED);
                        boolean isInWaitingExecutionState = actionableTask.getExecutionControl().getExecutionCommand().equals(TaskExecutionCommandEnum.TASK_COMMAND_WAIT);
                        if (isInRegisteredState && isInWaitingExecutionState) {
                            waitingActionableTasks.addActionableTask(actionableTask);
                        }
                    }
                }
            }
        }
        getLogger().debug(".getPendingActionableTasks(): Exit");
        return(waitingActionableTasks);
    }

    public PetasosActionableTaskSet getAllOffloadedPendingActionableTasks(PetasosParticipantId participantId) {
        getLogger().debug(".getPendingActionableTasks(): Entry, participantId->{}", participantId);
        if(participantId == null){
            getLogger().debug(".getPendingActionableTasks(): Exit, participantId is null, returning empty list");
            return(new PetasosActionableTaskSet());
        }
        PetasosActionableTaskSet waitingActionableTasks = new PetasosActionableTaskSet();
        synchronized (getTaskCacheLock()) {
            for (PetasosActionableTaskRegistrationType currentTaskRegistration : getTaskRegistrationCache().values()) {
                for (PetasosParticipantId currentParticipant : currentTaskRegistration.getFulfillerParticipantIdSet()) {
                    if (currentParticipant.equals(participantId)) {
                        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(currentTaskRegistration.getActionableTaskId());
                        PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
                        boolean isNotLocallyBuffered = actionableTask.getTaskTraceability().getPersistenceStatus().getLocalStorageStatus().equals(TaskStorageStatusEnum.TASK_UNSAVED);
                        boolean isInRegisteredState = actionableTask.getTaskFulfillment().getStatus().equals(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_REGISTERED);
                        boolean isInWaitingExecutionState = actionableTask.getExecutionControl().getExecutionCommand().equals(TaskExecutionCommandEnum.TASK_COMMAND_WAIT);
                        if (isNotLocallyBuffered && isInRegisteredState && isInWaitingExecutionState) {
                            waitingActionableTasks.addActionableTask(actionableTask);
                        }
                    }
                }
            }
        }
        getLogger().debug(".getPendingActionableTasks(): Exit");
        return(waitingActionableTasks);
    }

    public Integer markTasksAsOffloaded(PetasosParticipantId participantId, PetasosTaskIdSet taskIdSet){
        getLogger().debug(".markTasksAsOffloaded(): Entry, participantId->{}", participantId);
        if(participantId == null){
            getLogger().debug(".markTasksAsOffloaded(): Exit, participantId is null, returning 0");
            return(0);
        }
        Integer count = 0;
        synchronized (getTaskCacheLock()) {
            for (PetasosActionableTaskRegistrationType currentTaskRegistration : getTaskRegistrationCache().values()) {
                for (PetasosParticipantId currentParticipant : currentTaskRegistration.getFulfillerParticipantIdSet()) {
                    if (currentParticipant.equals(participantId)) {
                        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(currentTaskRegistration.getActionableTaskId());
                        PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
                        if (actionableTask != null) {
                            actionableTask.getExecutionControl().setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_WAIT);
                            actionableTask.getTaskFulfillment().setStatus(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_REGISTERED);
                            actionableTask.getTaskTraceability().getPersistenceStatus().setLocalStorageStatus(TaskStorageStatusEnum.TASK_UNSAVED);
                            actionableTask.getTaskTraceability().getPersistenceStatus().setLocalStorageInstant(Instant.now());
                            count += 1;
                        }
                    }
                }
            }
        }
        getLogger().debug(".markTasksAsOffloaded(): Exit");
        return(count);
    }

    public PetasosTaskIdSet synchroniseOffloadedTaskSet(PetasosParticipantId participantId, PetasosTaskIdSet knownLocalBufferedTasks){
        getLogger().debug(".markTasksAsOffloaded(): Entry, participantId->{}", participantId);
        if(participantId == null){
            getLogger().debug(".markTasksAsOffloaded(): Exit, participantId is null, returning 0");
            return(knownLocalBufferedTasks);
        }
        PetasosTaskIdSet actualOffloadedTaskSet = new PetasosTaskIdSet();
        synchronized (getTaskCacheLock()) {
            for (PetasosActionableTaskRegistrationType currentTaskRegistration : getTaskRegistrationCache().values()) {
                for (PetasosParticipantId currentParticipant : currentTaskRegistration.getFulfillerParticipantIdSet()) {
                    if (currentParticipant.equals(participantId)) {
                        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(currentTaskRegistration.getActionableTaskId());
                        PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
                        if (knownLocalBufferedTasks.getTaskIdSet().contains(actionableTask.getTaskId())) {
                            actionableTask.getTaskTraceability().getPersistenceStatus().setLocalStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
                            actionableTask.getTaskTraceability().getPersistenceStatus().setLocalStorageInstant(Instant.now());
                            actionableTask.getTaskTraceability().getPersistenceStatus().setLocalStorageLocation(participantId.getName());
                        } else {
                            boolean isInRegisteredState = actionableTask.getTaskFulfillment().getStatus().equals(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_REGISTERED);
                            boolean isInWaitingExecutionState = actionableTask.getExecutionControl().getExecutionCommand().equals(TaskExecutionCommandEnum.TASK_COMMAND_WAIT);
                            if (isInRegisteredState && isInWaitingExecutionState) {
                                actualOffloadedTaskSet.addTaskId(actionableTask.getTaskId());
                            }
                        }
                    }
                }
            }
        }
        getLogger().debug(".markTasksAsOffloaded(): Exit");
        return(actualOffloadedTaskSet);
    }

    public boolean archivePetasosActionableTask(PetasosActionableTask actionableTask) {
        boolean success = false;
        if(actionableTask != null){
            success = archivePetasosActionableTask(actionableTask.getTaskId());
        }
        return false;
    }

    public boolean archivePetasosActionableTask(TaskIdType taskId) {
        getLogger().debug(".archivePetasosActionableTask(): Entry, taskId->{}", taskId);
        if(taskId == null){
            getLogger().debug(".archivePetasosActionableTask(): Exit, taskId is null");
            return(false);
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(taskId);
        PetasosActionableTaskRegistrationType actionableTaskRegistrationType = getTaskRegistrationCache().get(entryKey);
        actionableTaskRegistrationType.setResourceStatus(DatagridPersistenceResourceStatusEnum.RESOURCE_SAVE_REQUESTED);
        return(true);
    }

    public List<PetasosActionableTask> getLastInChainActionableEvents(){
        getLogger().debug(".getLastInChainActionableEvents(): Entry");

        List<PetasosActionableTask> endedJourneyList = new ArrayList<>();
        synchronized (getTaskCacheLock()) {
            CacheCollection<PetasosActionableTask> values = getTaskCache().values();
            for(PetasosActionableTask currentTask: values){
                if(getLogger().isInfoEnabled()){
                    getLogger().trace(".getLastInChainActionableEvents(): Iterating, currentTask->{}", currentTask);
                }
                if(currentTask.hasTaskCompletionSummary()){
                    if(currentTask.getTaskCompletionSummary().isLastInChain()){
                        endedJourneyList.add(SerializationUtils.clone(currentTask));
                    }
                }
            }
        }
        if(getLogger().isInfoEnabled()) {
            getLogger().debug(".getLastInChainActionableEvents(): Exit, number of entries->{}", endedJourneyList.size());
        }
        return(endedJourneyList);
    }

    public boolean hasAlreadyBeenReportedOn(TaskIdType taskId){
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(taskId);
        if(getTaskJourneyReportedMap().containsKey(entryKey)){
            return(getTaskJourneyReportedMap().get(entryKey));
        } else {
            getTaskJourneyReportedMap().put(entryKey, false);
            return(false);
        }
    }

    public void setReportStatus(TaskIdType taskId, boolean status){
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(taskId);
        if(getTaskJourneyReportedMap().containsKey(entryKey)){
            getTaskJourneyReportedMap().replace(entryKey, status);
        } else {
            getTaskJourneyReportedMap().put(entryKey, status);
        }
    }

    //
    // Cache Cleanup
    //

    public Set<DatagridElementKeyInterface> getAgedCacheContent(Long thresholdAge){
        CacheSet<DatagridElementKeyInterface> datagridElementKeys = getTaskRegistrationCache().keySet();
        Set<DatagridElementKeyInterface> agedTaskSet = new HashSet<>();
        Instant now = Instant.now();
        synchronized (getTaskCacheLock()) {
            for (DatagridElementKeyInterface currentKey : datagridElementKeys) {
                PetasosActionableTaskRegistrationType petasosActionableTaskRegistration = getTaskRegistrationCache().get(currentKey);
                if (petasosActionableTaskRegistration.getRegistrationInstant() == null) {
                    petasosActionableTaskRegistration.setRegistrationInstant(Instant.now());
                }
                Long age = now.getEpochSecond() - petasosActionableTaskRegistration.getRegistrationInstant().getEpochSecond();
                if (age > thresholdAge) {
                    if (!agedTaskSet.contains(currentKey)) {
                        agedTaskSet.add(currentKey);
                    }
                }
            }
        }
        return(agedTaskSet);
    }

    public void clearTaskFromCache(DatagridElementKeyInterface key){
        if(key != null) {
            synchronized (getTaskCacheLock()) {
                getTaskCache().remove(key);
                getTaskRegistrationCache().remove(key);
                getTaskJourneyReportedMap().remove(key);
            }
        }
    }

    //
    // Cache Size Information
    public int getTaskCacheSize(){
        int size = getTaskCache().size();
        return(size);
    }

    public int getTaskPersistenceServiceCacheSize(){
        int size = getTaskPersistenceServiceCache().size();
        return(size);
    }

    public int getTaskRegistrationCacheSize(){
        int size = getTaskRegistrationCache().size();
        return(size);
    }

    //
    // Getters and Setters (and Helpers)
    //

    protected Logger getLogger() {
        return (LOG);
    }

    protected Cache<DatagridElementKeyInterface, PetasosActionableTask> getTaskCache(){
        return(this.taskCache);
    }

    protected Cache<DatagridElementKeyInterface, PetasosActionableTaskRegistrationType> getTaskRegistrationCache(){
        return(this.taskRegistrationCache);
    }

    protected Cache<DataParcelTypeDescriptor, DatagridPersistenceServiceRegistrationType> getTaskPersistenceServiceCache(){
        return(this.taskPersistenceServiceCache);
    }

    private Object getTaskCacheLock(){
        return(taskCacheLock);
    }

    private Cache<DatagridElementKeyInterface, Boolean> getTaskJourneyReportedMap(){
        return(this.taskJourneyReportedMap);
    }
}
