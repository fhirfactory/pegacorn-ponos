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
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceResourceCapabilityType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceServiceRegistrationType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceServiceType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.PonosDatagridTaskKey;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceResourceStatusEnum;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceServiceDeploymentScopeEnum;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceServiceResourceScopeEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelTypeDescriptor;
import net.fhirfactory.pegacorn.core.model.petasos.participant.id.PetasosParticipantId;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.collections.PetasosActionableTaskSet;
import net.fhirfactory.pegacorn.core.model.petasos.task.collections.PetasosTaskIdSet;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.TaskFulfillmentType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.valuesets.TaskExecutionCommandEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.datatypes.TaskOutcomeStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.TaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.valuesets.TaskStorageStatusEnum;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.core.PonosReplicatedCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.ActionableTaskPersistenceService;
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
import java.sql.Date;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class ActionableTaskCacheServices  {
    private static final Logger LOG = LoggerFactory.getLogger(ActionableTaskCacheServices.class);

    private boolean initialised;

    private static final String PONOS_TASK_PERSISTENCE_SERVICE = "Ponos-ActionableTask-Persistence-Service";

    private static final String ACTIONABLE_TASK_CACHE_NAME = "ActionableTaskSharedCache";
    private static final String ACTIONABLE_TASK_REGISTRATION_CACHE_NAME = "ActionableTaskRegistrationSharedCache";
    private static final String ACTIONABLE_TASK_PERSISTENCE_SERVICE_CACHE_NAME = "ActionableTaskPersistenceSharedCache";
    private static final String TASK_JOURNEY_REPORTING_STATUS_CACHE_NAME = "TaskJourneyReportingSharedCache";

    private Cache<DatagridElementKeyInterface, PetasosActionableTask> taskCache;
    private Object taskCacheLock;

    private Cache<DatagridElementKeyInterface, PetasosActionableTaskRegistrationType> taskRegistrationCache;
    private Cache<DataParcelTypeDescriptor, DatagridPersistenceServiceRegistrationType> taskPersistenceServiceCache;

    private Cache<DatagridElementKeyInterface, Boolean> taskJourneyReportedMap;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private ActionableTaskPersistenceService actionableTaskPersistenceService;

    @Inject
    private PonosReplicatedCacheServices replicatedCacheServices;

    //
    // Constructor(s)
    //

    public ActionableTaskCacheServices(){
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
            if(replicatedCacheServices.getCacheManager().cacheExists(ACTIONABLE_TASK_CACHE_NAME)){
                taskCache = replicatedCacheServices.getCacheManager().getCache(ACTIONABLE_TASK_CACHE_NAME);
            } else {
                taskCache = replicatedCacheServices.getCacheManager().createCache(ACTIONABLE_TASK_CACHE_NAME, replicatedCacheServices.getCacheConfigurationBuild());
            }
            if(replicatedCacheServices.getCacheManager().cacheExists(ACTIONABLE_TASK_REGISTRATION_CACHE_NAME)){
                taskRegistrationCache = replicatedCacheServices.getCacheManager().getCache(ACTIONABLE_TASK_REGISTRATION_CACHE_NAME);
            } else {
                taskRegistrationCache = replicatedCacheServices.getCacheManager().createCache(ACTIONABLE_TASK_REGISTRATION_CACHE_NAME, replicatedCacheServices.getCacheConfigurationBuild());
            }
            if(replicatedCacheServices.getCacheManager().cacheExists(ACTIONABLE_TASK_PERSISTENCE_SERVICE_CACHE_NAME)){
                taskPersistenceServiceCache = replicatedCacheServices.getCacheManager().getCache(ACTIONABLE_TASK_PERSISTENCE_SERVICE_CACHE_NAME);
            } else {
                taskPersistenceServiceCache = replicatedCacheServices.getCacheManager().createCache(ACTIONABLE_TASK_PERSISTENCE_SERVICE_CACHE_NAME, replicatedCacheServices.getCacheConfigurationBuild());
            }
            if(replicatedCacheServices.getCacheManager().cacheExists(TASK_JOURNEY_REPORTING_STATUS_CACHE_NAME)){
                taskJourneyReportedMap = replicatedCacheServices.getCacheManager().getCache(TASK_JOURNEY_REPORTING_STATUS_CACHE_NAME);
            } else {
                taskJourneyReportedMap = replicatedCacheServices.getCacheManager().createCache(TASK_JOURNEY_REPORTING_STATUS_CACHE_NAME, replicatedCacheServices.getCacheConfigurationBuild());
            }
            getLogger().info(".initialise(): [Initialising Caches] End");

            //
            // Register Myself as a Persistence Service
            getLogger().info(".initialise(): [Register As a Persistence Service] Start");
            DatagridPersistenceServiceType persistenceService = new DatagridPersistenceServiceType();
            DataParcelTypeDescriptor supportedResourceType = new DataParcelTypeDescriptor();
            supportedResourceType.setDataParcelDefiner("FHIRFactory");
            supportedResourceType.setDataParcelCategory("Petasos");
            supportedResourceType.setDataParcelSubCategory("Tasking");
            supportedResourceType.setDataParcelResource("PetasosActionableTask");
            DatagridPersistenceResourceCapabilityType persistenceResourceCapability = new DatagridPersistenceResourceCapabilityType();
            persistenceResourceCapability.setPersistenceServiceResourceScope(DatagridPersistenceServiceResourceScopeEnum.PERSISTENCE_SERVICE_RESOURCE_SCOPE_RESOURCE);
            persistenceResourceCapability.setPersistenceServiceDeploymentScope(DatagridPersistenceServiceDeploymentScopeEnum.PERSISTENCE_SERVICE_SCOPE_SITE);
            persistenceResourceCapability.setSupportedResourceDescriptor(supportedResourceType);
            persistenceService.getSupportedResourceTypes().add(persistenceResourceCapability);
            persistenceService.setActive(true);
            persistenceService.setSite(processingPlant.getDeploymentSite());
            persistenceService.setPersistenceServiceInstance(processingPlant.getTopologyNode().getComponentId());
            DatagridPersistenceServiceRegistrationType persistenceServiceRegistration = null;
            if(getTaskPersistenceServiceCache().containsKey(supportedResourceType)) {
                persistenceServiceRegistration = SerializationUtils.clone(getTaskPersistenceServiceCache().get(supportedResourceType));
                persistenceServiceRegistration.getPersistenceServices().add(persistenceService);
                getTaskPersistenceServiceCache().replace(supportedResourceType, persistenceServiceRegistration);
            } else {
                persistenceServiceRegistration = new DatagridPersistenceServiceRegistrationType();
                persistenceServiceRegistration.setResourceType(supportedResourceType);
                getTaskPersistenceServiceCache().put(supportedResourceType, persistenceServiceRegistration);
            }
            getLogger().info(".initialise(): [Register As a Persistence Service] End");
        } else {
            getLogger().debug(".initialise(): Nothing to do, already initialised");
        }
        getLogger().debug(".initialise(): Exit");
    }

    //
    // Business Methods
    //

    public PetasosActionableTask addTask(PetasosActionableTask actionableTask) {
        getLogger().debug(".addTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask == null) {
            getLogger().debug(".addTask(): Exit, actionableTask is null");
            return null;
        }

        getLogger().trace(".addTask(): [Add Task To Cache] Start");
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(actionableTask.getTaskId());
        PetasosActionableTask task = null;
        synchronized (getTaskCache()) {
            if (getTaskCache().containsKey(entryKey)) {
                PetasosActionableTask inCacheTask = getTaskCache().get(entryKey);
                inCacheTask.update(actionableTask);
                task = SerializationUtils.clone(inCacheTask);
            } else {
                getTaskCache().put(entryKey, actionableTask);
                task = SerializationUtils.clone(actionableTask);
            }
        }
        getLogger().trace(".addTask(): [Add Task To Cache] Finish");

        getLogger().trace(".addTask(): [Persist Task] Start");
        getActionableTaskPersistenceService().saveActionableTask(task);
        getLogger().trace(".addTask(): [Persist Task] Finish");

        getLogger().debug(".addTask(): Exit, task->{}", task);
        return(task);
    }

    public PetasosActionableTask updateTask(PetasosActionableTask actionableTask){
        PetasosActionableTask updatedTask = updateTask(actionableTask, true);
        return(updatedTask);
    }

    public PetasosActionableTask updateTask(PetasosActionableTask actionableTask, boolean synchronousPersist) {
        getLogger().debug(".updateTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask == null) {
            getLogger().debug(".updateTask(): Exit, actionableTask is null");
            return null;
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(actionableTask.getTaskId());
        PetasosActionableTask updatedTask = null;

        getLogger().trace(".addTask(): [Add Task To Cache] Start");
        synchronized (getTaskCache()) {
            if (getTaskCache().containsKey(entryKey)) {
                PetasosActionableTask inCacheTask = getTaskCache().get(entryKey);
                inCacheTask.update(actionableTask);
                updatedTask = SerializationUtils.clone(inCacheTask);
            } else {
                getTaskCache().put(entryKey, actionableTask);
                updatedTask = SerializationUtils.clone(actionableTask);
            }
        }
        getLogger().trace(".addTask(): [Add Task To Cache] Start");

        if(synchronousPersist){
            getLogger().trace(".addTask(): [Persist Task] Start");
            actionableTaskPersistenceService.saveActionableTask(updatedTask);
            getLogger().trace(".addTask(): [Persist Task] Finish");
        }

        getLogger().debug(".updateTask(): Exit, updatedTask->{}", updatedTask);
        return(updatedTask);
    }

    public PetasosActionableTask getTask(TaskIdType taskId) {
        getLogger().debug(".getTask(): Entry, taskId->{}", taskId);
        if(taskId == null) {
            getLogger().debug(".getTask(): Exit, taskId is null");
            return null;
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(taskId);
        PetasosActionableTask task = null;

        getLogger().trace(".addTask(): [Check Cache for Task] Start");
        synchronized (getTaskCache()) {
            if (getTaskCache().containsKey(entryKey)) {
                PetasosActionableTask inCacheTask = getTaskCache().get(entryKey);
                task = SerializationUtils.clone(inCacheTask);
            }
        }
        getLogger().trace(".addTask(): [Check Cache for Task] Finish");

        if(task == null){
            getLogger().trace(".addTask(): [Check Persistence for Task] Start");
            PetasosActionableTask persistedActionableTask = actionableTaskPersistenceService.loadActionableTask(taskId);
            getLogger().trace(".addTask(): [Check Persistence for Task] Finish");
            if(persistedActionableTask != null){
                getLogger().trace(".addTask(): [Cache the Persisted Task] Start");
                synchronized (getTaskCacheLock()) {
                    getTaskCache().put(entryKey, persistedActionableTask);
                    task = SerializationUtils.clone(persistedActionableTask);
                }
                getLogger().trace(".addTask(): [Cache the Persisted Task] Finish");
            }
        }

        getLogger().debug(".getTask(): Exit, task->{}", task);
        return(task);
    }

    public PetasosActionableTask setTaskStatus(TaskIdType taskId, TaskOutcomeStatusEnum taskOutcomeStatus, FulfillmentExecutionStatusEnum fulfillmentStatus, Instant updateInstant){
        getLogger().debug(".setTaskStatus(): Entry, taskId->{}, taskOutcomeStatus->{}, executionStatus->{}, updateInstant->{}", taskId, taskOutcomeStatus, fulfillmentStatus, updateInstant);
        if(taskId == null){
            getLogger().debug(".setTaskStatus(): Exit, taskId is null, doing nothing");
            return(null);
        }

        if(getLogger().isTraceEnabled()) {
            getLogger().trace(logAllTaskIdsInCache(".setTaskStatus"));
        }

        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(taskId);
        PetasosActionableTask updatedTask = null;
        synchronized(getTaskCacheLock()){
            PetasosActionableTask inCacheTask = getTaskCache().get(entryKey);
            if(getLogger().isTraceEnabled()) {
                getLogger().trace(".setTaskStatus(): inCacheTask->{}", inCacheTask.getTaskId());
            }
            if(updateInstant == null){
                updateInstant = Instant.now();
            }
            if(inCacheTask != null) {
                if (taskOutcomeStatus != null) {
                    if (!inCacheTask.hasTaskOutcomeStatus()) {
                        inCacheTask.setTaskOutcomeStatus(new TaskOutcomeStatusType());
                    }
                    inCacheTask.getTaskOutcomeStatus().setOutcomeStatus(taskOutcomeStatus);
                    inCacheTask.getTaskOutcomeStatus().setEntryInstant(updateInstant);
                }
                if (fulfillmentStatus != null) {
                    if (!inCacheTask.hasTaskFulfillment()) {
                        inCacheTask.setTaskFulfillment(new TaskFulfillmentType());
                        inCacheTask.getTaskFulfillment().setStatus(fulfillmentStatus);
                    }
                    switch (fulfillmentStatus) {
                        case FULFILLMENT_EXECUTION_STATUS_UNREGISTERED:
                            break;
                        case FULFILLMENT_EXECUTION_STATUS_REGISTERED:
                            inCacheTask.getTaskFulfillment().setRegistrationInstant(updateInstant);
                            break;
                        case FULFILLMENT_EXECUTION_STATUS_CANCELLED:
                            inCacheTask.getTaskFulfillment().setCancellationDate(Date.from(updateInstant));
                            break;
                        case FULFILLMENT_EXECUTION_STATUS_NO_ACTION_REQUIRED:
                            inCacheTask.getTaskFulfillment().setFinishInstant(updateInstant);
                            break;
                        case FULFILLMENT_EXECUTION_STATUS_INITIATED:
                        case FULFILLMENT_EXECUTION_STATUS_ACTIVE:
                            inCacheTask.getTaskFulfillment().setStartInstant(updateInstant);
                            break;
                        case FULFILLMENT_EXECUTION_STATUS_FINISHED:
                        case FULFILLMENT_EXECUTION_STATUS_FAILED:
                            inCacheTask.getTaskFulfillment().setFinishInstant(updateInstant);
                            break;
                        case FULFILLMENT_EXECUTION_STATUS_FINALISED:
                            inCacheTask.getTaskFulfillment().setFinalisationInstant(updateInstant);
                            break;
                    }
                }
                updatedTask = SerializationUtils.clone(inCacheTask);
            }
        }
        if (updatedTask == null) {
            getLogger().warn(".setTaskStatus(): Could not resolve ActionableTask for taskId->{}", taskId);
        }
        getLogger().debug(".setTaskStatus(): Exit, updatedTask->{}", updatedTask);
        return(updatedTask);
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
        PetasosActionableTask task = getTask(taskId);
        getActionableTaskPersistenceService().saveActionableTask(task);
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

    public Set<TaskIdType> getAllTaskIdsFromCache(){
        Set<TaskIdType> taskIdSet = new HashSet<>();
        synchronized (getTaskCacheLock()){
            for(PetasosActionableTask currentTask: getTaskCache().values()){
                taskIdSet.add(currentTask.getTaskId());
            }
        }
        return(taskIdSet);
    }

    protected String logAllTaskIdsInCache(String invocationLocation){
        Set<TaskIdType> allTaskIdsFromCache = getAllTaskIdsFromCache();
        StringBuilder taskIdLogBuilder = new StringBuilder();
        taskIdLogBuilder.append(invocationLocation);
        taskIdLogBuilder.append("->");
        for(TaskIdType currentTaskId: allTaskIdsFromCache){
            taskIdLogBuilder.append("[");
            taskIdLogBuilder.append(currentTaskId.getLocalId());
            taskIdLogBuilder.append(",");
            taskIdLogBuilder.append(currentTaskId.getTaskSequenceNumber().getCompleteSequenceNumberAsString());
            taskIdLogBuilder.append("]");
        }
        return(taskIdLogBuilder.toString());
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

    protected ActionableTaskPersistenceService getActionableTaskPersistenceService(){
        return(actionableTaskPersistenceService);
    }

    private Object getTaskCacheLock(){
        return(taskCacheLock);
    }

    private Cache<DatagridElementKeyInterface, Boolean> getTaskJourneyReportedMap(){
        return(this.taskJourneyReportedMap);
    }
}
