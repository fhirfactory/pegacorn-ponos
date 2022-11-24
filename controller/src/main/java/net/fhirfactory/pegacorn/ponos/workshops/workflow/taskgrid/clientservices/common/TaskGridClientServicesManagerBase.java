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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices.common;

import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipantControlStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.DownstreamTaskStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.TaskCompletionSummaryType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.TaskFulfillmentType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.datatypes.TaskExecutionControl;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.valuesets.TaskExecutionCommandEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.datatypes.TaskOutcomeStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.ActionableTaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.valuesets.TaskStorageStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.queue.ParticipantTaskQueueEntry;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayloadSet;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWProcessingOutcomeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.wup.valuesets.PetasosTaskExecutionStatusEnum;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.ParticipantCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.TaskGridCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.services.TaskRouterFHIRTaskService;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.queues.CentralTaskQueueMap;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.queing.TaskQueueServices;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.routing.InterSubsystemTaskingServices;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TaskGridClientServicesManagerBase  {

    private boolean busy;
    private Instant busyStartTime;

    @Inject
    private CentralTaskQueueMap taskQueueMap;

    @Inject
    private TaskGridCacheServices taskCacheServices;

    @Inject
    private TaskQueueServices taskQueueServices;

    @Inject
    private ParticipantCacheServices participantCacheServices;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private TaskRouterFHIRTaskService taskPersistenceService;

    @Inject
    private InterSubsystemTaskingServices interSubsystemTaskingServices;

    //
    // Constructor(s)
    //

    public TaskGridClientServicesManagerBase(){
        this.busy = true;
        this.busyStartTime = Instant.EPOCH;
    }


    //
    // Abstract Methods
    //

    abstract protected Logger getLogger();

    //
    // Getters (and Setters)
    //


    public boolean isBusy() {
        return busy;
    }

    public void setBusy(boolean busy) {
        this.busy = busy;
    }

    public Instant getBusyStartTime() {
        return busyStartTime;
    }

    public void setBusyStartTime(Instant busyStartTime) {
        this.busyStartTime = busyStartTime;
    }

    protected TaskGridCacheServices getTaskCache() {
        return (taskCacheServices);
    }

    protected TaskQueueServices getTaskQueueServices(){
        return(taskQueueServices);
    }

    protected CentralTaskQueueMap getTaskQueueMap(){
        return(taskQueueMap);
    }

    protected ParticipantCacheServices getParticipantCacheServices(){
        return(participantCacheServices);
    }

    protected ProcessingPlantInterface getProcessingPlant(){
        return(processingPlant);
    }

    public TaskRouterFHIRTaskService getTaskPersistenceService(){
        return(taskPersistenceService);
    }

    //
    // Worker Availability
    //

    protected void amBusy(){
        setBusy(true);
        setBusyStartTime(Instant.now());
    }
    
    protected void amNotBusy(){
        setBusy(false);
    }

    //
    // Business Methods (Task Cache Support Services)
    //

    public PetasosActionableTask registerExternallyTriggeredTask(String participantName, PetasosActionableTask task){
        getLogger().debug(".registerExternallyTriggeredTask(): Entry");
        amBusy();
        getLogger().debug(".registerExternallyTriggeredTask(): Entry, participantName->{}, task->{}", participantName, task);
        if(StringUtils.isEmpty(participantName)){
            getLogger().warn(".registerExternallyTriggeredTask(): Cannot add task to cache, participantName is empty");
            amNotBusy();
            return(null);
        }
        if(task == null){
            getLogger().warn(".registerExternallyTriggeredTask(): Cannot add task to cache, task is empty");
            amNotBusy();
            return(null);
        }
        ConcurrentHashMap<String, PetasosActionableTask> taskCache = null;
        Object lockObject = null;
        synchronized (getTaskCache().getTaskCacheLock()){
            taskCache = getTaskCache().getParticipantTaskCache().get(participantName);
            if(taskCache == null){
                taskCache = new ConcurrentHashMap<>();
                getTaskCache().getParticipantTaskCache().put(participantName, taskCache);
            }
            lockObject = getTaskCache().getParticipantTaskCacheLockMap().get(participantName);
            if(lockObject == null){
                lockObject = new Object();
                getTaskCache().getParticipantTaskCacheLockMap().put(participantName, lockObject);
            }
        }
        synchronized (lockObject){
            if(taskCache.size() < getTaskCache().getMaxCachedTasksPerParticipant() ) {
                taskCache.put(task.getTaskId().getId(), task);
            }
        }

        TaskIdType taskId = null;
        synchronized(getTaskCache().getParticipantTaskSetLock(participantName)) {
            taskId = saveNewTask(participantName, task);
        }
        if(taskId == null){
            getLogger().error(".registerExternallyTriggeredTask(): Cannot add task to cache, task could not be persisted");
            taskCache.put(task.getTaskId().getId(), task);
            amNotBusy();
            return(null);
        }
        getLogger().debug(".registerExternallyTriggeredTask(): Exit, taskId->{}", taskId);
        amNotBusy();
        return(task);
    }

    public TaskIdType addTask(String participantName, PetasosActionableTask task, ParticipantTaskQueueEntry queueEntry){
        amBusy();
        getLogger().debug(".addTask(): Entry, participantName->{}, task->{}", participantName, task);
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".addTask(): Cannot add task to cache, participantName is empty");
            amNotBusy();
            return(null);
        }
        if(task == null){
            getLogger().debug(".addTask(): Cannot add task to cache, task is empty");
            amNotBusy();
            return(null);
        }
        if(queueEntry == null){
            amNotBusy();
            getLogger().debug(".addTask(): Cannot add task to cache, queueEntry is empty");
            return(null);
        }
        ConcurrentHashMap<String, PetasosActionableTask> taskCache = null;
        Object lockObject = null;
        synchronized (getTaskCache().getTaskCacheLock()){
            taskCache = getTaskCache().getParticipantTaskCache().get(participantName);
            if(taskCache == null){
                taskCache = new ConcurrentHashMap<>();
                getTaskCache().getParticipantTaskCache().put(participantName, taskCache);
            }
            lockObject = getTaskCache().getParticipantTaskCacheLockMap().get(participantName);
            if(lockObject == null){
                lockObject = new Object();
                getTaskCache().getParticipantTaskCacheLockMap().put(participantName, lockObject);
            }
        }
        synchronized (lockObject){
            if(taskCache.size() < getTaskCache().getMaxCachedTasksPerParticipant() ) {
                taskCache.put(task.getTaskId().getId(), task);
                queueEntry.setCentralCacheStatus(TaskStorageStatusEnum.TASK_SAVED);
                queueEntry.setCentralCacheLocation(getProcessingPlant().getSubsystemName());
                queueEntry.setCentralCacheInstant(Instant.now());
            }
        }

        TaskIdType taskId = null;
        synchronized(getTaskCache().getParticipantTaskSetLock(participantName)) {
            taskId = saveNewTask(participantName, task);
        }
        if(taskId == null){
            getLogger().error(".addTask(): Cannot add task to cache, task could not be persisted");
            taskCache.put(task.getTaskId().getId(), task);
            amNotBusy();
            return(null);
        } else {
            queueEntry.setPersistenceStatus(TaskStorageStatusEnum.TASK_SAVED);
            queueEntry.setPersistenceLocation(getProcessingPlant().getSubsystemName());
            queueEntry.setPersistenceInstant(Instant.now());
        }

        getLogger().debug(".addTask(): Exit, taskId->{}", taskId);
        amNotBusy();
        return(taskId);
    }

    //
    // Task Persistence
    //

    protected PetasosActionableTask loadTask(String participantName, TaskIdType taskId){
        amBusy();
        getLogger().debug(".loadTask(): Entry, participantName->{}, taskId->{}", participantName, taskId);
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".loadTask(): Exit, participantName is empty, returning CANCEL");
            amNotBusy();
            return(null);
        }
        if(taskId == null){
            getLogger().debug(".loadTask(): Exit, taskId is null, returning CANCEL");
            amNotBusy();
            return(null);
        }
        PetasosActionableTask task = null;
        PetasosActionableTask taskFromCache = getTaskCache().getTaskFromCache(participantName, taskId);
        PetasosActionableTask taskFromPersistence = null;
        if(taskFromCache == null){
            getLogger().trace(".loadTask(): Task was NOT in cache");
            taskFromPersistence = getTaskPersistenceService().loadTask(taskId);
            if(taskFromPersistence == null){
                getLogger().debug(".loadTask(): Exit, not task exists for taskId->{}", taskId);
                amNotBusy();
                return(null);
            }
            task = taskFromPersistence;
            getTaskCache().addTaskToParticipantTaskCache(participantName, task);
        } else {
            getLogger().trace(".loadTask(): Task was in cache");
            task = taskFromCache;
        }
        getLogger().debug(".loadTask(): Exit, task->{}", task);
        amNotBusy();
        return(task);
    }

    public TaskIdType updateExistingPersistedTask(String participantName, PetasosActionableTask task){
        amBusy();
        getLogger().debug(".updateExistingPersistedTask(): Entry, participantName->{}, task->{}", participantName, task);
        if(task == null){
            getLogger().debug(".updateExistingPersistedTask(): Exit, cannot save task, task is null");
            amNotBusy();
            return(null);
        }

        TaskIdType taskId = getTaskPersistenceService().updatePersistedTask(task);
        if(taskId != null){
            if(taskId.getResourceId() != null) {
                task.getTaskId().setResourceId(taskId.getResourceId());
            }
        }

        getLogger().debug(".updateExistingPersistedTask(): Exit, taskId->{}", taskId);
        amNotBusy();
        return(taskId);
    }

    public TaskIdType saveNewTask(String participantName, PetasosActionableTask task){
        amBusy();
        getLogger().debug(".saveNewTask(): Entry, participantName->{}, task->{}", participantName, task);
        if(task == null){
            getLogger().debug(".saveNewTask(): Exit, cannot save task, task is null");
            amNotBusy();
            return(null);
        }

        PetasosActionableTask returnedTask = getTaskPersistenceService().createPersistedTask(task);

        getLogger().debug(".saveNewTask(): Exit, returnedTask.getTaskId->{}", returnedTask.getTaskId());
        amNotBusy();
        return(returnedTask.getTaskId());
    }

    public TaskIdType synchroniseTaskIntoPersistence(String participantName, PetasosActionableTask task,  ParticipantTaskQueueEntry queueEntry){
        amBusy();
        getLogger().debug(".synchroniseTaskIntoPersistence(): Entry, participantName->{}, task->{}", participantName, task);
        if(task == null){
            getLogger().debug(".synchroniseTaskIntoPersistence(): Exit, cannot save task, task is null");
            amNotBusy();
            return(null);
        }
        ConcurrentHashMap<String, PetasosActionableTask> taskCache = null;
        Object lockObject = null;
        synchronized (getTaskCache().getTaskCacheLock()){
            taskCache = getTaskCache().getParticipantTaskCache().get(participantName);
            if(taskCache == null){
                taskCache = new ConcurrentHashMap<>();
                getTaskCache().getParticipantTaskCache().put(participantName, taskCache);
            }
            lockObject = getTaskCache().getParticipantTaskCacheLockMap().get(participantName);
            if(lockObject == null){
                lockObject = new Object();
                getTaskCache().getParticipantTaskCacheLockMap().put(participantName, lockObject);
            }
        }
        synchronized (lockObject){
            taskCache.put(task.getTaskId().getId(), task);
            queueEntry.setCentralCacheStatus(TaskStorageStatusEnum.TASK_SAVED);
            queueEntry.setCentralCacheLocation(processingPlant.getSubsystemName());
            queueEntry.setCentralCacheInstant(Instant.now());
        }

        TaskIdType taskId = getTaskPersistenceService().updatePersistedTask(task);
        if(taskId == null){
            getLogger().error(".synchroniseTaskIntoPersistence(): Cannot add task to cache, task could not be persisted");
        } else {
            if(taskId != null){
                if(taskId.getResourceId() != null) {
                    task.getTaskId().setResourceId(taskId.getResourceId());
                }
            }
            queueEntry.setPersistenceStatus(TaskStorageStatusEnum.TASK_SAVED);
            queueEntry.setPersistenceLocation(processingPlant.getSubsystemName());
            queueEntry.setPersistenceInstant(Instant.now());
        }

        getLogger().debug(".synchroniseTaskIntoPersistence(): Exit, taskId->{}", taskId);
        amNotBusy();
        return(taskId);
    }

    //
    // Business Methods (Client Services)
    //

    public TaskIdType queueTask(PetasosActionableTask actionableTask) {
        amBusy();
        getLogger().debug(".queueTask(): Entry, actionableTask->{}", actionableTask);
        Boolean queued = getTaskQueueServices().queueTask(actionableTask);
        actionableTask.setRegistered(queued);
        getLogger().debug(".queueTask(): Exit, actionableTask->{}", actionableTask);
        amNotBusy();
        return(actionableTask.getTaskId());
    }

    public PetasosActionableTask getNextPendingTask(String participantName) {
        amBusy();
        getLogger().debug(".getNextPendingTask(): Entry, participantName->{}", participantName);
        if(StringUtils.isEmpty(participantName)){
            getLogger().warn(".getNextPendingTask(): Exit, participantName is empty");
            amNotBusy();
            return(null);
        }
        PetasosParticipantControlStatusEnum participantStatus = getParticipantCacheServices().getParticipantStatus(participantName);
        if(participantStatus == null){
            getLogger().info(".getNextPendingTask(): Exit, participant {} is not registered, returning null", participantName);
            amNotBusy();
            return(null);
        }
        if(!participantStatus.equals(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED)){
            getLogger().debug(".getNextPendingTask(): Exit, participant {} is not ENABLED, returning null", participantName);
            amNotBusy();
            return(null);
        }
        ParticipantTaskQueueEntry participantTaskQueueEntry = getTaskQueueMap().peekNextTask(participantName);
        if(participantTaskQueueEntry == null){
            getLogger().debug(".getNextPendingTask(): Exit, participant {} is no PENDING tasks, returning null", participantName);
            amNotBusy();
            return(null);
        }
        PetasosActionableTask task = loadTask(participantName, participantTaskQueueEntry.getTaskId());
        if(task == null){
            getLogger().warn(".getNextPendingTask(): Task (taskId->{}) cannot be found in cache or persistence - but was in the queue, QueueEntry->{}", participantTaskQueueEntry.getTaskId(), participantTaskQueueEntry);
            getTaskQueueMap().pollNextTask(participantName);
            getLogger().debug(".getNextPendingTask(): Exit, cannot find corresponding task to queue entry, removed queue entry and returning null");
            amNotBusy();
            return(null);
        }
        task.getTaskExecutionDetail().setCurrentExecutionStatus(PetasosTaskExecutionStatusEnum.PETASOS_TASK_ACTIVITY_STATUS_ASSIGNED);
        getTaskQueueMap().pollNextTask(participantName);
        amNotBusy();
        getLogger().debug(".getNextPendingTask(): Exit, task->{}", task);
        return(task);
    }

    //
    // Task Notification Update Services
    //

    //
    // Task Start

    public TaskExecutionControl updateTaskStatusToStart(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail) {
        amBusy();
        getLogger().debug(".updateTaskStatusToStart(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}", participantName, taskId, taskFulfillmentDetail);

        getLogger().trace(".updateTaskStatusToStart(): [Basic Defensive Programming] Start");
        TaskExecutionControl taskExecutionControl = new TaskExecutionControl();
        taskExecutionControl.setUpdateInstant(Instant.now());
        if(StringUtils.isEmpty(participantName)){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToStart(): Exit, participantName is empty, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        if(taskId == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToStart(): Exit, taskId is null, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToStart(): [Basic Defensive Programming] Finish");

        getLogger().trace(".updateTaskStatusToStart(): [Load Task] Start");
        PetasosActionableTask task = loadTask(participantName, taskId);
        if(task == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().warn(".updateTaskStatusToStart(): [Load Task] Task (taskId->{}) does not exist in system, CANCEL", taskId);
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToStart(): [Load Task] Finish");

        getLogger().trace(".updateTaskStatusToStart(): [Update Task] Start");
        if(taskFulfillmentDetail != null){
            task.setTaskFulfillment(taskFulfillmentDetail);
        } else {
            if(!task.hasTaskFulfillment()){
                task.setTaskFulfillment( new TaskFulfillmentType());
            }
            task.getTaskFulfillment().setStatus(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_ACTIVE);
            task.getTaskFulfillment().setStartInstant(Instant.now());
            task.getTaskFulfillment().setLastCheckedInstant(Instant.now());
        }
        getLogger().trace(".updateTaskStatusToStart(): [Update Task] Finish");

        taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_EXECUTE);
        getLogger().debug(".updateTaskStatusToStart(): Exit");
        amNotBusy();
        return(taskExecutionControl);
    }

    //
    // Task Finish

    public TaskExecutionControl updateTaskStatusToFinish(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String statusReason) {
        amBusy();
        getLogger().debug(".updateTaskStatusToFinish(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome );

        TaskExecutionControl taskExecutionControl = new TaskExecutionControl();
        taskExecutionControl.setUpdateInstant(Instant.now());

        getLogger().trace(".updateTaskStatusToFinish(): [Basic Defensive Programming] Start");
        if(StringUtils.isEmpty(participantName)){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToFinish(): Exit, participantName is empty, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        if(taskId == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToFinish(): Exit, taskId is null, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        if(egressPayload == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToFinish(): Exit, egressPayload is null, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToFinish(): [Basic Defensive Programming] Finish");


        getLogger().trace(".updateTaskStatusToFinish(): [Load Task] Start");
        PetasosActionableTask task = loadTask(participantName, taskId);
        if(task == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().warn(".updateTaskStatusToFinish(): [Load Task] Task (taskId->{}) does not exist in system, CANCEL", taskId);
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToFinish(): [Load Task] Finish, task->{}", task);

        getLogger().trace(".updateTaskStatusToFinish(): [Update Task] Start");
        getLogger().trace(".updateTaskStatusToFinish(): [Update Task] [Update Task Fulfillment] Start");
        if(taskFulfillmentDetail != null){
            task.setTaskFulfillment(taskFulfillmentDetail);
        } else {
            if(!task.hasTaskFulfillment()){
                task.setTaskFulfillment( new TaskFulfillmentType());
            }
            task.getTaskFulfillment().setStatus(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_FINISHED);
            task.getTaskFulfillment().setFinishInstant(Instant.now());
            task.getTaskFulfillment().setLastCheckedInstant(Instant.now());
        }
        getLogger().trace(".updateTaskStatusToFinish(): [Update Task] [Update Task Fulfillment] Finish");
        getLogger().trace(".updateTaskStatusToFinish(): [Update Task] [Update Task Egress Payload] Start");
        if(egressPayload != null){
            task.getTaskWorkItem().setEgressContent(egressPayload);
        }
        getLogger().trace(".updateTaskStatusToFinish(): [Update Task] [Update Task Egress Payload] Finish");
        getLogger().trace(".updateTaskStatusToFinish(): [Update Task] [Update Task Outcome Status] Start");
        if(taskOutcome != null){
            task.setTaskOutcomeStatus(taskOutcome);
        } else {
            if(!task.hasTaskOutcomeStatus()){
                task.setTaskOutcomeStatus(new TaskOutcomeStatusType());
            }
            task.getTaskOutcomeStatus().setOutcomeStatus(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_FINISHED);
            task.getTaskOutcomeStatus().setEntryInstant(Instant.now());
        }
        getLogger().trace(".updateTaskStatusToFinish(): [Update Task] [Update Task Outcome Status] Finish");
        getLogger().trace(".updateTaskStatusToFinish(): [Update Task] Finish");

        getLogger().trace(".updateTaskStatusToFinish(): [Save Task] Start");
        TaskIdType savedTaskId = updateExistingPersistedTask(participantName, task);
        if(savedTaskId == null){
            getLogger().error(".updateTaskStatusToFinish(): [Save Task] Unable to save Task, task->{}", task);
        }
        getLogger().trace(".updateTaskStatusToFinish(): [Save Task] Finish");

        taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
        getLogger().debug(".updateTaskStatusToFinish(): Exit");
        amNotBusy();
        return(taskExecutionControl);
    }

    //
    // Task Cancellation

    public TaskExecutionControl updateTaskStatusToCancelled(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String statusReason) {
        amBusy();
        getLogger().debug(".updateTaskStatusToCancelled(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome );

        TaskExecutionControl taskExecutionControl = new TaskExecutionControl();
        taskExecutionControl.setUpdateInstant(Instant.now());

        getLogger().trace(".updateTaskStatusToCancelled(): [Basic Defensive Programming] Start");
        if(StringUtils.isEmpty(participantName)){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToCancelled(): Exit, participantName is empty, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        if(taskId == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToCancelled(): Exit, taskId is null, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToCancelled(): [Basic Defensive Programming] Finish");


        getLogger().trace(".updateTaskStatusToCancelled(): [Load Task] Start");
        PetasosActionableTask task = loadTask(participantName, taskId);
        if(task == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().warn(".updateTaskStatusToCancelled(): [Load Task] Task (taskId->{}) does not exist in system, CANCEL", taskId);
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToCancelled(): [Load Task] Finish");

        getLogger().trace(".updateTaskStatusToCancelled(): [Update Task] Start");
        getLogger().trace(".updateTaskStatusToCancelled(): [Update Task] [Update Task Fulfillment] Start");
        if(taskFulfillmentDetail != null){
            task.setTaskFulfillment(taskFulfillmentDetail);
        } else {
            if(!task.hasTaskFulfillment()){
                task.setTaskFulfillment( new TaskFulfillmentType());
            }
            task.getTaskFulfillment().setStatus(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_CANCELLED);
            task.getTaskFulfillment().setFinishInstant(Instant.now());
            task.getTaskFulfillment().setLastCheckedInstant(Instant.now());
        }
        getLogger().trace(".updateTaskStatusToCancelled(): [Update Task] [Update Task Fulfillment] Finish");
        getLogger().trace(".updateTaskStatusToCancelled(): [Update Task] [Update Task Egress Payload] Start");
        if(egressPayload != null){
            task.getTaskWorkItem().setEgressContent(egressPayload);
        } else {
            task.getTaskWorkItem().setProcessingOutcome(UoWProcessingOutcomeEnum.UOW_OUTCOME_CANCELLED);
        }
        if(StringUtils.isNotEmpty(statusReason)){
            task.getTaskWorkItem().setFailureDescription(statusReason);
        }
        getLogger().trace(".updateTaskStatusToCancelled(): [Update Task] [Update Task Egress Payload] Finish");
        getLogger().trace(".updateTaskStatusToCancelled(): [Update Task] [Update Task Outcome Status] Start");
        if(taskOutcome != null){
            task.setTaskOutcomeStatus(taskOutcome);
        } else {
            if(!task.hasTaskOutcomeStatus()){
                task.setTaskOutcomeStatus(new TaskOutcomeStatusType());
            }
            task.getTaskOutcomeStatus().setOutcomeStatus(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_CANCELLED);
            task.getTaskOutcomeStatus().setEntryInstant(Instant.now());
        }
        getLogger().trace(".updateTaskStatusToCancelled(): [Update Task] [Update Task Outcome Status] Finish");
        getLogger().trace(".updateTaskStatusToCancelled(): [Update Task] Finish");

        getLogger().trace(".updateTaskStatusToCancelled(): [Save Task] Start");
        TaskIdType savedTaskId = updateExistingPersistedTask(participantName, task);
        if(savedTaskId == null){
            getLogger().error(".updateTaskStatusToCancelled(): [Save Task] Unable to save Task, task->{}", task);
        }
        getLogger().trace(".updateTaskStatusToCancelled(): [Save Task] Finish");

        taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);

        getLogger().debug(".updateTaskStatusToCancelled(): Exit");
        amNotBusy();
        return(taskExecutionControl);
    }

    //
    // Task Failure

    public TaskExecutionControl updateTaskStatusToFailed(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, String failureDescription) {
        amBusy();
        getLogger().debug(".updateTaskStatusToFailed(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}, failureDescription->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome , failureDescription);

        TaskExecutionControl taskExecutionControl = new TaskExecutionControl();
        taskExecutionControl.setUpdateInstant(Instant.now());

        getLogger().trace(".updateTaskStatusToFailed(): [Basic Defensive Programming] Start");
        if(StringUtils.isEmpty(participantName)){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToFailed(): Exit, participantName is empty, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        if(taskId == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToFailed(): Exit, taskId is null, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToFailed(): [Basic Defensive Programming] Finish");


        getLogger().trace(".updateTaskStatusToFailed(): [Load Task] Start");
        PetasosActionableTask task = loadTask(participantName, taskId);
        if(task == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().warn(".updateTaskStatusToFailed(): [Load Task] Task (taskId->{}) does not exist in system, CANCEL", taskId);
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToFailed(): [Load Task] Finish");

        getLogger().trace(".updateTaskStatusToFailed(): [Update Task] Start");
        getLogger().trace(".updateTaskStatusToFailed(): [Update Task] [Update Task Fulfillment] Start");
        if(taskFulfillmentDetail != null){
            task.setTaskFulfillment(taskFulfillmentDetail);
        } else {
            if(!task.hasTaskFulfillment()){
                task.setTaskFulfillment( new TaskFulfillmentType());
            }
            task.getTaskFulfillment().setStatus(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_FAILED);
            task.getTaskFulfillment().setFinishInstant(Instant.now());
            task.getTaskFulfillment().setLastCheckedInstant(Instant.now());
        }
        getLogger().trace(".updateTaskStatusToFailed(): [Update Task] [Update Task Fulfillment] Finish");
        getLogger().trace(".updateTaskStatusToFailed(): [Update Task] [Update Task Egress Payload] Start");
        if(egressPayload != null){
            task.getTaskWorkItem().setEgressContent(egressPayload);
        } else {
            task.getTaskWorkItem().setProcessingOutcome(UoWProcessingOutcomeEnum.UOW_OUTCOME_FAILED);
        }
        if(StringUtils.isNotEmpty(failureDescription)){
            task.getTaskWorkItem().setFailureDescription(failureDescription);
        }
        getLogger().trace(".updateTaskStatusToFailed(): [Update Task] [Update Task Egress Payload] Finish");
        getLogger().trace(".updateTaskStatusToFailed(): [Update Task] [Update Task Outcome Status] Start");
        if(taskOutcome != null){
            task.setTaskOutcomeStatus(taskOutcome);
        } else {
            if(!task.hasTaskOutcomeStatus()){
                task.setTaskOutcomeStatus(new TaskOutcomeStatusType());
            }
            task.getTaskOutcomeStatus().setOutcomeStatus(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_FAILED);
            task.getTaskOutcomeStatus().setEntryInstant(Instant.now());
        }
        getLogger().trace(".updateTaskStatusToFailed(): [Update Task] [Update Task Outcome Status] Finish");
        getLogger().trace(".updateTaskStatusToFailed(): [Update Task] Finish");

        getLogger().trace(".updateTaskStatusToFailed(): [Save Task] Start");
        TaskIdType savedTaskId = updateExistingPersistedTask(participantName, task);
        if(savedTaskId == null){
            getLogger().error(".updateTaskStatusToFailed(): [Save Task] Unable to save Task, task->{}", task);
        }
        getLogger().trace(".updateTaskStatusToFailed(): [Save Task] Finish");

        taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_FAIL);
        getLogger().debug(".updateTaskStatusToFailed(): Exit");
        amNotBusy();
        return(taskExecutionControl);
    }

    //
    // Finalise

    public TaskExecutionControl updateTaskStatusToFinalised(String participantName, TaskIdType taskId, TaskCompletionSummaryType completionSummary) {
        amBusy();
        getLogger().debug(".updateTaskStatusToFinalised(): Entry, participantName->{}, taskId->{}, completionSummary->{}", participantName, taskId, completionSummary);

        TaskExecutionControl taskExecutionControl = new TaskExecutionControl();
        taskExecutionControl.setUpdateInstant(Instant.now());

        getLogger().trace(".updateTaskStatusToFinalised(): [Basic Defensive Programming] Start");
        if(StringUtils.isEmpty(participantName)){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToFinalised(): Exit, participantName is empty, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        if(taskId == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().debug(".updateTaskStatusToFinalised(): Exit, taskId is null, returning CANCEL");
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToFinalised(): [Basic Defensive Programming] Finish");

        getLogger().trace(".updateTaskStatusToFinalised(): [Load Task] Start");
        PetasosActionableTask task = loadTask(participantName, taskId);
        if(task == null){
            taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
            getLogger().warn(".updateTaskStatusToFinalised(): [Load Task] Task (taskId->{}) does not exist in system, CANCEL", taskId);
            amNotBusy();
            return(taskExecutionControl);
        }
        getLogger().trace(".updateTaskStatusToFinalised(): [Load Task] Finish");

        getLogger().trace(".updateTaskStatusToFinalised(): [Check for Inter-Subsystem Task Subscriptions] Start");
        interSubsystemTaskingServices.collectOutcomesAndCreateNewTasks(task);
        getLogger().trace(".updateTaskStatusToFinalised(): [Check for Inter-Subsystem Task Subscriptions] Finish");

        getLogger().trace(".updateTaskStatusToFinalised(): [Update Task] Start");
        getLogger().trace(".updateTaskStatusToFinalised(): [Update Task] [Update Completion Details] Start");
        if(task.getTaskCompletionSummary() == null){
            task.setTaskCompletionSummary(new TaskCompletionSummaryType());
        }
        task.getTaskCompletionSummary().setFinalised(true);
        if(completionSummary != null) {
            if (!completionSummary.getDownstreamTaskMap().isEmpty()) {
                for (TaskIdType currentDownstreamTaskId : completionSummary.getDownstreamTaskMap().keySet()) {
                    DownstreamTaskStatusType downstreamTaskStatusType = completionSummary.getDownstreamTaskMap().get(currentDownstreamTaskId);
                    task.getTaskCompletionSummary().getDownstreamTaskMap().put(currentDownstreamTaskId, downstreamTaskStatusType);
                }
            }
        }
        getLogger().trace(".updateTaskStatusToFinalised(): [Update Task] [Update Completion Details] Finish");
        getLogger().trace(".updateTaskStatusToFinalised(): [Update Task] Finish");

        getLogger().trace(".updateTaskStatusToFinalised(): [Save Task] Start");
        TaskIdType savedTaskId = updateExistingPersistedTask(participantName, task);
        if(savedTaskId == null){
            getLogger().error(".updateTaskStatusToFinalised(): [Save Task] Unable to save Task, task->{}", task);
        }
        getLogger().trace(".updateTaskStatusToFinalised(): [Save Task] Finish");

        getLogger().trace(".updateTaskStatusToFinalised(): [Flush Task From Cache] Start");
        getTaskCache().deleteTaskFromCache(participantName,taskId);
        getLogger().trace(".updateTaskStatusToFinalised(): [Flush Task From Cache] Finish");

        taskExecutionControl.setExecutionCommand(TaskExecutionCommandEnum.TASK_COMMAND_FINALISE);

        getLogger().debug(".updateTaskStatusToFinalised(): Exit");
        amNotBusy();
        return(taskExecutionControl);
    }
}
