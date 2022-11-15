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

import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.TaskCompletionSummaryType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.TaskFulfillmentType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.datatypes.TaskExecutionControl;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.valuesets.TaskExecutionCommandEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.datatypes.TaskOutcomeStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.valuesets.TaskStorageStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.queue.ParticipantTaskQueueEntry;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayloadSet;
import net.fhirfactory.pegacorn.core.model.petasos.wup.valuesets.PetasosTaskExecutionStatusEnum;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.services.TaskRouterFHIRTaskService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class TaskGridCacheServices {
    private static final Logger LOG = LoggerFactory.getLogger(TaskGridCacheServices.class);

    private Integer maxCachedTasksPerParticipant;

    private boolean initialised;

    private static final Integer PONOS_MAXIMUM_CACHED_TASKS_PER_PARTICIPANT = 250;

    private ConcurrentHashMap<String, ConcurrentHashMap<String, PetasosActionableTask>> participantTaskCache;
    private ConcurrentHashMap<String, Object> participantTaskCacheLockMap;
    private Object participantTaskCacheLock;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private TaskRouterFHIRTaskService taskPersistenceService;

    //
    // Constructor(s)
    //

    public TaskGridCacheServices(){
        super();
        this.initialised = false;
        this.participantTaskCacheLock = new Object();
        this.participantTaskCache = new ConcurrentHashMap<>();
        this.participantTaskCacheLockMap = new ConcurrentHashMap<>();
        setMaxCachedTasksPerParticipant(PONOS_MAXIMUM_CACHED_TASKS_PER_PARTICIPANT);
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise(){
        getLogger().debug(".initialise(): Entry");
        if(!initialised) {
            getLogger().info(".initialise(): Initialisation Start");

            this.initialised = true;

            getLogger().info(".initialise(): Initialisation Finish");
        } else {
            getLogger().debug(".initialise(): Nothing to do, already initialised");
        }
        getLogger().debug(".initialise(): Exit");
    }

    //
    // Basic Cache Add, Get
    //

    public TaskIdType addTask(String participantName, PetasosActionableTask task, ParticipantTaskQueueEntry queueEntry){
        getLogger().debug(".addTask(): Entry, participantName->{}, task->{}", participantName, task);
        if(StringUtils.isEmpty(participantName)){
            getLogger().warn(".addTask(): Cannot add task to cache, participantName is empty");
            return(null);
        }
        if(task == null){
            getLogger().warn(".addTask(): Cannot add task to cache, task is empty");
            return(null);
        }
        if(queueEntry == null){
            getLogger().warn(".addTask(): Cannot add task to cache, queueEntry is empty");
            return(null);
        }
        ConcurrentHashMap<String, PetasosActionableTask> taskCache = null;
        Object lockObject = null;
        synchronized (getTaskCacheLock()){
            taskCache = getParticipantTaskCache().get(participantName);
            if(taskCache == null){
                taskCache = new ConcurrentHashMap<>();
                getParticipantTaskCache().put(participantName, taskCache);
            }
            lockObject = getParticipantTaskCacheLockMap().get(participantName);
            if(lockObject == null){
                lockObject = new Object();
                getParticipantTaskCacheLockMap().put(participantName, lockObject);
            }
        }
        synchronized (lockObject){
            if(taskCache.size() < getMaxCachedTasksPerParticipant() ) {
                taskCache.put(task.getTaskId().getId(), task);
                queueEntry.setCentralCacheStatus(TaskStorageStatusEnum.TASK_SAVED);
                queueEntry.setCentralCacheLocation(processingPlant.getSubsystemName());
                queueEntry.setCentralCacheInstant(Instant.now());
            }
        }

        TaskIdType taskId = saveTaskIntoPersistence(participantName, task);
        if(taskId == null){
            getLogger().error(".addTask(): Cannot add task to cache, task could not be persisted");
            taskCache.put(task.getTaskId().getId(), task);
            return(null);
        } else {
            queueEntry.setPersistenceStatus(TaskStorageStatusEnum.TASK_SAVED);
            queueEntry.setPersistenceLocation(processingPlant.getSubsystemName());
            queueEntry.setPersistenceInstant(Instant.now());
        }

        getLogger().debug(".addTask(): Exit, taskId->{}", taskId);
        return(taskId);
    }

    public TaskIdType saveTaskIntoPersistence(String participantName, PetasosActionableTask task){
        getLogger().debug(".saveTaskIntoPersistence(): Entry, participantName->{}, task->{}", participantName, task);
        if(task == null){
            getLogger().debug(".saveTaskIntoPersistence(): Exit, cannot save task, task is null");
        }
        TaskIdType taskId = getTaskPersistenceService().saveActionableTask(task);

        getLogger().debug(".saveTaskIntoPersistence(): Exit, taskId->{}", taskId);
        return(taskId);
    }

    public TaskIdType synchroniseTaskIntoPersistence(String participantName, PetasosActionableTask task,  ParticipantTaskQueueEntry queueEntry){
        getLogger().debug(".saveTaskIntoPersistence(): Entry, participantName->{}, task->{}", participantName, task);
        if(task == null){
            getLogger().debug(".saveTaskIntoPersistence(): Exit, cannot save task, task is null");
        }
        ConcurrentHashMap<String, PetasosActionableTask> taskCache = null;
        Object lockObject = null;
        synchronized (getTaskCacheLock()){
            taskCache = getParticipantTaskCache().get(participantName);
            if(taskCache == null){
                taskCache = new ConcurrentHashMap<>();
                getParticipantTaskCache().put(participantName, taskCache);
            }
            lockObject = getParticipantTaskCacheLockMap().get(participantName);
            if(lockObject == null){
                lockObject = new Object();
                getParticipantTaskCacheLockMap().put(participantName, lockObject);
            }
        }
        synchronized (lockObject){
            taskCache.put(task.getTaskId().getId(), task);
            queueEntry.setCentralCacheStatus(TaskStorageStatusEnum.TASK_SAVED);
            queueEntry.setCentralCacheLocation(processingPlant.getSubsystemName());
            queueEntry.setCentralCacheInstant(Instant.now());
        }

        TaskIdType taskId = getTaskPersistenceService().saveActionableTask(task);
        if(taskId == null){
            getLogger().error(".addTask(): Cannot add task to cache, task could not be persisted");
        } else {
            queueEntry.setPersistenceStatus(TaskStorageStatusEnum.TASK_SAVED);
            queueEntry.setPersistenceLocation(processingPlant.getSubsystemName());
            queueEntry.setPersistenceInstant(Instant.now());
        }

        getLogger().debug(".saveTaskIntoPersistence(): Exit, taskId->{}", taskId);
        return(taskId);
    }

    //
    // Get

    public PetasosActionableTask getTaskFromCache(String participantName, TaskIdType taskId){
        getLogger().debug(".getTaskFromCache(): Entry, participantName->{}, taskId->{}", participantName, taskId);
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getTaskFromCache(): Cannot find task in cache, participantName is empty");
            return(null);
        }
        if(taskId == null){
            getLogger().debug(".getTaskFromCache(): Cannot find task in cache, taskId is empty");
            return(null);
        }

        PetasosActionableTask task = null;

        ConcurrentHashMap<String, PetasosActionableTask> taskCache = null;
        synchronized (getTaskCacheLock()){
            taskCache = getParticipantTaskCache().get(participantName);
        }
        if(taskCache != null){
            synchronized (getParticipantTaskCacheLockMap().get(participantName)){
                task = taskCache.get(taskId.getId());
            }
        }

        getLogger().debug(".getTaskFromCache(): Exit, task->{}", task);
        return(task);
    }

    public PetasosActionableTask getTaskFromPersistence(String participantName, TaskIdType taskId){
        getLogger().debug(".getTaskFromPersistence(): Entry, participantName->{}, taskId->{}", participantName, taskId);
        if(taskId == null){
            getLogger().debug(".getTaskFromPersistence(): Exit, cannot retrieve task, taskId is null");
            return(null);
        }
        PetasosActionableTask actionableTask = getTaskPersistenceService().loadActionableTask(taskId);

        getLogger().debug(".getTaskFromPersistence(): Exit, actionableTask->{}", actionableTask);
        return(actionableTask);
    }

    //
    // Delete

    public void deleteTaskFromCache(String participantName, TaskIdType taskId){
        getLogger().debug(".deleteTaskFromCache(): Entry, participantName->{}, taskId->{}", participantName, taskId);
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".deleteTaskFromCache(): Cannot find task in cache, participantName is empty");
            return;
        }
        if(taskId == null){
            getLogger().debug(".deleteTaskFromCache(): Exit, cannot delete task, taskId is null");
            return;
        }

        ConcurrentHashMap<String, PetasosActionableTask> taskCache = null;
        synchronized (getTaskCacheLock()){
            taskCache = getParticipantTaskCache().get(participantName);
        }
        if(taskCache != null){
            synchronized (getParticipantTaskCacheLockMap().get(participantName)){
                taskCache.remove(taskId.getId());
            }
        }

        getLogger().debug(".deleteTaskFromCache(): Exit");
    }

    //
    // Task Status Updates
    //

    //
    // Task Start

    public TaskExecutionCommandEnum updateTaskStatusToStarted(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail){

    }

    //
    // Task Finish

    public TaskExecutionCommandEnum updateTaskStatusToFinish(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome){

    }

    //
    // Task Cancellation

    public TaskExecutionCommandEnum updateTaskStatusToCancelled(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome){

    }

    //
    // Task Failure

    public TaskExecutionCommandEnum updateTaskStatusToFailed(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome){

    }

   //
   // Finalise

   public TaskExecutionCommandEnum updateTaskStatusToFinalised(String participantName, TaskIdType taskId, TaskCompletionSummaryType taskOutcome){
        getLogger().debug(".finaliseTask(): Entry, participantName->{}, task->{}", participantName, task);

        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".finaliseTask(): Cannot find task in cache, participantName is empty");
        }
        if(task == null){
            getLogger().debug(".finaliseTask(): Exit, cannot delete task, task is null");
        }

        task.getTaskCompletionSummary().setFinalised(true);
        getTaskPersistenceService().saveActionableTask(task);
        deleteTaskFromCache(participantName,task.getTaskId());

        getLogger().debug(".finaliseTask(): Exit");
    }

    //
    // Task Lock
    //

    public Object getTaskInstanceLock(TaskIdType taskId){
        Object lockObject = getParticipantTaskCacheLockMap().get(taskId);
        if(lockObject == null){
            getParticipantTaskCacheLockMap().put(taskId.getId(), new Object());
        }
    }

    //
    // Cache Cleanup
    //


    //
    // Getters and Setters (and Helpers)
    //

    protected Logger getLogger() {
        return (LOG);
    }

    protected ConcurrentHashMap<String, ConcurrentHashMap<String, PetasosActionableTask>> getParticipantTaskCache(){
        return(participantTaskCache);
    }

    protected ConcurrentHashMap<String, Object> getParticipantTaskCacheLockMap(){
        return(participantTaskCacheLockMap);
    }

    protected Object getTaskCacheLock(){
        return(participantTaskCacheLock);
    }

    protected TaskRouterFHIRTaskService getTaskPersistenceService(){
        return(taskPersistenceService);
    }

    public Integer getMaxCachedTasksPerParticipant() {
        return maxCachedTasksPerParticipant;
    }

    public void setMaxCachedTasksPerParticipant(Integer maxCachedTasksPerParticipant) {
        this.maxCachedTasksPerParticipant = maxCachedTasksPerParticipant;
    }
}
