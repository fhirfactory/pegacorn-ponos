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
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.valuesets.TaskStorageStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.queue.ParticipantTaskQueueEntry;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.services.TaskRouterFHIRTaskService;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class TaskRoutingCacheServices {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRoutingCacheServices.class);

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

    public TaskRoutingCacheServices(){
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

    public boolean addTask(String participantName, PetasosActionableTask task, ParticipantTaskQueueEntry queueEntry){
        getLogger().debug(".addTask(): Entry, participantName->{}, task->{}", participantName, task);
        if(StringUtils.isEmpty(participantName)){
            getLogger().warn(".addTask(): Cannot add task to cache, participantName is empty");
            return(false);
        }
        if(task == null){
            getLogger().warn(".addTask(): Cannot add task to cache, task is empty");
            return(false);
        }
        if(queueEntry == null){
            getLogger().warn(".addTask(): Cannot add task to cache, queueEntry is empty");
            return(false);
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

        IIdType iIdType = saveTaskIntoPersistence(participantName, task);
        if(iIdType == null){
            getLogger().error(".addTask(): Cannot add task to cache, task could not be persisted");
            taskCache.put(task.getTaskId().getId(), task);
            return(false);
        } else {
            queueEntry.setPersistenceStatus(TaskStorageStatusEnum.TASK_SAVED);
            queueEntry.setPersistenceLocation(processingPlant.getSubsystemName());
            queueEntry.setPersistenceInstant(Instant.now());
        }

        getLogger().debug(".addTask(): Exit, success->{}", true);
        return(true);
    }

    public IIdType saveTaskIntoPersistence(String participantName, PetasosActionableTask task){
        getLogger().debug(".saveTaskIntoPersistence(): Entry, participantName->{}, task->{}", participantName, task);
        if(task == null){
            getLogger().debug(".saveTaskIntoPersistence(): Exit, cannot save task, task is null");
        }
        IIdType iIdType = getTaskPersistenceService().saveActionableTask(task);

        getLogger().debug(".saveTaskIntoPersistence(): Exit, iIdType->{}", iIdType);
        return(iIdType);
    }

    public IIdType synchroniseTaskIntoPersistence(String participantName, PetasosActionableTask task,  ParticipantTaskQueueEntry queueEntry){
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

        IIdType iIdType = getTaskPersistenceService().saveActionableTask(task);
        if(iIdType == null){
            getLogger().error(".addTask(): Cannot add task to cache, task could not be persisted");
        } else {
            queueEntry.setPersistenceStatus(TaskStorageStatusEnum.TASK_SAVED);
            queueEntry.setPersistenceLocation(processingPlant.getSubsystemName());
            queueEntry.setPersistenceInstant(Instant.now());
        }

        getLogger().debug(".saveTaskIntoPersistence(): Exit, iIdType->{}", iIdType);
        return(iIdType);
    }

    //
    // Get

    public PetasosActionableTask getTask(String participantName, TaskIdType taskId, ParticipantTaskQueueEntry queueEntry){
        getLogger().debug(".getTask(): Entry, participantName->{}, taskId->{}", participantName, taskId);
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getTask(): Cannot find task in cache, participantName is empty");
            return(null);
        }
        if(taskId == null){
            getLogger().debug(".getTask(): Cannot find task in cache, taskId is empty");
            return(null);
        }
        PetasosActionableTask actionableTask = getTaskFromCache(participantName, taskId);
        if(actionableTask == null){
            actionableTask = getTaskFromPersistence(participantName, taskId);
            if(actionableTask != null){
                addTask(participantName, actionableTask, queueEntry);
            }
        }
        getLogger().debug(".getTask(): Exit, actionableTask->{}", actionableTask);
        return(actionableTask);
    }

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
    // Finalise

    public void finaliseTask(String participantName, PetasosActionableTask task){
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
