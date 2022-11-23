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
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.DownstreamTaskStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.TaskCompletionSummaryType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.TaskFulfillmentType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.valuesets.TaskExecutionCommandEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.datatypes.TaskOutcomeStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.ActionableTaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.valuesets.TaskStorageStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.queue.ParticipantTaskQueueEntry;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayloadSet;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWProcessingOutcomeEnum;
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



    public boolean addTaskToParticipantTaskCache(String participantName, PetasosActionableTask task){
        Object lockObject = null;
        ConcurrentHashMap<String, PetasosActionableTask> taskCache = null;
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
            taskCache.put(task.getTaskId().getId(), task);
        }
        return(true);
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
    // General Task Interaction Helper
    //



    public Object getParticipantTaskSetLock(String participantName){
        if(StringUtils.isEmpty(participantName)){
            getLogger().error(".getParticipantTaskSetLock(): Warning, empty participantName!");
            return(null);
        }
        Object lockObject = getParticipantTaskCacheLockMap().get(participantName);
        if(lockObject == null){
            lockObject = new Object();
            getParticipantTaskCacheLockMap().put(participantName, lockObject);
        }
        return(lockObject);
    }





    //
    // Task Metrics
    //

    public Integer getParticipantTaskCacheSize(){
        int size = getParticipantTaskCache().size();
        return(size);
    }

    public Integer getFullTaskCacheSize(){
        int size = 0;
        synchronized (getTaskCacheLock()) {
            for(String currentParticipant: getParticipantTaskCache().keySet()){
                size += getParticipantTaskCache().get(currentParticipant).size();
            }
        }
        return(size);
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

    public ConcurrentHashMap<String, ConcurrentHashMap<String, PetasosActionableTask>> getParticipantTaskCache(){
        return(participantTaskCache);
    }

    public ConcurrentHashMap<String, Object> getParticipantTaskCacheLockMap(){
        return(participantTaskCacheLockMap);
    }

    public Object getTaskCacheLock(){
        return(participantTaskCacheLock);
    }

    public TaskRouterFHIRTaskService getTaskPersistenceService(){
        return(taskPersistenceService);
    }

    public Integer getMaxCachedTasksPerParticipant() {
        return maxCachedTasksPerParticipant;
    }

    public void setMaxCachedTasksPerParticipant(Integer maxCachedTasksPerParticipant) {
        this.maxCachedTasksPerParticipant = maxCachedTasksPerParticipant;
    }
}
