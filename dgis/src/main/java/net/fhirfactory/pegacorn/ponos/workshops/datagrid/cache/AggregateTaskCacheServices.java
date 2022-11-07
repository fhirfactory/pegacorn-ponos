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
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceResourceCapabilityType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceServiceRegistrationType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceServiceType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.PonosDatagridTaskKey;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceServiceDeploymentScopeEnum;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceServiceResourceScopeEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelTypeDescriptor;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosAggregateTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.core.PonosReplicatedCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.AggregateTaskPersistenceService;
import org.apache.commons.lang3.SerializationUtils;
import org.infinispan.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

@ApplicationScoped
public class AggregateTaskCacheServices {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateTaskCacheServices.class);

    private boolean initialised;

    private static final String PONOS_TASK_PERSISTENCE_SERVICE = "Ponos-AggregateTask-Persistence-Service";

    private static final String AGGREGATE_TASK_CACHE_NAME = "AggregateTaskSharedCache";
    private static final String AGGREGATE_TASK_REGISTRATION_CACHE_NAME = "AggregateTaskRegistrationSharedCache";
    private static final String AGGREGATE_TASK_PERSISTENCE_SERVICE_CACHE_NAME = "AggregateTaskPersistenceSharedCache";

    private Cache<DatagridElementKeyInterface, PetasosAggregateTask> aggregateTaskCache;
    private Object aggregateTaskCacheLock;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private AggregateTaskPersistenceService aggregateTaskPersistenceService;

    @Inject
    private PonosReplicatedCacheServices replicatedCacheServices;

    //
    // Constructor(s)
    //

    public AggregateTaskCacheServices(){
        super();
        this.initialised = false;
        this.aggregateTaskCacheLock = new Object();
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
            if(replicatedCacheServices.getCacheManager().cacheExists(AGGREGATE_TASK_CACHE_NAME)){
                aggregateTaskCache = replicatedCacheServices.getCacheManager().getCache(AGGREGATE_TASK_CACHE_NAME);
            } else {
                aggregateTaskCache = replicatedCacheServices.getCacheManager().createCache(AGGREGATE_TASK_CACHE_NAME, replicatedCacheServices.getCacheConfigurationBuild());
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
            supportedResourceType.setDataParcelResource("PetasosAggregateTask");
            DatagridPersistenceResourceCapabilityType persistenceResourceCapability = new DatagridPersistenceResourceCapabilityType();
            persistenceResourceCapability.setPersistenceServiceResourceScope(DatagridPersistenceServiceResourceScopeEnum.PERSISTENCE_SERVICE_RESOURCE_SCOPE_RESOURCE);
            persistenceResourceCapability.setPersistenceServiceDeploymentScope(DatagridPersistenceServiceDeploymentScopeEnum.PERSISTENCE_SERVICE_SCOPE_SITE);
            persistenceResourceCapability.setSupportedResourceDescriptor(supportedResourceType);
            persistenceService.getSupportedResourceTypes().add(persistenceResourceCapability);
            persistenceService.setActive(true);
            persistenceService.setSite(processingPlant.getDeploymentSite());
            persistenceService.setPersistenceServiceInstance(processingPlant.getTopologyNode().getComponentId());
            DatagridPersistenceServiceRegistrationType persistenceServiceRegistration = null;
            getLogger().info(".initialise(): [Register As a Persistence Service] End");
        } else {
            getLogger().debug(".initialise(): Nothing to do, already initialised");
        }
        getLogger().debug(".initialise(): Exit");
    }

    //
    // Business Methods
    //

    public PetasosAggregateTask addTask(PetasosAggregateTask aggregateTask) {
        getLogger().debug(".addTask(): Entry, aggregateTask->{}", aggregateTask);
        if(aggregateTask == null) {
            getLogger().debug(".addTask(): Exit, aggregateTask is null");
            return null;
        }

        getLogger().trace(".addTask(): [Add Task To Cache] Start");
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(aggregateTask.getTaskId());
        PetasosAggregateTask task = null;
        synchronized (getAggregateTaskCache()) {
            if (getAggregateTaskCache().containsKey(entryKey)) {
                PetasosAggregateTask inCacheTask = getAggregateTaskCache().get(entryKey);
                inCacheTask.update(aggregateTask);
                task = SerializationUtils.clone(inCacheTask);
            } else {
                getAggregateTaskCache().put(entryKey, aggregateTask);
                task = SerializationUtils.clone(aggregateTask);
            }
        }
        getLogger().trace(".addTask(): [Add Task To Cache] Finish");

        getLogger().trace(".addTask(): [Persist Task] Start");
        getAggregateTaskPersistenceService().saveAggregateTask(task);
        getLogger().trace(".addTask(): [Persist Task] Finish");

        getLogger().debug(".addTask(): Exit, task->{}", task);
        return(task);
    }

    public PetasosAggregateTask updateTask(PetasosAggregateTask aggregateTask){
        PetasosAggregateTask updatedTask = updateTask(aggregateTask, true);
        return(updatedTask);
    }

    public PetasosAggregateTask updateTask(PetasosAggregateTask aggregateTask, boolean synchronousPersist) {
        getLogger().debug(".updateTask(): Entry, aggregateTask->{}", aggregateTask);
        if(aggregateTask == null) {
            getLogger().debug(".updateTask(): Exit, aggregateTask is null");
            return null;
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(aggregateTask.getTaskId());
        PetasosAggregateTask updatedTask = null;

        getLogger().trace(".addTask(): [Add Task To Cache] Start");
        synchronized (getAggregateTaskCache()) {
            if (getAggregateTaskCache().containsKey(entryKey)) {
                PetasosAggregateTask inCacheTask = getAggregateTaskCache().get(entryKey);
                inCacheTask.update(aggregateTask);
                updatedTask = SerializationUtils.clone(inCacheTask);
            } else {
                getAggregateTaskCache().put(entryKey, aggregateTask);
                updatedTask = SerializationUtils.clone(aggregateTask);
            }
        }
        getLogger().trace(".addTask(): [Add Task To Cache] Start");

        if(synchronousPersist){
            getLogger().trace(".addTask(): [Persist Task] Start");
            aggregateTaskPersistenceService.saveAggregateTask(updatedTask);
            getLogger().trace(".addTask(): [Persist Task] Finish");
        }

        getLogger().debug(".updateTask(): Exit, updatedTask->{}", updatedTask);
        return(updatedTask);
    }

    public PetasosAggregateTask getTask(TaskIdType taskId) {
        getLogger().debug(".getTask(): Entry, taskId->{}", taskId);
        if(taskId == null) {
            getLogger().debug(".getTask(): Exit, taskId is null");
            return null;
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(taskId);
        PetasosAggregateTask task = null;

        getLogger().trace(".addTask(): [Check Cache for Task] Start");
        synchronized (getAggregateTaskCache()) {
            if (getAggregateTaskCache().containsKey(entryKey)) {
                PetasosAggregateTask inCacheTask = getAggregateTaskCache().get(entryKey);
                task = SerializationUtils.clone(inCacheTask);
            }
        }
        getLogger().trace(".addTask(): [Check Cache for Task] Finish");

        if(task == null){
            getLogger().trace(".addTask(): [Check Persistence for Task] Start");
            PetasosAggregateTask persistedTask = aggregateTaskPersistenceService.loadTask(taskId);
            getLogger().trace(".addTask(): [Check Persistence for Task] Finish");
            if(persistedTask != null){
                getLogger().trace(".addTask(): [Cache the Persisted Task] Start");
                synchronized (getAggregateTaskCacheLock()) {
                    getAggregateTaskCache().put(entryKey, persistedTask);
                    task = SerializationUtils.clone(persistedTask);
                }
                getLogger().trace(".addTask(): [Cache the Persisted Task] Finish");
            }
        }

        getLogger().debug(".getTask(): Exit, task->{}", task);
        return(task);
    }


    //
    // Cache Cleanup
    //



    //
    // Cache Size Information
    public int getTaskCacheSize(){
        int size = getAggregateTaskCache().size();
        return(size);
    }

    public Set<TaskIdType> getAllTaskIdsFromCache(){
        Set<TaskIdType> taskIdSet = new HashSet<>();
        synchronized (getAggregateTaskCacheLock()){
            for(PetasosAggregateTask currentTask: getAggregateTaskCache().values()){
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

    protected Cache<DatagridElementKeyInterface, PetasosAggregateTask> getAggregateTaskCache(){
        return(this.aggregateTaskCache);
    }

    private Object getAggregateTaskCacheLock(){
        return(aggregateTaskCacheLock);
    }

    private AggregateTaskPersistenceService getAggregateTaskPersistenceService(){
        return(aggregateTaskPersistenceService);
    }

}
