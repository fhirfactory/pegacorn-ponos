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
package net.fhirfactory.pegacorn.ponos.datagrid;

import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridElementKeyInterface;
import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridEntryLoadRequestInterface;
import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridEntrySaveRequestInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceResourceCapabilityType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceServiceRegistrationType;
import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridPersistenceServiceType;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceResourceStatusEnum;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceServiceDeploymentScopeEnum;
import net.fhirfactory.pegacorn.core.model.datagrid.valuesets.DatagridPersistenceServiceResourceScopeEnum;
import net.fhirfactory.pegacorn.core.model.dataparcel.DataParcelTypeDescriptor;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.edge.jgroups.JGroupsIntegrationPointSummary;
import net.fhirfactory.pegacorn.ponos.datagrid.cache.PonosReplicatedCacheServices;
import net.fhirfactory.pegacorn.ponos.datagrid.datatypes.PonosDatagridTaskKey;
import net.fhirfactory.pegacorn.services.tasks.cache.PetasosActionableTaskDM;
import net.fhirfactory.pegacorn.services.tasks.datatypes.PetasosActionableTaskRegistrationType;
import org.apache.commons.lang3.SerializationUtils;
import org.infinispan.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class PonosTaskCacheServices extends PetasosActionableTaskDM {
    private static final Logger LOG = LoggerFactory.getLogger(PonosTaskCacheServices.class);

    private boolean initialised;

    private static final String PONOS_TASK_PERSISTENCE_SERVICE = "Ponos-ActionableTask-Persistence-Service";

    private Cache<DatagridElementKeyInterface, PetasosActionableTask> taskCache;
    private Cache<DatagridElementKeyInterface, PetasosActionableTaskRegistrationType> taskRegistrationCache;
    private Cache<DataParcelTypeDescriptor, DatagridPersistenceServiceRegistrationType> taskPersistenceServiceCache;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private DatagridEntryLoadRequestInterface datagridEntryLoadRequestService;

    @Inject
    private DatagridEntrySaveRequestInterface datagridEntrySaveRequestService;

    @Inject
    private PonosReplicatedCacheServices replicatedCacheServices;

    //
    // Constructor(s)
    //

    public PonosTaskCacheServices(){
        super();
        this.initialised = false;
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
            taskCache = replicatedCacheServices.getCacheManager().createCache("ActionableTaskCache", replicatedCacheServices.getCacheConfigurationBuild());
            taskRegistrationCache = replicatedCacheServices.getCacheManager().createCache("ActionableTaskRegistrationCache", replicatedCacheServices.getCacheConfigurationBuild());
            taskPersistenceServiceCache = replicatedCacheServices.getCacheManager().createCache("ActionableTaskPersistenceServiceCache", replicatedCacheServices.getCacheConfigurationBuild());
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
            persistenceService.setPersistenceServiceInstance(processingPlant.getMeAsASoftwareComponent().getComponentID());
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

    @Override
    public PetasosActionableTaskRegistrationType registerPetasosActionableTask(PetasosActionableTask actionableTask, JGroupsIntegrationPointSummary integrationPoint) {
        getLogger().debug(".registerPetasosActionableTask(): Entry, actionableTask->{}, integrationPoint->{}", actionableTask,integrationPoint);
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
        actionableTaskRegistration.addFulfillmentServiceName(integrationPoint.getSubsystemParticipantName());
        actionableTaskRegistration.addFulfillmentProcessingPlant(integrationPoint.getProcessingPlantInstanceId());
        actionableTaskRegistration.addPerformerTypes(actionableTask.getTaskPerformerTypes());
        if(!taskAlreadyRegistered){
            actionableTask.setRegistered(true);
            actionableTask.getTaskFulfillment().setRegistrationInstant(Instant.now());
            getTaskCache().put(entryKey, actionableTask);
            getTaskRegistrationCache().put(entryKey, actionableTaskRegistration);
        } else {
            getTaskCache().replace(entryKey, actionableTask);
            getTaskRegistrationCache().replace(entryKey, actionableTaskRegistration);
        }
        getLogger().debug(".registerPetasosActionableTask(): Exit, actionableTaskRegistration->{}", actionableTaskRegistration);
        return(actionableTaskRegistration);
    }

    @Override
    public PetasosActionableTaskRegistrationType updatePetasosActionableTask(PetasosActionableTask actionableTask, JGroupsIntegrationPointSummary integrationPoint) {
        getLogger().debug(".updatePetasosActionableTask(): Entry, actionableTask->{}, integrationPoint->{}", actionableTask,integrationPoint);
        if(actionableTask == null) {
            getLogger().debug(".updatePetasosActionableTask(): Exit, actionableTask is null");
            return null;
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(actionableTask.getTaskId());
        PetasosActionableTaskRegistrationType actionableTaskRegistration = null;
        if(getTaskRegistrationCache().containsKey(entryKey)){
            PetasosActionableTask registeredActionableTask = getTaskCache().get(entryKey);
            actionableTaskRegistration = SerializationUtils.clone(getTaskRegistrationCache().get(entryKey));
            getTaskCache().replace(entryKey, actionableTask);
            actionableTaskRegistration.setCheckInstant(Instant.now());
            actionableTaskRegistration.addFulfillmentServiceName(integrationPoint.getSubsystemParticipantName());
            actionableTaskRegistration.addPerformerTypes(actionableTask.getTaskPerformerTypes());
            actionableTaskRegistration.addFulfillmentProcessingPlant(integrationPoint.getProcessingPlantInstanceId());
            getTaskRegistrationCache().replace(entryKey, actionableTaskRegistration);
        } else{
            actionableTaskRegistration = registerPetasosActionableTask(actionableTask, integrationPoint);
        }
        getLogger().debug(".updatePetasosActionableTask(): Exit, actionableTaskRegistration->{}", actionableTaskRegistration);
        return(actionableTaskRegistration);
    }

    @Override
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

    @Override
    public List<PetasosActionableTask> getPetasosActionableTasksForComponent(ComponentIdType componentId) {
        getLogger().debug(".getPetasosActionableTasksForComponent(): Entry, componentId->{}", componentId);
        if(componentId == null){
            getLogger().debug(".getPetasosActionableTasksForComponent(): Exit, componentId is null, returning empty list");
            return(new ArrayList<>());
        }
        List<PetasosActionableTask> activeActionableTasks = new ArrayList<>();
        for(PetasosActionableTaskRegistrationType currentTaskRegistration: getTaskRegistrationCache().values()){
            for(ComponentIdType currentComponentId: currentTaskRegistration.getFulfillmentProcessingPlants()) {
                if (currentComponentId.equals(componentId)) {
                    PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(currentTaskRegistration.getActionableTaskId());
                    PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
                    activeActionableTasks.add(actionableTask);
                }
            }
        }
        getLogger().debug(".getPetasosActionableTasksForComponent(): Exit");
        return(activeActionableTasks);
    }

    @Override
    public List<PetasosActionableTask> getWaitingActionableTasksForComponent(ComponentIdType componentId) {
        getLogger().debug(".getWaitingActionableTasksForComponent(): Entry, componentId->{}", componentId);
        if(componentId == null){
            getLogger().debug(".getWaitingActionableTasksForComponent(): Exit, componentId is null, returning empty list");
            return(new ArrayList<>());
        }
        List<PetasosActionableTask> waitingActionableTasks = new ArrayList<>();
        for(PetasosActionableTaskRegistrationType currentTaskRegistration: getTaskRegistrationCache().values()){
            for(ComponentIdType currentComponentId: currentTaskRegistration.getFulfillmentProcessingPlants()) {
                if (currentComponentId.equals(componentId)) {
                    PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(currentTaskRegistration.getActionableTaskId());
                    PetasosActionableTask actionableTask = SerializationUtils.clone(getTaskCache().get(entryKey));
                    if(actionableTask.getTaskFulfillment().getStatus().equals(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_REGISTERED)) {
                        waitingActionableTasks.add(actionableTask);
                    }
                }
            }
        }
        getLogger().debug(".getWaitingActionableTasksForComponent(): Exit");
        return(waitingActionableTasks);
    }

    @Override
    public boolean archivePetasosActionableTask(PetasosActionableTask actionableTask) {
        getTaskRegistrationCache().get(actionableTask.getTaskId());
        return false;
    }

    @Override
    public boolean archivePetasosActionableTask(TaskIdType taskId) {
        getLogger().debug(".archivePetasosActionableTask(): Entry, taskId->{}", taskId);
        if(taskId == null){
            getLogger().debug(".archivePetasosActionableTask(): Exit, taskId is null");
            return(false);
        }
        PonosDatagridTaskKey entryKey = new PonosDatagridTaskKey(taskId);
        PetasosActionableTaskRegistrationType actionableTaskRegistrationType = getTaskRegistrationCache().get(entryKey);
        actionableTaskRegistrationType.setResourceStatus(DatagridPersistenceResourceStatusEnum.RESOURCE_SAVE_REQUESTED);
        getDatagridEntrySaveRequestService().requestDatagridEntrySave(entryKey);
        return(true);
    }

    //
    // Getters and Setters (and Helpers)
    //

    @Override
    protected Logger specifyLogger() {
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

    protected DatagridEntryLoadRequestInterface getDatagridEntryLoadRequestService() {
        return datagridEntryLoadRequestService;
    }

    protected DatagridEntrySaveRequestInterface getDatagridEntrySaveRequestService() {
        return datagridEntrySaveRequestService;
    }
}
