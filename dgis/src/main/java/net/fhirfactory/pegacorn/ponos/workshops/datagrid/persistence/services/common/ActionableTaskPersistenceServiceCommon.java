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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.services.common;

import ca.uhn.fhir.rest.api.MethodOutcome;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.petasos.participant.id.PetasosParticipantId;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.tasktype.valuesets.TaskTypeTypeEnum;
import net.fhirfactory.pegacorn.internals.fhir.r4.resources.task.factories.TaskIdentifierFactory;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.TaskLoggingCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.ProvenanceDataManagerClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.TaskDataManagerClient;
import net.fhirfactory.pegacorn.services.tasks.transforms.fromfhir.PetasosActionableTaskFromFHIRTask;
import net.fhirfactory.pegacorn.services.tasks.transforms.tofhir.FHIRResourceSetFromPetasosActionableTask;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.*;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public abstract class ActionableTaskPersistenceServiceCommon {
    private String persistenceKeyName;

    private boolean initialised;
    private static final String PERSISTENCE_SERVICE_NAME = "Ponos.DM";

    @Inject
    private TaskLoggingCacheServices taskCacheServices;

    @Inject
    private FHIRResourceSetFromPetasosActionableTask actionableTaskToFHIRTaskTransformer;

    @Inject
    private PetasosActionableTaskFromFHIRTask fhirToActionableTaskTransformer;

    @Inject
    private ProcessingPlantInterface processingPlantInterface;

    @Inject
    private TaskIdentifierFactory taskIdentifierFactory;

    //
    // Constructor(s)
    //

    public ActionableTaskPersistenceServiceCommon(){
        this.initialised = false;
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise(){
        getLogger().debug(".initialise(): Entry");
        if(!initialised){
            String deploymentSite = getProcessingPlant().getDeploymentSite();
            this.persistenceKeyName = deploymentSite + "." + PERSISTENCE_SERVICE_NAME;
            this.initialised = true;
        }
        getLogger().debug(".initialise(): Exit");
    }

    //
    // Business Logic
    //

    public PetasosActionableTask createPersistedTask(PetasosActionableTask actionableTask){
        getLogger().debug(".createPersistedTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask == null){
            getLogger().debug(".createPersistedTask():Exit, actionableTask is null");
            return(null);
        }

        getLogger().trace(".createPersistedTask(): [Convert ActionableTask to FHIR Resource Set] Start");
        List<Resource> resourceList = actionableTaskToFHIRTaskTransformer.transformTask(actionableTask);
        getLogger().trace(".createPersistedTask(): [Convert ActionableTask to FHIR Resource Set] Finish");

        if(resourceList == null){
            getLogger().debug(".createPersistedTask():Exit, resourceList is null");
            return(null);
        }
        if(resourceList.isEmpty()){
            getLogger().debug(".createPersistedTask():Exit, resourceList is empty");
            return(null);
        }

        getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] Start");
        IIdType id = null;
        for(Resource currentResource: resourceList){
            getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] currentResource->{}", currentResource );
            MethodOutcome outcome = null;
            if(currentResource.getResourceType().equals(ResourceType.Task)){
                getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] processingTask" );
                Task currentTask = (Task)currentResource;
                getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Creating Task" );
                outcome = getTaskFHIRClient().createTask(currentTask);
                if(outcome != null){
                    if(outcome.getId() != null){
                        id = outcome.getId();
                        IdType resourceId = new IdType();
                        resourceId.setParts(id.getBaseUrl(), id.getResourceType(), id.getIdPart(), id.getVersionIdPart());
                        actionableTask.getTaskId().setResourceId(resourceId);
                        getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] New Task Id->{}", id );
                    }
                }
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] Task Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] Task Resource Save Failed, outcome is null!");
                    }
                }
            }
            if(currentResource.getResourceType().equals(ResourceType.Provenance)){
                getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] processingProvenance" );
                Provenance currentProvenance = (Provenance)currentResource;
                currentProvenance.addTarget(new Reference(id));
                outcome = getProvenanceFHIRClient().createProvenance(currentProvenance);
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        if(outcome.getId() != null){
                            id = outcome.getId();
                            IdType resourceId = new IdType();
                            resourceId.setParts(id.getBaseUrl(), id.getResourceType(), id.getIdPart(), id.getVersionIdPart());
                            actionableTask.getTaskTraceability().setResourceId(resourceId);
                            getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] New Task Id->{}", id );
                        }
                        getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] Provenance Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] Provenance Resource Save Failed, outcome is null!");
                    }
                }
            }
            /*
            if(currentResource.getResourceType().equals(ResourceType.Encounter)){
                Encounter currentEncounter = (Encounter)currentResource;
                Resource existingResource = provenanceFHIRClient.findResourceByIdentifier(ResourceType.Encounter, currentEncounter.getIdentifierFirstRep());
                if(existingResource == null){
                    outcome = encounterFHIRClient.createEncounter(currentEncounter);
                } else {
                    currentEncounter.setId(existingResource.getId());
                    outcome = encounterFHIRClient.createEncounter(currentEncounter);
                }
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Encounter Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Encounter Resource Save Failed, outcome is null!");
                    }
                }
            }
             */
            /*
            if(currentResource.getResourceType().equals(ResourceType.Patient)){
                Patient currentPatient = (Patient)currentResource;
                Resource existingResource = provenanceFHIRClient.findResourceByIdentifier(ResourceType.Patient, currentPatient.getIdentifierFirstRep());
                if(existingResource == null){
                    outcome = patientFHIRClient.createPatient(currentPatient);
                } else {
                    currentPatient.setId(existingResource.getId());
                    outcome = patientFHIRClient.createPatient(currentPatient);
                }
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Patient Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Patient Resource Save Failed, outcome is null!");
                    }
                }
            }
             */
        }
        getLogger().trace(".createPersistedTask(): [Save FHIR Resource Set] Finish");
        return(actionableTask);
    }

    public TaskIdType updatePersistedTask(PetasosActionableTask actionableTask){
        getLogger().debug(".updatePersistedTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask == null){
            getLogger().debug(".updatePersistedTask():Exit, actionableTask is null");
            return(null);
        }

        getLogger().trace(".updatePersistedTask(): [Convert ActionableTask to FHIR Resource Set] Start");
        List<Resource> resourceList = actionableTaskToFHIRTaskTransformer.transformTask(actionableTask);
        getLogger().trace(".updatePersistedTask(): [Convert ActionableTask to FHIR Resource Set] Finish");

        if(resourceList == null){
            getLogger().debug(".updatePersistedTask():Exit, resourceList is null");
            return(null);
        }
        if(resourceList.isEmpty()){
            getLogger().debug(".updatePersistedTask():Exit, resourceList is empty");
            return(null);
        }

        getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] Start");
        IIdType id = null;
        for(Resource currentResource: resourceList){
            getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] currentResource->{}", currentResource );
            MethodOutcome outcome = null;
            if(currentResource.getResourceType().equals(ResourceType.Task)){
                getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] processingTask" );
                Task currentTask = (Task)currentResource;
//                Resource existingResource = getTaskFHIRClient().findResourceByIdentifier(currentTask.getIdentifierFirstRep());
                getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] Updating Task" );
                outcome = getTaskFHIRClient().updateTask(currentTask);
                if(outcome != null){
                    if(outcome.getId() != null){
                        id = outcome.getId();
                        IdType resourceId = new IdType();
                        resourceId.setParts(id.getBaseUrl(), id.getResourceType(), id.getIdPart(), id.getVersionIdPart());
                        actionableTask.getTaskId().setResourceId(resourceId);
                        getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] New Task Id->{}", id );
                    }
                }
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] Task Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] Task Resource Save Failed, outcome is null!");
                    }
                }
            }
            if(currentResource.getResourceType().equals(ResourceType.Provenance)){
                getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] processingProvenance" );
                Provenance currentProvenance = (Provenance)currentResource;
                currentProvenance.addTarget(new Reference(id));
                if(actionableTask.getTaskTraceability().getResourceId() == null) {
                    outcome = getProvenanceFHIRClient().createProvenance(currentProvenance);
                } else {
                    outcome = getProvenanceFHIRClient().updateProvenance(currentProvenance);
                }
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        if(outcome.getId() != null){
                            id = outcome.getId();
                            IdType resourceId = new IdType();
                            resourceId.setParts(id.getBaseUrl(), id.getResourceType(), id.getIdPart(), id.getVersionIdPart());
                            actionableTask.getTaskTraceability().setResourceId(resourceId);
                        }
                        getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] Provenance Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] Provenance Resource Save Failed, outcome is null!");
                    }
                }
            }
            /*
            if(currentResource.getResourceType().equals(ResourceType.Encounter)){
                Encounter currentEncounter = (Encounter)currentResource;
                Resource existingResource = provenanceFHIRClient.findResourceByIdentifier(ResourceType.Encounter, currentEncounter.getIdentifierFirstRep());
                if(existingResource == null){
                    outcome = encounterFHIRClient.createEncounter(currentEncounter);
                } else {
                    currentEncounter.setId(existingResource.getId());
                    outcome = encounterFHIRClient.createEncounter(currentEncounter);
                }
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Encounter Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Encounter Resource Save Failed, outcome is null!");
                    }
                }
            }
             */
            /*
            if(currentResource.getResourceType().equals(ResourceType.Patient)){
                Patient currentPatient = (Patient)currentResource;
                Resource existingResource = provenanceFHIRClient.findResourceByIdentifier(ResourceType.Patient, currentPatient.getIdentifierFirstRep());
                if(existingResource == null){
                    outcome = patientFHIRClient.createPatient(currentPatient);
                } else {
                    currentPatient.setId(existingResource.getId());
                    outcome = patientFHIRClient.createPatient(currentPatient);
                }
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Patient Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Patient Resource Save Failed, outcome is null!");
                    }
                }
            }
             */
        }
        getLogger().trace(".updatePersistedTask(): [Save FHIR Resource Set] Finish");
        return(actionableTask.getTaskId());
    }



    public PetasosActionableTask loadTask(TaskIdType taskId){
        getLogger().debug(".loadTask(): Entry, taskId->{}", taskId);

        if(taskId == null){
            getLogger().debug("loadTask(): Exit, taskId is null");
            return(null);
        }

        //
        // Load the Task
        getLogger().trace(".loadTask(): [Retrieve FHIR Task Resource] Start");
        Identifier taskIdentifier = getTaskIdentifierFactory().newTaskIdentifier(TaskTypeTypeEnum.PETASOS_ACTIONABLE_TASK_TYPE, taskId);

        Resource resource = getTaskFHIRClient().findResourceByIdentifier(taskIdentifier);
        Task task = null;
        if(resource != null){
            if(resource.getResourceType().equals(ResourceType.Task)){
                task = (Task)resource;
            }
        }
        getLogger().trace(".loadTask(): [Retrieve FHIR Task Resource] Finish, task->{}", task);

        if(task == null){
            getLogger().debug("loadTask(): Exit, unable to load task!");
            return(null);
        }

        PetasosActionableTask actionableTask = getFhirToActionableTaskTransformer().transformFHIRTaskIntoPetasosActionableTask(task);

        //
        // Load the Provenance (if any)
        getLogger().trace(".loadTask(): [Retrieve FHIR Provenance Resource Set] Start");
        List<Provenance> provenanceList = getProvenanceFHIRClient().findByReferenceToTask(taskId.getResourceId());
        getLogger().trace(".loadTask(): [Retrieve FHIR Provenance Resource Set] Finish");

        PetasosActionableTask loadedActionableTask = actionableTask;
        if(provenanceList != null) {
            if(provenanceList.size() > 0) {
                Provenance firstProvenance = provenanceList.get(0);
                if (firstProvenance != null) {
                    loadedActionableTask = (PetasosActionableTask) getFhirToActionableTaskTransformer().enrichPetasosActionableTaskWithTraceabilityDetails(actionableTask, firstProvenance);
                }
            }
        }

        getLogger().debug(".loadTask(): Exit, loadedActionableTask->{}", loadedActionableTask);
        return(loadedActionableTask);
    }

    public Set<PetasosActionableTask> loadPendingActionableTasks(PetasosParticipantId participantId, IdType participantRID){
        getLogger().debug(".loadPendingActionableTasks(): Entry, participantId->{}", participantId);

        Set<PetasosActionableTask> taskSet = new HashSet<>();

        if(participantId == null){
            return(taskSet);
        }
        if(StringUtils.isEmpty(participantId.getName()) || StringUtils.isEmpty(participantId.getFullName())){
            return(taskSet);
        }



        return(taskSet);

    }

     //
     // Getters (and Setters)
     //

    abstract protected Logger getLogger();

    protected TaskLoggingCacheServices getTaskCacheServices(){
        return(taskCacheServices);
    }

    public String getPersistenceKeyName() {
        return persistenceKeyName;
    }

    protected ProcessingPlantInterface getProcessingPlant(){
        return(processingPlantInterface);
    }

    protected TaskIdentifierFactory getTaskIdentifierFactory(){
        return(taskIdentifierFactory);
    }

    protected PetasosActionableTaskFromFHIRTask getFhirToActionableTaskTransformer(){
        return(fhirToActionableTaskTransformer);
    }

    abstract protected ProvenanceDataManagerClient getProvenanceFHIRClient();

    abstract protected TaskDataManagerClient getTaskFHIRClient();
}
