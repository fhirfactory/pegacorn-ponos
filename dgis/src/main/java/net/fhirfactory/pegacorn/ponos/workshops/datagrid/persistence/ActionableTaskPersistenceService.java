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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence;

import ca.uhn.fhir.rest.api.MethodOutcome;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.tasktype.valuesets.TaskTypeTypeEnum;
import net.fhirfactory.pegacorn.internals.fhir.r4.resources.task.factories.TaskIdentifierFactory;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.ActionableTaskCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm.EncounterDataManagerClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm.PatientDataManagerClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm.ProvenanceDataManagerClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm.TaskDataManagerClient;
import net.fhirfactory.pegacorn.services.tasks.transforms.fromfhir.PetasosActionableTaskFromFHIRTask;
import net.fhirfactory.pegacorn.services.tasks.transforms.tofhir.FHIRResourceSetFromPetasosActionableTask;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class ActionableTaskPersistenceService  {
    private final static Logger LOG = LoggerFactory.getLogger(ActionableTaskPersistenceService.class);
    private String persistenceKeyName;

    private boolean initialised;
    private static final String PERSISTENCE_SERVICE_NAME = "Ponos.DM";

    @Inject
    private ActionableTaskCacheServices taskCacheServices;

    @Inject
    private FHIRResourceSetFromPetasosActionableTask actionableTaskToFHIRTaskTransformer;

    @Inject
    private PetasosActionableTaskFromFHIRTask fhirToActionableTaskTransformer;

    @Inject
    private PatientDataManagerClient patientFHIRClient;

    @Inject
    private ProvenanceDataManagerClient provenanceFHIRClient;

    @Inject
    private TaskDataManagerClient taskFHIRClient;

    @Inject
    private EncounterDataManagerClient encounterFHIRClient;

    @Inject
    private ProcessingPlantInterface processingPlantInterface;

    @Inject
    private TaskIdentifierFactory taskIdentifierFactory;

    //
    // Constructor(s)
    //

    public ActionableTaskPersistenceService(){
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

    public IIdType saveActionableTask(PetasosActionableTask actionableTask){
        getLogger().debug(".savePetasosActionableTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask == null){
            getLogger().debug(".savePetasosActionableTask():Exit, actionableTask is null");
            return(null);
        }

        getLogger().trace(".savePetasosActionableTask(): [Convert ActionableTask to FHIR Resource Set] Start");
        List<Resource> resourceList = actionableTaskToFHIRTaskTransformer.transformTask(actionableTask);
        getLogger().trace(".savePetasosActionableTask(): [Convert ActionableTask to FHIR Resource Set] Finish");

        if(resourceList == null){
            getLogger().debug(".savePetasosActionableTask():Exit, resourceList is null");
            return(null);
        }
        if(resourceList.isEmpty()){
            getLogger().debug(".savePetasosActionableTask():Exit, resourceList is empty");
            return(null);
        }

        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Start");
        IIdType id = null;
        for(Resource currentResource: resourceList){
            MethodOutcome outcome = null;
            if(currentResource.getResourceType().equals(ResourceType.Task)){
                Task currentTask = (Task)currentResource;
                Resource existingResource = taskFHIRClient.findResourceByIdentifier(ResourceType.Task, currentTask.getIdentifierFirstRep());
                if(existingResource == null){
                    outcome = taskFHIRClient.createTask(currentTask);
                } else {
                    currentTask.setId(existingResource.getId());
                    outcome = taskFHIRClient.updateTask(currentTask);
                }
                if(outcome != null){
                    if(outcome.getId() != null){
                        id = outcome.getId();
                    }
                }
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Task Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Task Resource Save Failed, outcome is null!");
                    }
                }
            }
            if(currentResource.getResourceType().equals(ResourceType.Provenance)){
                Provenance currentProvenance = (Provenance)currentResource;
                currentProvenance.addTarget(new Reference(id));
                outcome = provenanceFHIRClient.createProvenance(currentProvenance);
                if(getLogger().isTraceEnabled()) {
                    if (outcome != null) {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Provenance Resource Save Done, outcome->{}", outcome);
                    } else {
                        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Provenance Resource Save Failed, outcome is null!");
                    }
                }
            }
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
        }
        getLogger().trace(".savePetasosActionableTask(): [Save FHIR Resource Set] Finish");
        return(id);
    }



    public PetasosActionableTask loadActionableTask(TaskIdType taskId){
        getLogger().debug(".loadActionableTask(): Entry, taskId->{}", taskId);

        if(taskId == null){
            getLogger().debug("loadActionableTask(): Exit, taskId is null");
            return(null);
        }

        //
        // Load the Task
        getLogger().trace(".loadActionableTask(): [Retrieve FHIR Task Resource] Start");
        Identifier taskIdentifier = getTaskIdentifierFactory().newTaskIdentifier(TaskTypeTypeEnum.PETASOS_ACTIONABLE_TASK_TYPE, taskId);

        Resource resource = taskFHIRClient.findResourceByIdentifier(ResourceType.Task, taskIdentifier);
        Task task = null;
        if(resource != null){
            if(resource.getResourceType().equals(ResourceType.Task)){
                task = (Task)resource;
            }
        }
        getLogger().trace(".loadActionableTask(): [Retrieve FHIR Task Resource] Finish, task->{}", task);

        if(task == null){
            getLogger().debug("loadActionableTask(): Exit, unable to load task!");
            return(null);
        }

        PetasosActionableTask actionableTask = getFhirToActionableTaskTransformer().transformFHIRTaskIntoPetasosActionableTask(task);

        //
        // Load the Provenance (if any)
        getLogger().trace(".loadActionableTask(): [Retrieve FHIR Provenance Resource Set] Start");
        List<Provenance> provenanceList = provenanceFHIRClient.findByReferenceToTask(taskId.getResourceId());
        getLogger().trace(".loadActionableTask(): [Retrieve FHIR Provenance Resource Set] Finish");

        PetasosActionableTask loadedActionableTask = actionableTask;
        if(provenanceList != null) {
            if(provenanceList.size() > 0) {
                Provenance firstProvenance = provenanceList.get(0);
                if (firstProvenance != null) {
                    loadedActionableTask = (PetasosActionableTask) getFhirToActionableTaskTransformer().enrichPetasosActionableTaskWithTraceabilityDetails(actionableTask, firstProvenance);
                }
            }
        }

        getLogger().debug(".loadActionableTask(): Exit, loadedActionableTask->{}", loadedActionableTask);
        return(loadedActionableTask);
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    protected ActionableTaskCacheServices getTaskCacheServices(){
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
}
