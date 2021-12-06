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
package net.fhirfactory.pegacorn.ponos.dgis.workshops.datagrid.save;

import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.platform.edge.ask.EncounterFHIRClientService;
import net.fhirfactory.pegacorn.platform.edge.ask.PatientFHIRClientService;
import net.fhirfactory.pegacorn.platform.edge.ask.ProvenanceFHIRClientService;
import net.fhirfactory.pegacorn.platform.edge.ask.TaskFHIRClientService;
import net.fhirfactory.pegacorn.ponos.dgis.workshops.edge.ask.LadonDataGridHTTPClient;
import net.fhirfactory.pegacorn.services.tasks.transforms.tofhir.FHIRResourceSetFromPetasosActionableTask;
import org.hl7.fhir.r4.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class PetasosActionableTaskSaveActivity {
    private final static Logger LOG = LoggerFactory.getLogger(PetasosActionableTaskSaveActivity.class);

    private boolean initialised;

    @Inject
    private FHIRResourceSetFromPetasosActionableTask actionableTaskToFHIRTaskTransformer;

    @Inject
    private PatientFHIRClientService patientFHIRClient;

    @Inject
    private ProvenanceFHIRClientService provenanceFHIRClient;

    @Inject
    private TaskFHIRClientService taskFHIRClient;

    @Inject
    private EncounterFHIRClientService encounterFHIRClient;

    //
    // Constructor(s)
    //

    public PetasosActionableTaskSaveActivity(){
        this.initialised = false;
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise(){
        getLogger().debug(".initialise(): Entry");
        if(!initialised){

            this.initialised = true;
        }
        getLogger().debug(".initialise(): Exit");
    }

    //
    // Business Logic
    //

    public void savePetasosActionableTask(PetasosActionableTask actionableTask){
        getLogger().debug(".savePetasosActionableTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask == null){
            getLogger().debug(".savePetasosActionableTask():Exit, actionableTask is null");
            return;
        }
        List<Resource> resourceList = actionableTaskToFHIRTaskTransformer.transformTask(actionableTask);
        if(resourceList == null){
            getLogger().debug(".savePetasosActionableTask():Exit, resourceList is null");
            return;
        }
        if(resourceList.isEmpty()){
            getLogger().debug(".savePetasosActionableTask():Exit, resourceList is empty");
            return;
        }
        for(Resource currentResource: resourceList){
            if(currentResource.getResourceType().equals(ResourceType.Task)){
                Task currentTask = (Task)currentResource;
                Resource existingResource = taskFHIRClient.findResourceByIdentifier(ResourceType.Task, currentTask.getIdentifierFirstRep());
                if(existingResource == null){
                    taskFHIRClient.createTask(currentTask);
                } else {
                    currentTask.setId(existingResource.getId());
                    taskFHIRClient.updateTask(currentTask);
                }
            }
            if(currentResource.getResourceType().equals(ResourceType.Provenance)){
                Provenance currentProvenance = (Provenance)currentResource;
                provenanceFHIRClient.createProvenance(currentProvenance);
            }
            if(currentResource.getResourceType().equals(ResourceType.Encounter)){
                Encounter currentEncounter = (Encounter)currentResource;
                Resource existingResource = provenanceFHIRClient.findResourceByIdentifier(ResourceType.Encounter, currentEncounter.getIdentifierFirstRep());
                if(existingResource == null){
                    encounterFHIRClient.createEncounter(currentEncounter);
                } else {
                    currentEncounter.setId(existingResource.getId());
                    encounterFHIRClient.createEncounter(currentEncounter);
                }
            }
            if(currentResource.getResourceType().equals(ResourceType.Patient)){
                Patient currentPatient = (Patient)currentResource;
                Resource existingResource = provenanceFHIRClient.findResourceByIdentifier(ResourceType.Patient, currentPatient.getIdentifierFirstRep());
                if(existingResource == null){
                    patientFHIRClient.createPatient(currentPatient);
                } else {
                    currentPatient.setId(existingResource.getId());
                    patientFHIRClient.createPatient(currentPatient);
                }
            }
        }
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }
}
