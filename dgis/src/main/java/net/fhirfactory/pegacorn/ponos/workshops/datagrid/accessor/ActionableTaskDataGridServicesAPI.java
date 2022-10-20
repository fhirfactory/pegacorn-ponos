/*
 * Copyright (c) 2022 Mark A. Hunter
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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.accessor;

import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.participant.id.PetasosParticipantId;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.collections.PetasosActionableTaskSet;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.services.tasks.datatypes.PetasosActionableTaskRegistrationType;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class ActionableTaskDataGridServicesAPI {
    private static final Logger LOG = LoggerFactory.getLogger(ActionableTaskDataGridServicesAPI.class);

    //
    // Constructor(s)
    //

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    //
    // Business Methods
    //


    public PetasosActionableTaskRegistrationType registerActionableTask(PetasosActionableTask actionableTask, PetasosActionableTaskRegistrationType localRegistration){
        PetasosActionableTaskRegistrationType centralRegistration = SerializationUtils.clone(localRegistration);
        return(centralRegistration);
    }

    public PetasosActionableTaskRegistrationType updatePetasosActionableTask(PetasosActionableTask actionableTask, PetasosActionableTaskRegistrationType localRegistration){
        PetasosActionableTaskRegistrationType centralRegistration = SerializationUtils.clone(localRegistration);
        return(centralRegistration);
    }

    public PetasosActionableTask getPetasosActionableTask(TaskIdType taskId){
        PetasosActionableTask actionableTask = null;
        return(actionableTask);
    }

    public PetasosActionableTaskRegistrationType cancelTask(TaskIdType taskId, PetasosActionableTaskRegistrationType localRegistration){

        PetasosActionableTaskRegistrationType centralRegistration = SerializationUtils.clone(localRegistration);
        return(centralRegistration);
    }

    public PetasosActionableTaskRegistrationType redoTask(TaskIdType taskId, PetasosActionableTaskRegistrationType localRegistration){
        PetasosActionableTaskRegistrationType centralRegistration = SerializationUtils.clone(localRegistration);
        return(centralRegistration);
    }

    public List<PetasosActionableTask> getPetasosActionableTasksForComponent(ComponentIdType componentId){
        return(new ArrayList<>());
    }

    public Boolean hasOffloadedPendingActionableTasks(PetasosParticipantId participantId){
        return(false);
    }

    public PetasosActionableTaskSet getAllPendingActionableTasks(PetasosParticipantId participantId){
        return(new PetasosActionableTaskSet());
    }

}
