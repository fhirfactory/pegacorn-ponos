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
package net.fhirfactory.pegacorn.ponos.dgis.workshops.datagrid.load;

import net.fhirfactory.pegacorn.core.model.datagrid.datatypes.DatagridElementSourceResourceIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.platform.edge.ask.ProvenanceFHIRClientService;
import net.fhirfactory.pegacorn.platform.edge.ask.TaskFHIRClientService;
import org.hl7.fhir.r4.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class PetasosActionableTaskLoadActivity {
    private static final Logger LOG = LoggerFactory.getLogger(PetasosActionableTaskLoadActivity.class);

    @Inject
    private TaskFHIRClientService taskFHIRClient;

    @Inject
    private ProvenanceFHIRClientService provenanceFHIRClient;

    //
    // Business Methods
    //


    public PetasosActionableTask loadActionableTask(DatagridElementSourceResourceIdType resourceId){

        return(null);
    }


    public PetasosActionableTask loadActionableTask(TaskIdType taskId){

        //
        // Load the Task
        Task task = loadTask(taskId);

        if(task == null){
            return(null);
        }

        //
        // Load the Provenance (if any)
        List<Provenance> provenanceList = provenanceFHIRClient.findByReferenceToTask(taskId.getPrimaryBusinessIdentifier());




        return(null);
    }

    public PetasosActionableTask loadActionableTask(Identifier identifier){

        return(null);
    }

    //
    // Helpers
    //

    protected Task loadTask(TaskIdType taskId){
        Task task = null;
        if(taskId.getPrimaryBusinessIdentifier() != null){
            Resource resourceByIdentifier = taskFHIRClient.findResourceByIdentifier(ResourceType.Task.name(), taskId.getPrimaryBusinessIdentifier());
            if(resourceByIdentifier != null){
                task = (Task)resourceByIdentifier;
                return(task);
            }
        }

        return(null);
    }
}
