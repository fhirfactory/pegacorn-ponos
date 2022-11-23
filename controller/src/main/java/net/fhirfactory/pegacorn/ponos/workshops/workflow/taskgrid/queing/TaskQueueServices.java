/*
 * Copyright (c) 2022 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.queing;

import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.performer.datatypes.TaskPerformerTypeType;
import net.fhirfactory.pegacorn.core.model.petasos.task.queue.ParticipantTaskQueueEntry;
import net.fhirfactory.pegacorn.core.model.petasos.wup.valuesets.PetasosTaskExecutionStatusEnum;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.queues.CentralTaskQueueMap;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices.TaskGridClientServicesManagerAlpha;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices.TaskGridClientServicesManagerBeta;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.clientservices.common.TaskGridClientServicesManagerBase;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;

@ApplicationScoped
public class TaskQueueServices {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueueServices.class);

    private static final Long MAXIMUM_ACTIVITY_DURATION = 3000L;

    @Inject
    private CentralTaskQueueMap taskQueueMap;

    @Inject
    private TaskGridClientServicesManagerAlpha taskServicesManagerAlpha;

    @Inject
    private TaskGridClientServicesManagerBeta taskServicesManagerBeta;

    @Inject
    private ProcessingPlantInterface processingPlant;


    //
    // Constructor(s)
    //

    public TaskQueueServices(){
    }

    //
    // PostConstructor(s)
    //

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    protected TaskGridClientServicesManagerBase getTaskGridClientServicesManager(){
        if(taskServicesManagerAlpha.isBusy()){
            if(taskServicesManagerBeta.isBusy()){
                Long alphaActivityDuration = Instant.now().getEpochSecond() - taskServicesManagerAlpha.getBusyStartTime().getEpochSecond();
                if(alphaActivityDuration > MAXIMUM_ACTIVITY_DURATION){
                    return(taskServicesManagerAlpha);
                } else {
                    return(taskServicesManagerBeta);
                }
            } else {
                return(taskServicesManagerBeta);
            }
        }
        return(taskServicesManagerAlpha);
    }

    //
    // Queue Input / Queue Output
    //

    public boolean queueTask(PetasosActionableTask actionableTask){
        getLogger().warn(".queueTask(): Entry, actionableTask->{}", actionableTask);

        getLogger().trace(".queueTask(): [Checking actionableTask] Start" );
        if(actionableTask == null){
            getLogger().debug(".queueTask(): [Checking actionableTask] is null, exiting");
            return(false);
        }
        getLogger().trace(".queueTask(): [Checking actionableTask] Finish (not null)" );

        getLogger().trace(".queueTask(): [Check Task for TaskPerformer] Start");
        if(!actionableTask.hasTaskPerformerTypes()){
            getLogger().warn(".queueTask(): [Check Task for TaskPerformer] has no performerType object, exiting");
            return(false);
        }
        if(actionableTask.getTaskPerformerTypes().isEmpty()){
            getLogger().warn(".queueTask(): [Check Task for TaskPerformer] performerType list is empty, exiting");
            return(false);
        }
        getLogger().trace(".queueTask(): [Check Task for TaskPerformer] Finish, has at least 1 performer");

        getLogger().trace(".queueTask(): [Queue to ALL TaskPerformers] Start");

        boolean allGood = true;
        for(TaskPerformerTypeType currentPerformer: actionableTask.getTaskPerformerTypes()) {
            getLogger().trace(".queueTask(): [Queue to ALL TaskPerformers] processing taskPerformer->{}", currentPerformer);
            if(StringUtils.isEmpty(currentPerformer.getRequiredParticipantName())) {
                getLogger().error(".queueTask(): Cannot route task, no identified target (performer), task->{}", actionableTask);
                allGood = false;
            } else {
                ParticipantTaskQueueEntry entry = new ParticipantTaskQueueEntry();
                entry.setTaskId(actionableTask.getTaskId());
                entry.setSequenceNumber(actionableTask.getTaskId().getTaskSequenceNumber());
                taskQueueMap.addEntry(currentPerformer.getRequiredParticipantName(), entry);
                actionableTask.getTaskExecutionDetail().setCurrentExecutionStatus(PetasosTaskExecutionStatusEnum.PETASOS_TASK_ACTIVITY_STATUS_QUEUED);
                TaskIdType updatedTaskId = getTaskGridClientServicesManager().addTask(currentPerformer.getRequiredParticipantName(), actionableTask, entry);
                getLogger().trace(".queueTask(): [Queue to ALL TaskPerformers] taskPerformer->{}, queueEntry->{}", currentPerformer.getRequiredParticipantName(), entry);
            }

        }
        getLogger().trace(".queueTask(): [Queue to ALL TaskPerformers] Finish");

        getLogger().debug(".queueTask(): Exit, allGood->{}", allGood);
        return(allGood);
    }
}
