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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.queues;

import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.performer.datatypes.TaskPerformerTypeType;
import net.fhirfactory.pegacorn.petasos.core.tasks.management.queue.ParticipantTaskQueue;
import net.fhirfactory.pegacorn.petasos.core.tasks.management.queue.ParticipantTaskQueueEntry;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.core.PonosReplicatedCacheServices;
import org.apache.commons.lang3.StringUtils;
import org.infinispan.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

public class ParticipentBasedTaskQueueMap {
    private static final Logger LOG = LoggerFactory.getLogger(ParticipentBasedTaskQueueMap.class);

    private Cache<String, ParticipantTaskQueue> participantBasedQueue;
    private Object participantBasedQueueLock;
    private boolean initialised;

    @Inject
    private PonosReplicatedCacheServices replicatedCacheServices;

    @Inject
    private ProcessingPlantInterface processingPlant;

    //
    // Constructor(s)
    //

    public ParticipentBasedTaskQueueMap(){
        super();
        this.initialised = false;
        this.participantBasedQueueLock = new Object();
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
            participantBasedQueue = getReplicatedCacheServices().getCacheManager().createCache("ParticipantBasedTaskQueue", replicatedCacheServices.getCacheConfigurationBuild());
            getLogger().info(".initialise(): [Initialising Caches] End");

            getLogger().info(".initialise(): [Register As a Persistence Service] End");
        } else {
            getLogger().debug(".initialise(): Nothing to do, already initialised");
        }
        getLogger().debug(".initialise(): Exit");
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    protected PonosReplicatedCacheServices getReplicatedCacheServices(){
        return(replicatedCacheServices);
    }

    protected ProcessingPlantInterface getProcessingPlant(){
        return(processingPlant);
    }

    protected Object getParticipantQueueMapLock(){
        return(participantBasedQueueLock);
    }

    protected Cache<String, ParticipantTaskQueue> getParticipantBasedQueue(){
        return(participantBasedQueue);
    }

    //
    // Business Methods
    //

    public void queueTask(PetasosActionableTask actionableTask){
        getLogger().debug(".queueTask(): Entry, actionableTask->{}", actionableTask);
        if(actionableTask != null){
            getLogger().debug(".queueTask(): Exit, actionableTask is null");
            return;
        }
        if(!actionableTask.hasTaskPerformerTypes()){
            getLogger().debug(".queueTask(): Exit, actionableTask has no performers!");
            return;
        }
        if(actionableTask.getTaskPerformerTypes().isEmpty()){
            getLogger().debug(".queueTask(): Exit, actionableTask has no performers!");
            return;
        }
        for(TaskPerformerTypeType performerType: actionableTask.getTaskPerformerTypes()){
            if(performerType.isCapabilityBased()){
                // do nothing
            } else {
                if(performerType.getKnownTaskPerformer() != null){
                    if(StringUtils.isNotEmpty(performerType.getKnownTaskPerformer().getName())){
                        queueTask(performerType.getKnownTaskPerformer().getName(), actionableTask);
                    }
                }
            }
        }
        getLogger().debug(".queueTask(): Exit");
    }

    public void queueTask(String participantName, PetasosActionableTask actionableTask){
        getLogger().debug(".queueTask(): Entry, participantName->{}, actionableTask->{}", participantName, actionableTask);
        boolean inserted = false;
        String errorMessage = null;
        if(StringUtils.isNotEmpty(participantName)){
            synchronized (getParticipantQueueMapLock()){
                ParticipantTaskQueue taskQueue = null;
                if(getParticipantBasedQueue().containsKey(participantName)){
                    taskQueue = getParticipantBasedQueue().get(participantName);
                } else {
                    taskQueue = new ParticipantTaskQueue();
                    getParticipantBasedQueue().put(participantName, taskQueue);
                }
                if(actionableTask.hasTaskId()) {
                    ParticipantTaskQueueEntry taskQueueEntry = new ParticipantTaskQueueEntry();
                    taskQueueEntry.setTaskId(actionableTask.getTaskId());
                    taskQueueEntry.setSequenceNumber(actionableTask.getTaskId().getTaskSequenceNumber());
                    taskQueue.insertEntry(taskQueueEntry);
                    inserted = true;
                } else {
                    inserted = false;
                    errorMessage = "actionableTask does not have taskId or sequenceNumber";
                }
            }
        } else {
            inserted = false;
            errorMessage = "participantName is emtpy";
        }
        if(inserted){
            getLogger().debug(".queueTask(): Exit, task inserted!");
        } else {
            getLogger().debug(".queueTask(): Exit, task not inserted, reason = {}", errorMessage);
        }
    }
}
