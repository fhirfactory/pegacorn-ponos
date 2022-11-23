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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.queues;

import net.fhirfactory.pegacorn.core.model.petasos.participant.queue.ParticipantTaskQueue;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.queue.ParticipantTaskQueueEntry;
import net.fhirfactory.pegacorn.deployment.topology.manager.TopologyIM;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class CentralTaskQueueMap {
    private static final Logger LOG = LoggerFactory.getLogger(CentralTaskQueueMap.class);

    private ConcurrentHashMap<String, ParticipantTaskQueue> participantQueueMap;
    private ConcurrentHashMap<String, Object> participantQueueLockMap;
    private Object participantQueueMapLock;
    private boolean initialised;

    private static final int MAXIMUM_QUEUE_SIZE = 500;

    @Inject
    private TopologyIM topologyIM;

    //
    // Constructor(s)
    //

    public CentralTaskQueueMap(){
        participantQueueMap = new ConcurrentHashMap<>();
        participantQueueLockMap  = new ConcurrentHashMap<>();
        participantQueueMapLock = new Object();

        initialised = false;
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise(){
        getLogger().debug(".initialise(): Entry");
        if(initialised){
            getLogger().debug(".initialise(): Exit, already initialised");
            return;
        }
        getLogger().info(".initialise(): Initialising...");


        setInitialised(true);
        getLogger().info(".initialise(): Initialisation complete...");
        getLogger().debug("initialise(): Exit");
    }

    //
    // Getters (and Setters)
    //


    protected ConcurrentHashMap<String, ParticipantTaskQueue> getParticipantQueueMap(){
        return(participantQueueMap);
    }

    protected ConcurrentHashMap<String, Object> getParticipantQueueLockMap(){
        return(participantQueueLockMap);
    }

    protected Object getParticipantQueueMapLock(){
        return(participantQueueMapLock);
    }

    protected boolean isInitialised(){
        return(initialised);
    }

    protected void setInitialised(boolean initialised){
        this.initialised = initialised;
    }

    protected Logger getLogger(){
        return(LOG);
    }

    protected TopologyIM getTopologyIM(){
        return(topologyIM);
    }

    //
    // Participant Processing Tasks
    //


    //
    // Participant Task Queueing
    //

    public boolean addEntry(String participantName, ParticipantTaskQueueEntry queueEntry){
        getLogger().debug(".addEntry(): Entry, participantName->{}, queueEntry->{}", participantName, queueEntry);
        boolean inserted = false;
        String errorMessage = null;
        if(StringUtils.isNotEmpty(participantName)){
            ParticipantTaskQueue taskQueue = null;
            synchronized (getParticipantQueueMapLock()) {
                if (getParticipantQueueMap().containsKey(participantName)) {
                    taskQueue = getParticipantQueueMap().get(participantName);
                } else {
                    taskQueue = new ParticipantTaskQueue();
                    getParticipantQueueMap().put(participantName, taskQueue);
                    getParticipantQueueLockMap().put(participantName, new Object());
                }
            }
            synchronized (getParticipantQueueLockMap().get(participantName)){
                if(queueEntry.getTaskId() != null) {
                    taskQueue.insertEntry(queueEntry);
                    inserted = true;
                } else {
                    inserted = false;
                    errorMessage = "actionableTask does not have taskId or sequenceNumber";
                }
            }
        } else {
            inserted = false;
            errorMessage = "participantName is empty";
        }
        if(inserted){
            getLogger().debug(".queueTask(): Exit, task inserted!");
        } else {
            getLogger().warn(".queueTask(): Exit, task not inserted, reason = {}", errorMessage);
        }
        return(inserted);
    }

    public ParticipantTaskQueueEntry removeEntry(String participantName, ParticipantTaskQueueEntry queueEntry){
        getLogger().debug(".removeEntry(): Entry, participantName->{}, queueEntry->{}", participantName, queueEntry);
        boolean removed = false;
        String errorMessage = null;
        ParticipantTaskQueueEntry taskQueueEntry = null;
        if(StringUtils.isNotEmpty(participantName)){
            ParticipantTaskQueue taskQueue = null;
            synchronized (getParticipantQueueMapLock()) {
                if (getParticipantQueueMap().containsKey(participantName)) {
                    taskQueue = getParticipantQueueMap().get(participantName);
                }
            }
            if(taskQueue != null){
                synchronized (getParticipantQueueLockMap().get(participantName)) {
                    taskQueueEntry = taskQueue.removeEntry(queueEntry.getTaskId());
                    removed = true;
                }
            } else {
                errorMessage = "queue entry does not exist";
            }
        } else {
            errorMessage = "participantName is empty";
        }
        if(removed){
            getLogger().debug(".removeEntry(): Exit, task removed!");
        } else {
            getLogger().warn(".removeEntry(): Exit, task not removed, reason = {}", errorMessage);
        }
        return(taskQueueEntry);
    }

    public boolean isFull(String participantName){
        if(StringUtils.isEmpty(participantName)){
            return(false);
        }
        int size = getParticipantQueueSize(participantName);
        if(size > MAXIMUM_QUEUE_SIZE){
            return(true);
        } else {
            return (false);
        }
    }

    public int getParticipantQueueSize(String participantName){
        if(StringUtils.isEmpty(participantName)){
            return(0);
        }
        int size = 0;
        if(getParticipantQueueMap().containsKey(participantName)){
            synchronized (getParticipantQueueLockMap().get(participantName)) {
                size = getParticipantQueueMap().get(participantName).getSize();
            }
        }
        return(size);
    }

    public ParticipantTaskQueueEntry peekNextTask(String participantName){
        getLogger().debug(".peekNextTask(): Entry, participantName->{}", participantName);
        ParticipantTaskQueueEntry taskQueueEntry = null;
        if(StringUtils.isNotEmpty(participantName)){
            getLogger().debug(".peekNextTask(): participantName is not empty");
            if(getParticipantQueueMap().containsKey(participantName)) {
                getLogger().debug(".peekNextTask(): participantName has an associated queue");
                synchronized (getParticipantQueueLockMap().get(participantName)) {
                    if (getParticipantQueueMap().get(participantName).hasEntries()) {
                        getLogger().debug(".peekNextTask(): participantName Queue has entries");
                        taskQueueEntry = getParticipantQueueMap().get(participantName).peek();
                    }
                }
            }
        }
        getLogger().debug(".peekNextTask(): Exit, taskQueueEntry->{}", taskQueueEntry);
        return(taskQueueEntry);
    }

    public ParticipantTaskQueueEntry pollNextTask(String participantName){
        getLogger().debug(".pollNextTask(): Entry, participantName->{}", participantName);
        ParticipantTaskQueueEntry taskQueueEntry = null;
        if(StringUtils.isNotEmpty(participantName)){
            if (getParticipantQueueMap().containsKey(participantName)) {
                synchronized (getParticipantQueueLockMap().get(participantName)) {
                    if (getParticipantQueueMap().get(participantName).hasEntries()) {
                        taskQueueEntry = getParticipantQueueMap().get(participantName).poll();
                    }
                }
            }
        }
        getLogger().debug(".pollNextTask(): Exit, taskQueueEntry->{}", taskQueueEntry);
        return(taskQueueEntry);
    }

    public ParticipantTaskQueueEntry getTaskQueueEntry(String participantName, TaskIdType taskId){
        getLogger().debug(".getTaskQueueEntry(): Entry, participantName->{}", participantName);
        ParticipantTaskQueueEntry taskQueueEntry = null;
        if(StringUtils.isNotEmpty(participantName)){
            if (getParticipantQueueMap().containsKey(participantName)) {
                synchronized (getParticipantQueueLockMap().get(participantName)) {
                    if (getParticipantQueueMap().get(participantName).hasEntries()) {
                        taskQueueEntry = getParticipantQueueMap().get(participantName).findEntry(taskId);
                    }
                }
            }
        }
        getLogger().debug(".getTaskQueueEntry(): Exit, taskQueueEntry->{}", taskQueueEntry);
        return(taskQueueEntry);
    }

    public Set<ParticipantTaskQueueEntry> getLastNTasks(String participantName, Integer queueOnloadThreshold, Integer nSize){
        getLogger().debug(".getLastNTasks(): Entry, participantName->{}, nSize->{}", participantName, nSize);

        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getLastNTasks(): Exit, participantName is empty");
            return(new HashSet<>());
        }
        if(nSize == null || nSize < 1){
            getLogger().debug(".getLastNTasks(): Exit, nSize is less than 1, nothing to get");
            return(new HashSet<>());
        }

        Set<ParticipantTaskQueueEntry> lastNTasks = new HashSet<>();
        if (getParticipantQueueMap().containsKey(participantName)) {
            synchronized (getParticipantQueueLockMap().get(participantName)) {
                ParticipantTaskQueue participantTaskQueue = getParticipantQueueMap().get(participantName);
                Integer queueSize = getParticipantQueueSize(participantName);
                if (queueSize > queueOnloadThreshold) {
                    Integer size = nSize;
                    Integer offloadable = queueSize - queueOnloadThreshold;
                    if (offloadable < nSize) {
                        size = offloadable;
                    }
                    lastNTasks = participantTaskQueue.getLastNTasks(queueOnloadThreshold, nSize);
                }
            }
        }
        getLogger().debug(".getLastNTasks(): Exit, lastNTasks->{}", lastNTasks);
        return(lastNTasks);
    }

    public Set<String> getParticipants(){
        getLogger().debug(".getParticipants(): Entry");

        Set<String> participantSet = new HashSet<>();
        synchronized (getParticipantQueueMapLock()) {
            Set<String> participantSetInCache = new HashSet<>(getParticipantQueueMap().keySet());
            for (String currentParticipant : participantSetInCache) {
                participantSet.add(currentParticipant);
            }
        }

        getLogger().debug(".getParticipants(): Exit, participantSet->{}", participantSet);
        return(participantSet);
    }

}
