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

import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.componentid.SoftwareComponentTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipant;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipantControlStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.participant.registration.PetasosParticipantRegistrationStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.work.datatypes.TaskWorkItemManifestType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.work.datatypes.TaskWorkItemSubscriptionType;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.core.PonosReplicatedCacheServices;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.infinispan.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

@ApplicationScoped
public class ParticipantCacheServices {
    private static final Logger LOG = LoggerFactory.getLogger(ParticipantCacheServices.class);

    private boolean initialised;

    // Cache<participantName, participantRegistration>
    private Cache<String, PetasosParticipant> participantRegistrationCache;
    private Object participantRegistrationCacheLock;

    // Cache<producerParticipantName, Set<subscribeParticipantName>>
    private Cache<String, Set<String>> interPodSubscriptionMap;
    private Object interPodSubscriptionMapLock;


    @Inject
    private PonosReplicatedCacheServices replicatedCacheServices;

    //
    // Constructor(s)
    //

    public ParticipantCacheServices(){
        this.initialised = false;
        this.participantRegistrationCache = null;
        this.participantRegistrationCacheLock = new Object();
        this.interPodSubscriptionMap = null;
        this.interPodSubscriptionMapLock = new Object();
    }

    @PostConstruct
    public void initialise() {
        getLogger().debug(".initialise(): Entry");
        if (!initialised) {
            getLogger().info(".initialise(): Initialisation Start");

            getLogger().info(".initialise(): [Initialising Caches] Start");
            participantRegistrationCache = replicatedCacheServices.getCacheManager().createCache("ParticipantRegistrationCache", replicatedCacheServices.getCacheConfigurationBuild());
            interPodSubscriptionMap = replicatedCacheServices.getCacheManager().createCache("InterPodSubscriptionMap", replicatedCacheServices.getCacheConfigurationBuild());
            getLogger().info(".initialise(): [Initialising Caches] End");

            this.initialised = true;
        }
        getLogger().debug(".initialised(): Exit");
    }

    //
    // Participant Cache
    //

    /**
     *
     * @param localParticipantRegistration
     * @return
     */
    public PetasosParticipant registerPetasosParticipant(PetasosParticipant localParticipantRegistration){
        getLogger().debug(".registerPetasosParticipant(): Entry, localParticipantRegistration->{}", localParticipantRegistration);
        PetasosParticipant registration = null;
        getLogger().trace(".registerPetasosParticipant(): First, we check the content of the passed-in parameter");
        if(localParticipantRegistration == null){
            getLogger().debug("registerPetasosParticipant(): Exit, localParticipantRegistration is null, returning -null-");
            return(null);
        }
        getLogger().trace(".registerTaskProducer(): Publisher is not in Map, so add it!");
        registration = SerializationUtils.clone(localParticipantRegistration);
        registration.setRegistrationCommentary("Publisher Registered");
        registration.setCentralRegistrationInstant(Instant.now());
        registration.setCentralRegistrationStatus(PetasosParticipantRegistrationStatusEnum.PARTICIPANT_REGISTERED);
        synchronized (this.participantRegistrationCacheLock) {
            getParticipantRegistrationCache().put(registration.getParticipantId().getName(), registration);
        }
        if(localParticipantRegistration.hasComponentType()) {
            if (localParticipantRegistration.getComponentType().equals(SoftwareComponentTypeEnum.PROCESSING_PLANT)) {
                for (TaskWorkItemSubscriptionType currentTaskWorkItem : localParticipantRegistration.getSubscriptions()) {
                    addDownstreamSubscriberParticipant(currentTaskWorkItem.getSourceProcessingPlantParticipantName(), registration);
                }
            }
        }
        registration.setControlStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED);
        registration.setUpdateInstant(Instant.now());
        getLogger().debug(".registerTaskProducer(): Exit, registration->{}", registration);
        return (registration);
    }

    /**
     *
     * @param participant
     * @return
     */
    public PetasosParticipant deregisterPetasosParticipant(PetasosParticipant participant){
        getLogger().debug(".deregisterPetasosParticipant(): Entry, participant->{}", participant);
        if(participant == null){
            getLogger().debug("deregisterPetasosParticipant(): Exit, participant is null");
            return(null);
        }
        PetasosParticipant registration = deregisterPetasosParticipant(participant.getLocalComponentId());
        getLogger().debug(".deregisterPetasosParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    /**
     *
     * @param componentId
     * @return
     */
    public PetasosParticipant deregisterPetasosParticipant(ComponentIdType componentId) {
        getLogger().debug(".deregisterPetasosParticipant(): Entry, componentId->{}", componentId);
        PetasosParticipant registration = null;
        if (componentId == null) {
            getLogger().debug("deregisterPetasosParticipant(): Exit, participant is null");
            return (null);
        }
        getLogger().debug(".deregisterPetasosParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    /**
     *
     * @param localParticipantRegistration
     * @return
     */
    public PetasosParticipant updateParticipantRegistration(PetasosParticipant localParticipantRegistration){
        getLogger().debug(".updateParticipantRegistration(): Entry, localParticipantRegistration->{}", localParticipantRegistration);
        if(localParticipantRegistration == null){
            getLogger().debug(".updateParticipantRegistration(): Exit, localParticipantRegistration is null, returning null");
            return(null);
        }
        boolean hasBeenUpdated = false;
        StringBuilder updateReasonBuilder = new StringBuilder();
        PetasosParticipant centralRegistration = getParticipant(localParticipantRegistration.getParticipantId().getName());
        if(centralRegistration == null){
            centralRegistration = registerPetasosParticipant(localParticipantRegistration);
            updateReasonBuilder.append("new registration:");
            hasBeenUpdated = true;
        } else {
            if(!centralRegistration.getLocalRegistrationStatus().equals(localParticipantRegistration.getLocalRegistrationStatus())) {
                centralRegistration.setLocalRegistrationStatus(localParticipantRegistration.getLocalRegistrationStatus());
                updateReasonBuilder.append("localRegistrationStatus change:");
                hasBeenUpdated = true;
            }
            if(!centralRegistration.getLocalRegistrationInstant().equals(localParticipantRegistration.getLocalRegistrationInstant())) {
                centralRegistration.setLocalRegistrationInstant(localParticipantRegistration.getLocalRegistrationInstant());
                // updateReasonBuilder.append("localRegistrationInstant change:");
                // hasBeenUpdated = true;
            }
            if(!centralRegistration.getCentralRegistrationStatus().equals(PetasosParticipantRegistrationStatusEnum.PARTICIPANT_REGISTERED)){
                centralRegistration.setCentralRegistrationStatus(PetasosParticipantRegistrationStatusEnum.PARTICIPANT_REGISTERED);
                centralRegistration.setCentralRegistrationInstant(Instant.now());
                updateReasonBuilder.append("centralRegistrationInstant change:");
                hasBeenUpdated = true;
            }
            if(!localParticipantRegistration.getSubscriptions().isEmpty()){
                for(TaskWorkItemSubscriptionType currentSubscription: localParticipantRegistration.getSubscriptions()){
                    if(!centralRegistration.getSubscriptions().contains(currentSubscription)){
                        centralRegistration.getSubscriptions().add(currentSubscription);
                        hasBeenUpdated = true;
                        updateReasonBuilder.append("subscriptions change:");
                    }
                }
            }
            if(!localParticipantRegistration.getOutputs().isEmpty()){
                for(TaskWorkItemManifestType currentOutput: localParticipantRegistration.getOutputs()){
                    if(!centralRegistration.getOutputs().contains(currentOutput)){
                        centralRegistration.getOutputs().add(currentOutput);
                        hasBeenUpdated = true;
                        updateReasonBuilder.append("outputs change:");
                    }
                }
            }
            if(!centralRegistration.getParticipantStatus().equals(localParticipantRegistration.getParticipantStatus())) {
                centralRegistration.setParticipantStatus(localParticipantRegistration.getParticipantStatus());
                hasBeenUpdated = true;
                updateReasonBuilder.append("participantStatus change:");
            }
            if (!centralRegistration.getInstanceComponentIds().contains(localParticipantRegistration.getLocalComponentId())) {
                centralRegistration.getInstanceComponentIds().add(localParticipantRegistration.getLocalComponentId());
                hasBeenUpdated = true;
                updateReasonBuilder.append("instanceComponentIds change:");
            }
            centralRegistration.setLocalComponentStatus(localParticipantRegistration.getLocalComponentStatus());
        }
        if(hasBeenUpdated){
            centralRegistration.setUpdateInstant(Instant.now());
            getLogger().warn(".updateParticipantRegistration(): registration has changed for {}, reason->{}", localParticipantRegistration.getParticipantId().getName(), updateReasonBuilder.toString());
        }
        getLogger().debug(".updateParticipantRegistration(): Exit, centralRegistration->{}", centralRegistration);
        return(centralRegistration);
    }

    /**
     *
     * @param participantName
     * @return
     */
    public PetasosParticipant getParticipant(String participantName){
        getLogger().debug(".getParticipant(): Entry, participantName->{}", participantName);

        PetasosParticipant registration = null;

        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getParticipant(): Exit, participantName is empty");
            return(null);
        }

        synchronized (getParticipantRegistrationCacheLock()) {
            registration = getParticipantRegistrationCache().get(participantName);
        }

        getLogger().debug(".getParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    //
    // "Subscription" Map (ProducerTaskConsumerMap)
    //

    /**
     *
     * @param producerParticipantName
     * @param downstreamParticipant
     */
    public void addDownstreamSubscriberParticipant(String producerParticipantName, PetasosParticipant downstreamParticipant){
        getLogger().debug(".addDownstreamSubscriberParticipant(): Entry, producerParticipantName->{}, downstreamParticipant->{}", producerParticipantName, downstreamParticipant);
        if(StringUtils.isEmpty(producerParticipantName)){
            getLogger().debug(".addDownstreamSubscriberParticipant(): Exit, producerParticipantName is empty");
            return;
        }
        if(downstreamParticipant == null){
            getLogger().debug(".addDownstreamSubscriberParticipant(): Exit, downstreamParticipant is null");
            return;
        }
        getLogger().trace(".addDownstreamSubscriberParticipant(): Iterating through WorkItemManifests for participant");
        synchronized (getInterPodSubscriptionMapLock()){
            if(getInterPodSubscriptionMap().containsKey(producerParticipantName)) {
                Set<String> registrationIdSet = getInterPodSubscriptionMap().get(producerParticipantName);
                if(!registrationIdSet.contains(downstreamParticipant.getParticipantId().getName())){
                    getLogger().trace(".addDownstreamSubscriberParticipant(): Adding consumer into already existing set");
                    registrationIdSet.add(downstreamParticipant.getParticipantId().getName());
                }
            } else {
                getLogger().trace(".addDownstreamSubscriberParticipant(): Creating a new Consumer set and adding this consumer to it!");
                Set<String> registrationIdSet = new HashSet<>();
                registrationIdSet.add(downstreamParticipant.getParticipantId().getName());
                getInterPodSubscriptionMap().put(producerParticipantName, registrationIdSet);
            }
        }
        getLogger().debug(".addDownstreamSubscriberParticipant(): Exit");
    }

    /**
     *
     * @param producerParticipantName
     * @param downstreamParticipant
     */
    public void removeDownstreamSubscriberParticipant(String producerParticipantName, PetasosParticipant downstreamParticipant){
        getLogger().debug(".removeDownstreamSubscriberParticipant(): Entry, producerParticipantName->{}, downstreamParticipant->{}", producerParticipantName, downstreamParticipant);
        if(StringUtils.isEmpty(producerParticipantName)){
            getLogger().debug(".removeDownstreamSubscriberParticipant(): Exit, producerParticipantName is empty");
            return;
        }
        if(downstreamParticipant == null){
            getLogger().debug(".removeDownstreamSubscriberParticipant(): Exit, downstreamParticipant is null");
            return;
        }
        getLogger().trace(".removeDownstreamSubscriberParticipant(): Iterating through WorkItemManifests for participant");
        synchronized (getInterPodSubscriptionMapLock()){
            if(getInterPodSubscriptionMap().containsKey(producerParticipantName)) {
                Set<String> registrationIdSet = getInterPodSubscriptionMap().get(producerParticipantName);
                if(registrationIdSet.contains(downstreamParticipant.getParticipantId().getName())){
                    getLogger().trace(".removeDownstreamSubscriberParticipant(): Removing consumer from Consumer set");
                    registrationIdSet.remove(downstreamParticipant.getParticipantId().getName());
                }
                if(registrationIdSet.isEmpty()){
                    getInterPodSubscriptionMap().remove(producerParticipantName);
                }
            }
        }
        getLogger().debug(".removeDownstreamSubscriberParticipant(): Exit");
    }

    /**
     *
     * @param producerParticipantName
     * @return
     */
    public Set<PetasosParticipant> getDownstreamParticipantSet(String producerParticipantName){
        getLogger().debug(".getDownstreamSubscribers(): Entry, producerParticipantName->{}", producerParticipantName);
        Set<PetasosParticipant> downstreamTaskPerformers = new HashSet<>();
        if(StringUtils.isEmpty(producerParticipantName)){
            getLogger().debug(".getDownstreamSubscribers(): Exit, producerParticipantName is empty, so returning empty list");
            return(downstreamTaskPerformers);
        }
        if(getInterPodSubscriptionMap().containsKey(producerParticipantName)) {
            getLogger().trace(".getDownstreamTaskPerformers(): producerParticipantName has at least one registered (downstream) consumer");
            synchronized (getInterPodSubscriptionMapLock()) {
                Set<String> consumerRegistrations = getInterPodSubscriptionMap().get(producerParticipantName);
                for (String currentConsumerRegistrationId : consumerRegistrations) {
                    if (getParticipantRegistrationCache().containsKey(currentConsumerRegistrationId)) {
                        PetasosParticipant currentRegistration = getParticipantRegistrationCache().get(currentConsumerRegistrationId);
                        if(!downstreamTaskPerformers.contains(currentRegistration)) {
                            downstreamTaskPerformers.add(currentRegistration);
                        }
                    }
                }
            }
        } else {
            getLogger().trace(".getDownstreamTaskPerformers(): producerParticipantName has no (downstream) consumers");
        }
        getLogger().debug(".getDownstreamTaskPerformers(): Exit");
        return(downstreamTaskPerformers);
    }

    //
    // "Participant Service Map"
    //

    //
    // Simple Registration Check
    //

    public boolean isPetasosParticipantRegistered(PetasosParticipant publisher) {
        return false;
    }

    //
    // Macro
    //

    /**
     *
     * @return
     */
    public Set<PetasosParticipant> getAllRegistrations(){
        Set<PetasosParticipant> registrationSet = new HashSet<>();

        return(registrationSet);
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger() {
        return (LOG);
    }

    protected Cache<String, PetasosParticipant> getParticipantRegistrationCache() {
        return participantRegistrationCache;
    }

    protected Object getParticipantRegistrationCacheLock() {
        return participantRegistrationCacheLock;
    }

    protected Cache<String, Set<String>> getInterPodSubscriptionMap() {
        return interPodSubscriptionMap;
    }

    protected Object getInterPodSubscriptionMapLock() {
        return interPodSubscriptionMapLock;
    }

}
