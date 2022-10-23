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
import net.fhirfactory.pegacorn.core.model.petasos.participant.registration.PetasosParticipantRegistration;
import net.fhirfactory.pegacorn.core.model.petasos.participant.registration.PetasosParticipantRegistrationStatus;
import net.fhirfactory.pegacorn.core.model.petasos.participant.registration.PetasosParticipantRegistrationStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.work.datatypes.TaskWorkItemManifestType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.work.datatypes.TaskWorkItemSubscriptionType;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.core.PonosReplicatedCacheServices;
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

    // Cache<registrationId, publisherRegistration>
    private Cache<String, PetasosParticipantRegistration> petasosParticipantRegistrationCache;
    private Object petasosParticipantRegistrationCacheLock;

    // Cache<componentId, registrationId>
    private Cache<ComponentIdType, String> petasosParticipantComponentIdMap;
    private Object petasosParticipantComponentIdMapLock;

    // Cache<producerServiceName, Set<registrationId>>
    private Cache<String, Set<String>> producerTaskConsumerMap;
    private Object producerTaskConsumerMapLock;

    // Cache<producerServiceName, Set<registrationId>>
    private Cache<String, Set<String>> participantServiceNameToPetasosParticipantRegistration;
    private Object participantServiceNameToPetasosParticipantRegistrationLock;


    @Inject
    private PonosReplicatedCacheServices replicatedCacheServices;

    //
    // Constructor(s)
    //

    public ParticipantCacheServices(){
        this.initialised = false;
        this.petasosParticipantRegistrationCache = null;
        this.petasosParticipantRegistrationCacheLock = new Object();
        this.participantServiceNameToPetasosParticipantRegistration = null;
        this.participantServiceNameToPetasosParticipantRegistrationLock = new Object();
        this.petasosParticipantRegistrationCache = null;
        this.petasosParticipantComponentIdMapLock = new Object();
        this.producerTaskConsumerMap = null;
        this.producerTaskConsumerMapLock = new Object();
    }

    @PostConstruct
    public void initialise() {
        getLogger().debug(".initialise(): Entry");
        if (!initialised) {
            getLogger().info(".initialise(): Initialisation Start");

            getLogger().info(".initialise(): [Initialising Caches] Start");
            petasosParticipantRegistrationCache = replicatedCacheServices.getCacheManager().createCache("PetasosParticipantRegistrationCache", replicatedCacheServices.getCacheConfigurationBuild());
            petasosParticipantComponentIdMap = replicatedCacheServices.getCacheManager().createCache("PetasosParticipantComponentIdMap", replicatedCacheServices.getCacheConfigurationBuild());
            producerTaskConsumerMap = replicatedCacheServices.getCacheManager().createCache("TaskProducerConsumerMap", replicatedCacheServices.getCacheConfigurationBuild());
            participantServiceNameToPetasosParticipantRegistration = replicatedCacheServices.getCacheManager().createCache("PetasosParticipantServiceMap", replicatedCacheServices.getCacheConfigurationBuild());
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
    public PetasosParticipantRegistration registerPetasosParticipant(PetasosParticipantRegistration localParticipantRegistration){
        getLogger().debug(".registerPetasosParticipant(): Entry, localParticipantRegistration->{}", localParticipantRegistration);
        PetasosParticipantRegistration registration = null;
        getLogger().trace(".registerPetasosParticipant(): First, we check the content of the passed-in parameter");
        if(localParticipantRegistration == null){
            getLogger().debug("registerPetasosParticipant(): Exit, localParticipantRegistration is null, returning -null-");
            return(null);
        }
        getLogger().trace(".registerTaskProducer(): Now, check to see if localParticipantRegistration (instance) is already cached and, if so, do nothing!");
        if(getPetasosParticipantComponentIdMap().containsKey(localParticipantRegistration.getLocalComponentId())){
            String registrationId = getPetasosParticipantComponentIdMap().get(localParticipantRegistration.getLocalComponentId());
            registration = getPetasosParticipantRegistrationCache().get(registrationId);
            getLogger().debug("registerPublisherInstance(): Exit, localParticipantRegistration already registered, registration->{}", registration);
            return(registration);
        } else {
            getLogger().trace(".registerTaskProducer(): Publisher is not in Map, so add it!");
            registration = new PetasosParticipantRegistration(localParticipantRegistration);
            registration.setRegistrationCommentary("Publisher Registered");
            registration.setCentralRegistrationInstant(Instant.now());
            registration.setCentralRegistrationStatus(PetasosParticipantRegistrationStatusEnum.PARTICIPANT_REGISTERED);
            synchronized (this.petasosParticipantRegistrationCacheLock) {
                getPetasosParticipantRegistrationCache().put(registration.getRegistrationId(), registration);
            }
            synchronized(this.getPetasosParticipantComponentIdMapLock()){
                getPetasosParticipantComponentIdMap().put(localParticipantRegistration.getLocalComponentId(), registration.getRegistrationId());
            }
            if(localParticipantRegistration.getComponentType().equals(SoftwareComponentTypeEnum.PROCESSING_PLANT)) {
                for (TaskWorkItemSubscriptionType currentTaskWorkItem : localParticipantRegistration.getSubscriptions()) {
                    addDownstreamSubscriberParticipant(currentTaskWorkItem.getSourceProcessingPlantParticipantName(), registration);
                }
                addPetasosParticipantInstanceForParticipantName(localParticipantRegistration.getParticipantId().getSubsystemName(), registration);
            }
            registration.setControlStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED);
            getLogger().debug(".registerTaskProducer(): Exit, registration->{}", registration);
            return (registration);
        }
    }

    /**
     *
     * @param participant
     * @return
     */
    public PetasosParticipantRegistration deregisterPetasosParticipant(PetasosParticipantRegistration participant){
        getLogger().debug(".deregisterPetasosParticipant(): Entry, participant->{}", participant);
        if(participant == null){
            getLogger().debug("deregisterPetasosParticipant(): Exit, participant is null");
            return(null);
        }
        PetasosParticipantRegistration registration = deregisterPetasosParticipant(participant.getLocalComponentId());
        getLogger().debug(".deregisterPetasosParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    /**
     *
     * @param componentId
     * @return
     */
    public PetasosParticipantRegistration deregisterPetasosParticipant(ComponentIdType componentId) {
        getLogger().debug(".deregisterPetasosParticipant(): Entry, componentId->{}", componentId);
        PetasosParticipantRegistration registration = null;
        if (componentId == null) {
            getLogger().debug("deregisterPetasosParticipant(): Exit, participant is null");
            return (null);
        }
        if(getPetasosParticipantComponentIdMap().containsKey(componentId)){
            String registrationId = getPetasosParticipantComponentIdMap().get(componentId);
            registration = getPetasosParticipantRegistrationCache().get(registrationId);
            synchronized (this.petasosParticipantRegistrationCacheLock) {
                getPetasosParticipantRegistrationCache().remove(registrationId);
            }
            synchronized (this.getPetasosParticipantComponentIdMapLock()) {
                getPetasosParticipantComponentIdMap().remove(componentId);
            }
            if(registration.getComponentType().equals(SoftwareComponentTypeEnum.PROCESSING_PLANT)) {
                for (TaskWorkItemSubscriptionType currentTaskWorkItem : registration.getSubscriptions()) {
                    removeDownstreamSubscriberParticipant(currentTaskWorkItem.getSourceProcessingPlantParticipantName(), registration);
                }
                removePetasosParticipantInstanceForParticipantName(registration.getParticipantId().getSubsystemName(), registration);
            }
            registration.setCentralRegistrationStatus(PetasosParticipantRegistrationStatusEnum.PARTICIPANT_UNREGISTERED);
            registration.setCentralRegistrationInstant(Instant.now());
        }
        getLogger().debug(".deregisterPetasosParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    /**
     *
     * @param localParticipantRegistration
     * @return
     */
    public PetasosParticipantRegistration updateParticipantRegistration(PetasosParticipantRegistration localParticipantRegistration){
        getLogger().debug(".updateParticipantRegistration(): Entry, localParticipantRegistration->{}", localParticipantRegistration);
        if(localParticipantRegistration == null){
            getLogger().debug(".updateParticipantRegistration(): Exit, localParticipantRegistration is null, returning null");
            return(null);
        }
        PetasosParticipantRegistration centralRegistration = getParticipantRegistration(localParticipantRegistration.getLocalComponentId());
        if(centralRegistration == null){
            centralRegistration = registerPetasosParticipant(localParticipantRegistration);
        } else {
            centralRegistration.setLocalRegistrationStatus(localParticipantRegistration.getLocalRegistrationStatus());
            centralRegistration.setLocalRegistrationInstant(localParticipantRegistration.getLocalRegistrationInstant());
            if(!centralRegistration.getCentralRegistrationStatus().equals(PetasosParticipantRegistrationStatusEnum.PARTICIPANT_REGISTERED)){
                centralRegistration.setCentralRegistrationStatus(PetasosParticipantRegistrationStatusEnum.PARTICIPANT_REGISTERED);
                centralRegistration.setCentralRegistrationInstant(Instant.now());
            }
            if(!localParticipantRegistration.getSubscriptions().isEmpty()){
                for(TaskWorkItemSubscriptionType currentSubscription: localParticipantRegistration.getSubscriptions()){
                    if(!centralRegistration.getSubscriptions().contains(currentSubscription)){
                        centralRegistration.getSubscriptions().add(currentSubscription);
                    }
                }
            }
            if(!localParticipantRegistration.getOutputs().isEmpty()){
                for(TaskWorkItemManifestType currentOutput: localParticipantRegistration.getOutputs()){
                    if(!centralRegistration.getOutputs().contains(currentOutput)){
                        centralRegistration.getOutputs().add(currentOutput);
                    }
                }
            }
            centralRegistration.setParticipantStatus(localParticipantRegistration.getParticipantStatus());
            centralRegistration.setControlStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED);
            centralRegistration.setLocalComponentStatus(localParticipantRegistration.getLocalComponentStatus());
        }
        getLogger().debug(".updateParticipantRegistration(): Exit, centralRegistration->{}", centralRegistration);
        return(centralRegistration);
    }

    /**
     *
     * @param componentId
     * @return
     */
    public PetasosParticipantRegistration getParticipantRegistration(ComponentIdType componentId){
        getLogger().debug(".getPetasosParticipantRegistration(): Entry, componentId->{}", componentId);
        if(getPetasosParticipantComponentIdMap().containsKey(componentId)){
            String registrationId = getPetasosParticipantComponentIdMap().get(componentId);
            if(getPetasosParticipantRegistrationCache().containsKey(registrationId)){
                PetasosParticipantRegistration registration = getPetasosParticipantRegistrationCache().get(registrationId);
                getLogger().debug(".getPetasosParticipantRegistration(): Exit, registration->{}", registration);
                return(registration);
            }
        }
        getLogger().debug(".getPetasosParticipantRegistration(): Exit, could not find registration");
        return(null);
    }

    //
    // "Subscription" Map (ProducerTaskConsumerMap)
    //

    /**
     *
     * @param producerParticipantName
     * @param downstreamParticipant
     */
    public void addDownstreamSubscriberParticipant(String producerParticipantName, PetasosParticipantRegistration downstreamParticipant){
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
        synchronized (getProducerTaskConsumerMapLock()){
            if(getProducerTaskConsumerMap().containsKey(producerParticipantName)) {
                Set<String> registrationIdSet = getProducerTaskConsumerMap().get(producerParticipantName);
                if(!registrationIdSet.contains(downstreamParticipant.getRegistrationId())){
                    getLogger().trace(".addDownstreamSubscriberParticipant(): Adding consumer into already existing set");
                    registrationIdSet.add(downstreamParticipant.getRegistrationId());
                }
            } else {
                getLogger().trace(".addDownstreamSubscriberParticipant(): Creating a new Consumer set and adding this consumer to it!");
                Set<String> registrationIdSet = new HashSet<>();
                registrationIdSet.add(downstreamParticipant.getRegistrationId());
                getProducerTaskConsumerMap().put(producerParticipantName, registrationIdSet);
            }
        }
        getLogger().debug(".addDownstreamSubscriberParticipant(): Exit");
    }

    /**
     *
     * @param producerParticipantName
     * @param downstreamParticipant
     */
    public void removeDownstreamSubscriberParticipant(String producerParticipantName, PetasosParticipantRegistrationStatus downstreamParticipant){
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
        synchronized (getProducerTaskConsumerMapLock()){
            if(getProducerTaskConsumerMap().containsKey(producerParticipantName)) {
                Set<String> registrationIdSet = getProducerTaskConsumerMap().get(producerParticipantName);
                if(registrationIdSet.contains(downstreamParticipant.getRegistrationId())){
                    getLogger().trace(".removeDownstreamSubscriberParticipant(): Removing consumer from Consumer set");
                    registrationIdSet.remove(downstreamParticipant.getRegistrationId());
                }
                if(registrationIdSet.isEmpty()){
                    getProducerTaskConsumerMap().remove(producerParticipantName);
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
    public Set<PetasosParticipantRegistration> getDownstreamParticipantSet(String producerParticipantName){
        getLogger().debug(".getDownstreamSubscribers(): Entry, producerParticipantName->{}", producerParticipantName);
        Set<PetasosParticipantRegistration> downstreamTaskPerformers = new HashSet<>();
        if(StringUtils.isEmpty(producerParticipantName)){
            getLogger().debug(".getDownstreamSubscribers(): Exit, producerParticipantName is empty, so returning empty list");
            return(downstreamTaskPerformers);
        }
        if(getProducerTaskConsumerMap().containsKey(producerParticipantName)) {
            getLogger().trace(".getDownstreamTaskPerformers(): producerParticipantName has at least one registered (downstream) consumer");
            synchronized (getProducerTaskConsumerMapLock()) {
                Set<String> consumerRegistrations = getProducerTaskConsumerMap().get(producerParticipantName);
                for (String currentConsumerRegistrationId : consumerRegistrations) {
                    if (getPetasosParticipantRegistrationCache().containsKey(currentConsumerRegistrationId)) {
                        PetasosParticipantRegistration currentRegistration = getPetasosParticipantRegistrationCache().get(currentConsumerRegistrationId);
                        downstreamTaskPerformers.add(currentRegistration);
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

    /**
     *
     * @param participantName
     * @param localParticipantRegistration
     */
    public void addPetasosParticipantInstanceForParticipantName(String participantName, PetasosParticipantRegistration localParticipantRegistration){
        getLogger().debug(".addPetasosParticipantInstanceForParticipantName(): participantName->{}, localParticipantRegistration->{}", participantName, localParticipantRegistration);
        if(localParticipantRegistration == null){
            getLogger().debug(".addPetasosParticipantInstanceForParticipantName(): Exit, localParticipantRegistration is null");
            return;
        }
        if(localParticipantRegistration.getComponentType().equals(SoftwareComponentTypeEnum.PROCESSING_PLANT)){
            if(StringUtils.isEmpty(participantName)){
                getLogger().debug(".addPetasosParticipantInstanceForParticipantName(): Exit, participantName is empty");
                return;
            }
            synchronized (getParticipantServiceNameToPetasosParticipantRegistrationLock()){
                if(getParticipantServiceNameToPetasosParticipantRegistration().containsKey(participantName)){
                    getLogger().debug(".addPetasosParticipantInstanceForParticipantName(): Adding localParticipantRegistration to an existing Service Set");
                    Set<String> instanceRegistrationSet = getParticipantServiceNameToPetasosParticipantRegistration().get(participantName);
                    if(!instanceRegistrationSet.contains(localParticipantRegistration.getRegistrationId())){
                        instanceRegistrationSet.add(localParticipantRegistration.getRegistrationId());
                    }
                } else {
                    getLogger().debug(".addPetasosParticipantInstanceForParticipantName(): Creating a new Service Set and adding localParticipantRegistration to it");
                    Set<String> instanceRegistrationSet = new HashSet<>();
                    instanceRegistrationSet.add(localParticipantRegistration.getRegistrationId());
                    getParticipantServiceNameToPetasosParticipantRegistration().put(participantName, instanceRegistrationSet);
                }
            }
        } else {
            getLogger().debug(".addPetasosParticipantInstanceForParticipantName(): localParticipantRegistration is for a non-ProcessingPlant component, so doing nothing");
        }
        getLogger().debug(".addPetasosParticipantInstanceForParticipantName(): Exit");
    }

    /**
     *
     * @param participantName
     * @param registration
     */
    public void removePetasosParticipantInstanceForParticipantName(String participantName, PetasosParticipantRegistration registration){
        getLogger().debug(".removePetasosParticipantInstanceForParticipantName(): participantName->{}, registration->{}", participantName, registration);
        if(registration == null){
            getLogger().debug(".removePetasosParticipantInstanceForParticipantName(): Exit, registration is null");
            return;
        }
        if(registration.getComponentType().equals(SoftwareComponentTypeEnum.PROCESSING_PLANT)){
            if(StringUtils.isEmpty(participantName)){
                getLogger().debug(".removePetasosParticipantInstanceForParticipantName(): Exit, participantName is empty");
                return;
            }
            synchronized (getParticipantServiceNameToPetasosParticipantRegistrationLock()){
                if(getParticipantServiceNameToPetasosParticipantRegistration().containsKey(participantName)){
                    getLogger().debug(".removePetasosParticipantInstanceForParticipantName(): Adding registration to an existing Service Set");
                    Set<String> instanceRegistrationSet = getParticipantServiceNameToPetasosParticipantRegistration().get(participantName);
                    if(!instanceRegistrationSet.contains(registration.getRegistrationId())){
                        instanceRegistrationSet.remove(registration.getRegistrationId());
                    }
                    if(instanceRegistrationSet.isEmpty()){
                        getParticipantServiceNameToPetasosParticipantRegistration().remove(participantName);
                    }
                }
            }
        } else {
            getLogger().debug(".removePetasosParticipantInstanceForParticipantName(): registration is for a non-ProcessingPlant component, so doing nothing");
        }
        getLogger().debug(".removePetasosParticipantInstanceForParticipantName(): Exit");

    }

    /**
     *
     * @param participantName
     * @return
     */
    public Set<PetasosParticipantRegistration> getPetasosParticipantInstancesForParticipantName(String participantName){
        getLogger().debug(".getPetasosParticipantInstancesForParticipantName(): Entry, participantName->{}", participantName);
        Set<PetasosParticipantRegistration> petasosParticipantSet = new HashSet<>();
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getPetasosParticipantInstancesForParticipantName(): Exit, participantName is empty");
            return(petasosParticipantSet);
        }
        synchronized (getParticipantServiceNameToPetasosParticipantRegistrationLock()) {
            if (getParticipantServiceNameToPetasosParticipantRegistration().containsKey(participantName)) {
                Set<String> participantRegistrationIdSet = getParticipantServiceNameToPetasosParticipantRegistration().get(participantName);
                if(!participantRegistrationIdSet.isEmpty()){
                    for(String participantRegistrationId: participantRegistrationIdSet){
                        if(getPetasosParticipantRegistrationCache().containsKey(participantRegistrationId)){
                            PetasosParticipantRegistration currentRegistration = getPetasosParticipantRegistrationCache().get(participantRegistrationId);
                            petasosParticipantSet.add(currentRegistration);
                        }
                    }
                }
            }
        }
        getLogger().debug(".getPetasosParticipantInstancesForParticipantName(): Exit");
        return(petasosParticipantSet);
    }

    /**
     *
     * @param participantName
     * @return
     */

    public Set<PetasosParticipantRegistration> getParticipantRegistrationSetForParticipantName(String participantName){
        getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Entry, participantName->{}", participantName);
        Set<PetasosParticipantRegistration> participantRegistrations = new HashSet<>();
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Exit, participantName is empty");
            return(participantRegistrations);
        }
        synchronized (getParticipantServiceNameToPetasosParticipantRegistrationLock()) {
            if (getParticipantServiceNameToPetasosParticipantRegistration().containsKey(participantName)) {
                Set<String> participantRegistrationIdSet = getParticipantServiceNameToPetasosParticipantRegistration().get(participantName);
                if(!participantRegistrationIdSet.isEmpty()){
                    for(String participantRegistrationId: participantRegistrationIdSet){
                        if(getPetasosParticipantRegistrationCache().containsKey(participantRegistrationId)){
                            PetasosParticipantRegistration currentRegistration = getPetasosParticipantRegistrationCache().get(participantRegistrationId);
                            participantRegistrations.add(currentRegistration);
                        }
                    }
                }
            }
        }
        getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Exit");
        return(participantRegistrations);
    }

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
    public Set<PetasosParticipantRegistration> getAllRegistrations(){
        Set<PetasosParticipantRegistration> registrationSet = new HashSet<>();

        return(registrationSet);
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger() {
        return (LOG);
    }

    protected Cache<String, PetasosParticipantRegistration> getPetasosParticipantRegistrationCache() {
        return petasosParticipantRegistrationCache;
    }

    protected Object getPetasosParticipantRegistrationCacheLock() {
        return petasosParticipantRegistrationCacheLock;
    }

    protected Cache<ComponentIdType, String> getPetasosParticipantComponentIdMap() {
        return petasosParticipantComponentIdMap;
    }

    protected Object getPetasosParticipantComponentIdMapLock() {
        return petasosParticipantComponentIdMapLock;
    }

    protected Cache<String, Set<String>> getParticipantServiceNameToPetasosParticipantRegistration() {
        return participantServiceNameToPetasosParticipantRegistration;
    }

    protected Object getParticipantServiceNameToPetasosParticipantRegistrationLock() {
        return this.participantServiceNameToPetasosParticipantRegistrationLock;
    }

    protected Cache<String, Set<String>> getProducerTaskConsumerMap() {
        return producerTaskConsumerMap;
    }

    protected Object getProducerTaskConsumerMapLock() {
        return producerTaskConsumerMapLock;
    }

}
