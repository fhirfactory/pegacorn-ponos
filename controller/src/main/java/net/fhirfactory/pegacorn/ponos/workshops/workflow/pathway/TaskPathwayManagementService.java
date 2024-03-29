/*
 * Copyright (c) 2020 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.pathway;

import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipant;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipantRegistration;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.PonosPetasosParticipantCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.oam.ProcessingPlantPathwayReportProxy;
import net.fhirfactory.pegacorn.services.tasks.endpoint.PetasosTaskPerformerServicesManagerEndpoint;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

@ApplicationScoped
public class TaskPathwayManagementService  extends PetasosTaskPerformerServicesManagerEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(TaskPathwayManagementService.class);

    @Inject
    private PonosPetasosParticipantCacheServices petasosParticipantCache;

    @Inject
    private ProcessingPlantPathwayReportProxy processingPlantPathwayReportProxy;

    @Override
    public boolean isPetasosParticipantRegistered(PetasosParticipant participant) {
        getLogger().debug(".isPetasosParticipantRegistered(): Entry, participant->{}", participant);
        boolean isRegistered = petasosParticipantCache.getPetasosParticipantRegistration(participant.getComponentID()) != null;
        getLogger().debug(".isPetasosParticipantRegistered(): Exit, isRegistered->{}", isRegistered);
        return(isRegistered);
    }


    @Override
    public Set<PetasosParticipant> getDownstreamTaskPerformersForTaskProducer(String producerParticipantName) {
        getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): Entry, producerParticipantName->{}", producerParticipantName);
        Set<PetasosParticipant> subscriberSet = petasosParticipantCache.getDownstreamParticipantSet(producerParticipantName);
        if(getLogger().isInfoEnabled()){
            getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): subscriberSet->{}", subscriberSet);
        }
        processingPlantPathwayReportProxy.touchProcessingPlantSubscriptionSynchronisationInstant(producerParticipantName);
        getLogger().info(".getDownstreamTaskPerformersForTaskProducer(): Exit");
        return(subscriberSet);
    }

    @Override
    public PetasosParticipantRegistration registerPetasosParticipant(PetasosParticipant participant) {
        getLogger().info(".registerPetasosParticipant(): Entry, participant->{}", participant);
        if(participant == null) {
            getLogger().info(".registerPetasosParticipant(): Exit, participant is null, returning null");
            return (null);
        }
        PetasosParticipantRegistration registration = petasosParticipantCache.registerPetasosParticipant(participant);
        processingPlantPathwayReportProxy.reportParticipantRegistration(participant, false);
        getLogger().info(".registerPetasosParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    @Override
    public PetasosParticipantRegistration updatePetasosParticipant(PetasosParticipant participant) {
        getLogger().debug(".updatePetasosParticipant(): Entry, participant->{}", participant);
        if(participant == null){
            getLogger().debug(".updatePetasosParticipant(): Exit, participant is null, returning null");
            return(null);
        }
        PetasosParticipantRegistration registration = petasosParticipantCache.updatePetasosParticipant(participant);
        processingPlantPathwayReportProxy.reportParticipantRegistration(participant, true);
        getLogger().debug(".updatePetasosParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    @Override
    public PetasosParticipantRegistration deregisterPetasosParticipant(PetasosParticipant participant) {
        getLogger().debug(".deregisterPetasosParticipant(): Entry, participant->{}", participant);
        if(participant == null){
            getLogger().debug(".deregisterPetasosParticipant(): Exit, participant is null, returning null");
            return(null);
        }
        PetasosParticipantRegistration registration = petasosParticipantCache.deregisterPetasosParticipant(participant);
        getLogger().debug(".deregisterPetasosParticipant(): Exit, registration->{}", registration);
        return(registration);
    }

    @Override
    public PetasosParticipantRegistration getPetasosParticipantRegistration(ComponentIdType participantId) {
        getLogger().debug(".getPetasosParticipantRegistration(): Entry, participantId->{}", participantId);
        if(participantId == null){
            getLogger().debug(".getPetasosParticipantRegistration(): Exit, participantId is null, returning null");
            return(null);
        }
        PetasosParticipantRegistration registration = petasosParticipantCache.deregisterPetasosParticipant(participantId);
        getLogger().debug(".getPetasosParticipantRegistration(): Exit, registration->{}", registration);
        return(registration);
    }

    @Override
    public Set<PetasosParticipantRegistration> getParticipantRegistrationSetForParticipantName(String participantName) {
        getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Entry, participantName->{}", participantName);
        if(StringUtils.isEmpty(participantName)){
            getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Exit, participantName is null, returning empty set");
            return(new HashSet<>());
        }
        Set<PetasosParticipantRegistration> downstreamTaskPerformers = petasosParticipantCache.getParticipantRegistrationSetForParticipantName(participantName);
        getLogger().debug(".getParticipantRegistrationSetForParticipantName(): Exit");
        return (downstreamTaskPerformers);
    }

    @Override
    public Set<PetasosParticipantRegistration> getAllRegistrations() {
        Set<PetasosParticipantRegistration> allRegistrations = petasosParticipantCache.getAllRegistrations();
        return (allRegistrations);
    }

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    @Override
    public PetasosParticipant getMyPetasosParticipant() {
        return (getMyPetasosParticipant());
    }

    @Override
    protected void initialiseCacheSynchronisationDaemon() {

    }
}
