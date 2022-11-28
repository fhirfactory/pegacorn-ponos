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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.participantgrid;

import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.participant.*;
import net.fhirfactory.pegacorn.petasos.endpoints.services.subscriptions.ParticipantServicesEndpointBase;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.ParticipantCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.oam.ProcessingPlantPathwayReportProxy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

@ApplicationScoped
public class ParticipantGridServer extends ParticipantServicesEndpointBase {
    private static final Logger LOG = LoggerFactory.getLogger(ParticipantGridServer.class);

    @Inject
    private ParticipantCacheServices petasosParticipantCache;

    @Inject
    private ProcessingPlantPathwayReportProxy processingPlantPathwayReportProxy;

    @Override
    public boolean isPetasosParticipantRegistered(PetasosParticipant participant) {
        getLogger().debug(".isPetasosParticipantRegistered(): Entry, participant->{}", participant);
        boolean isRegistered = petasosParticipantCache.getPetasosParticipantRegistration(participant.getParticipantName()) != null;
        getLogger().debug(".isPetasosParticipantRegistered(): Exit, isRegistered->{}", isRegistered);
        return(isRegistered);
    }

    @Override
    public PetasosParticipantRegistration getPetasosParticipantRegistration(String participantName) {
        getLogger().info(".getPetasosParticipantRegistration(): Entry, participantName->{}", participantName);
        if(StringUtils.isEmpty(participantName)){
            getLogger().info(".getPetasosParticipantRegistration(): Exit, participant is null, returning null");
            return (null);
        }
        PetasosParticipantRegistration registration = petasosParticipantCache.getPetasosParticipantRegistration(participantName);
        getLogger().info(".getPetasosParticipantRegistration(): Exit, registration->{}", registration);
        return(registration);
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
    public Set<PetasosParticipantRegistration> getAllRegistrations() {
        Set<PetasosParticipantRegistration> allRegistrations = petasosParticipantCache.getAllRegistrations();
        return (allRegistrations);
    }

    @Override
    public PetasosParticipantRegistrationSet updateParticipantRegistrationSet(String participantName, PetasosParticipantRegistrationSet registrationSet) {
        getLogger().debug(".updateParticipantRegistrationSet(): Entry, participantName->{}", participantName);
        PetasosParticipantRegistrationSet updates = new PetasosParticipantRegistrationSet();

        if(registrationSet != null){
            getLogger().trace(".updateParticipantRegistrationSet(): Not an empty set, size->{}", registrationSet.getRegistrationSet().size());
            for(PetasosParticipantRegistration currentRegistration: registrationSet.getRegistrationSet().values()){
                getLogger().trace(".updateParticipantRegistrationSet(): Processing->{}", currentRegistration.getParticipant().getParticipantName());
                PetasosParticipantRegistration petasosParticipantRegistration = petasosParticipantCache.registerPetasosParticipant(currentRegistration.getParticipant());
                updates.addRegistration(petasosParticipantRegistration);
            }
        }
        getLogger().debug(".updateParticipantRegistrationSet(): Exit");
        return(updates);
    }

    @Override
    public PetasosParticipantStatusSet updateParticipantStatusSet(String participantName, PetasosParticipantStatusSet statusSet) {
        getLogger().debug(".updateParticipantStatusSet(): Entry, participantName->{}", participantName);
        PetasosParticipantStatusSet updates = new PetasosParticipantStatusSet();

        if(statusSet != null){
            getLogger().debug(".updateParticipantStatusSet(): Not an empty set, size->{}", statusSet.getStatusMap().size());
            for(PetasosParticipantStatus currentStatus: statusSet.getStatusMap().values()){
                getLogger().debug(".updateParticipantStatusSet(): Processing->{}", currentStatus.getParticipantName());
                petasosParticipantCache.setParticipantStatus(currentStatus.getParticipantName(), currentStatus.getControlStatus());
            }
        }
        getLogger().debug(".updateParticipantStatusSet(): Exit");
        return(updates);
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
