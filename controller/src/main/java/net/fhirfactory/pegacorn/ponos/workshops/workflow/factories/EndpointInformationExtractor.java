/*
 * The MIT License
 *
 * Copyright 2022 Mark A. Hunter (ACT Health).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.fhirfactory.pegacorn.ponos.workshops.workflow.factories;

import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointTopologyTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.base.IPCTopologyEndpoint;
import net.fhirfactory.pegacorn.core.model.topology.nodes.WorkUnitProcessorSoftwareComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;

/**
 *
 * @author m.a.hunter@outlook.com
 */
@ApplicationScoped
public class EndpointInformationExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(EndpointInformationExtractor.class);
    
    
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
    
    public ComponentIdType getEndpointComponentId(PetasosActionableTask actionableTask){
        getLogger().debug(".getEndpointComponentId(): Entry, actionableTask->{}", actionableTask);
        
        ComponentIdType componentId = null;
        
        if(actionableTask != null){
            if(actionableTask.hasTaskFulfillment()){
                if(actionableTask.getTaskFulfillment().hasFulfiller()){
                    componentId = actionableTask.getTaskFulfillment().getFulfiller().getComponentID();
                }
            }
        }
        
        getLogger().debug(".getEndpointComponentId(): Exit, componentId->{}", componentId);
        return(componentId);
    }
    
    public String getEndpointParticipantName(PetasosActionableTask actionableTask, boolean isIngres){
        getLogger().debug(".getEndpointParticipantName(): Entry, actionableTask->{}, isIngres->{}", actionableTask, isIngres);
       
        String participantName = null;
        if(actionableTask != null){
            if(actionableTask.hasTaskFulfillment()){
                if(actionableTask.getTaskFulfillment().hasFulfiller()){
                    WorkUnitProcessorSoftwareComponent wupSoftwareComponent = (WorkUnitProcessorSoftwareComponent)actionableTask.getTaskFulfillment().getFulfiller();
                    participantName = getEndpointParticipantName(wupSoftwareComponent, isIngres);
                }
            }
        }
        
        getLogger().debug(".getEndpointParticipantName(): Exit, participantName->{}", participantName);
        return(participantName);
    }
    
    public String getEndpointParticipantName(WorkUnitProcessorSoftwareComponent endpointWUP, boolean isIngres){
        getLogger().debug(".getEndpointParticipantName(): Entry, endpointWUP->{}", endpointWUP);
        
        String participantName = null;
        if(isIngres){
            IPCTopologyEndpoint ingresEndpoint = endpointWUP.getIngresEndpoint();
            if(ingresEndpoint != null){
                if(ingresEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.MLLP_SERVER)){
                    participantName = ingresEndpoint.getParticipantId().getName();
                } else if(ingresEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.HTTP_API_SERVER)){
                    participantName = ingresEndpoint.getParticipantId().getName();
                } else if(ingresEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.FILE_SHARE_SOURCE)){
                    participantName = ingresEndpoint.getParticipantId().getName();
                }
            }
        } else {
            IPCTopologyEndpoint egressEndpoint = endpointWUP.getEgressEndpoint();
            if(egressEndpoint != null){
                if(egressEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.MLLP_CLIENT)){
                    participantName= egressEndpoint.getParticipantId().getName();
                } else if(egressEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.HTTP_API_CLIENT)){
                    participantName = egressEndpoint.getParticipantId().getName();
                } else if(egressEndpoint.getEndpointType().equals(PetasosEndpointTopologyTypeEnum.FILE_SHARE_SINK)){
                    participantName = egressEndpoint.getParticipantId().getName();
                }                
            }
        }
        
        getLogger().debug(".getEndpointParticipantName(): Exit, participantName->{}", participantName);
        return(participantName);
    }
}
