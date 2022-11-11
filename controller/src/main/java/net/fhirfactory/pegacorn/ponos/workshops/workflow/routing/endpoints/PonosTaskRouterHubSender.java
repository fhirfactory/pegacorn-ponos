/*
 * Copyright (c) 2021 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.routing.endpoints;

import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointFunctionTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointTopologyTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipantControlStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.edge.jgroups.JGroupsIntegrationPointSummary;
import net.fhirfactory.pegacorn.internals.fhir.r4.resources.endpoint.valuesets.EndpointPayloadTypeEnum;
import net.fhirfactory.pegacorn.platform.edge.model.ipc.packets.InterProcessingPlantHandoverPacket;
import net.fhirfactory.pegacorn.platform.edge.model.ipc.packets.InterProcessingPlantHandoverResponsePacket;
import net.fhirfactory.pegacorn.platform.edge.model.router.TaskRouterResponsePacket;
import net.fhirfactory.pegacorn.platform.edge.model.router.TaskRouterStatusPacket;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.routing.endpoints.common.PonosTaskRouterHubCommon;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.jgroups.Address;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class PonosTaskRouterHubSender extends PonosTaskRouterHubCommon {
    private static final Logger LOG = LoggerFactory.getLogger(PonosTaskRouterHubSender.class);

    @Produce
    private ProducerTemplate camelProducer;

    //
    // Constructor(s)
    //

    public PonosTaskRouterHubSender(){
        super();
    }

    //
    // PostConstruct Activities
    //

    @Override
    protected void executePostConstructActivities() {

    }

    //
    // Getters and Setters
    //

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }


    protected ProducerTemplate getCamelProducer() {
        return camelProducer;
    }

    //
    // Endpoint Specifications
    //

    @Override
    protected String specifyIPCInterfaceName() {
        return (getInterfaceNames().getPetasosIPCMessagingEndpointName());
    }

    @Override
    protected PetasosEndpointTopologyTypeEnum specifyIPCType() {
        return (PetasosEndpointTopologyTypeEnum.JGROUPS_INTEGRATION_POINT);
    }

    @Override
    protected String specifyJGroupsStackFileName() {
        return (getProcessingPlant().getMeAsASoftwareComponent().getPetasosIPCStackConfigFile());
    }

    @Override
    protected String specifyJGroupsClusterName() {
        return (getComponentNameUtilities().getPetasosIpcMessagingGroupName());
    }

    @Override
    protected void addIntegrationPointToJGroupsIntegrationPointSet() {
        getJgroupsIPSet().setPetasosTaskRoutingReceiverEndpoint(getJGroupsIntegrationPoint());
    }

    @Override
    protected String specifySubsystemParticipantName() {
        return (getProcessingPlant().getSubsystemParticipantName());
    }

    @Override
    protected PetasosEndpointFunctionTypeEnum specifyPetasosEndpointFunctionType() {
        return (PetasosEndpointFunctionTypeEnum.PETASOS_TASK_ROUTING_FORWARDER_ENDPOINT);
    }

    @Override
    protected EndpointPayloadTypeEnum specifyPetasosEndpointPayloadType() {
        return (EndpointPayloadTypeEnum.ENDPOINT_PAYLOAD_INTERNAL_TASK_ROUTING_FORWARDER);
    }

    //
    // Processing Plant check triggered by JGroups Cluster membership change
    //

    @Override
    protected void doIntegrationPointBusinessFunctionCheck(JGroupsIntegrationPointSummary integrationPointSummary, boolean isRemoved, boolean isAdded) {

    }

    //
    // Local Message Route Injection
    //

    protected InterProcessingPlantHandoverResponsePacket injectMessageIntoRoute(InterProcessingPlantHandoverPacket handoverPacket) {
        getLogger().debug(".injectMessageIntoRoute(): Entry, handoverPacket->{}", handoverPacket);
        InterProcessingPlantHandoverResponsePacket response =
                (InterProcessingPlantHandoverResponsePacket)getCamelProducer().sendBody(getIPCComponentNames().getInterZoneIPCReceiverRouteEndpointName(), ExchangePattern.InOut, handoverPacket);
        getLogger().debug(".injectMessageIntoRoute(): Exit, response->{}", response);
        return(response);
    }

    //
    // Message Senders
    //

    public TaskRouterResponsePacket forwardTask(String targetParticipantName, PetasosActionableTask task){
        getLogger().debug(".forwardTask(): Entry, targetParticipantName->{}, task->{}", targetParticipantName, task);
        Address targetAddress = resolveTargetAddressForTaskReceiver(targetParticipantName);
        if(targetAddress == null){
            getLogger().error(".forwardTask(): Cannot find candidate service address: targetParticipantName->{}, task->{}", targetParticipantName, task);
            getMetricsAgent().sendITOpsNotification("Error: Cannot find candidate " + targetParticipantName);
            getProcessingPlantMetricsAgent().sendITOpsNotification("Error: Cannot find candidate " + targetParticipantName);
            return(null);
        }
        try {
            String sourceName = getProcessingPlant().getMeAsASoftwareComponent().getParticipantName();
            Object objectSet[] = new Object[]{sourceName, task};
            Class classSet[] = createClassSet(objectSet);
            RequestOptions requestOptions = new RequestOptions( ResponseMode.GET_FIRST, getRPCUnicastTimeout());
            TaskRouterResponsePacket response = null;
            synchronized(getIPCChannelLock()) {
                response = getRPCDispatcher().callRemoteMethod(targetAddress, "receiveTask", objectSet, classSet, requestOptions);
            }
            getLogger().trace(".forwardTask(): Message.SEND.RESPONSE: response->{}", response);
            if(getLogger().isInfoEnabled()){
                getLogger().info(".forwardTask(): Forwarding of Task Complete");
            }
            return(response);
        } catch (NoSuchMethodException e) {
            getLogger().error(".forwardTask(): Error (NoSuchMethodException) ->{}", e.getMessage());
            TaskRouterResponsePacket response = new TaskRouterResponsePacket();
            response.setRoutedTaskId(task.getTaskId());
            response.setSuccessorTaskId(null);
            response.setRoutingActivityInstant(Instant.now());
            response.setParticipantStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_IN_ERROR);
            return(response);
        } catch (Exception e) {
            getLogger().error(".forwardTask: Error (GeneralException) ->{}", e.getMessage());
            TaskRouterResponsePacket response = new TaskRouterResponsePacket();
            response.setRoutedTaskId(task.getTaskId());
            response.setSuccessorTaskId(null);
            response.setRoutingActivityInstant(Instant.now());
            response.setParticipantStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_IN_ERROR);
            return(response);
        }
    }

    public TaskRouterStatusPacket getStatus(String targetParticipantName){
        getLogger().debug(".getStatus(): Entry, targetParticipantName->{}, task->{}", targetParticipantName);
        Address targetAddress = resolveTargetAddressForTaskReceiver(targetParticipantName);
        if(targetAddress == null){
            getLogger().error(".getStatus(): Cannot find candidate service address: targetParticipantName->{}", targetParticipantName);
            getMetricsAgent().sendITOpsNotification("Error: Cannot find candidate " + targetParticipantName);
            getProcessingPlantMetricsAgent().sendITOpsNotification("Error: Cannot find candidate " + targetParticipantName);
            return(null);
        }
        try {
            String sourceName = getProcessingPlant().getMeAsASoftwareComponent().getParticipantName();
            Object objectSet[] = new Object[]{sourceName};
            Class classSet[] = createClassSet(objectSet);
            RequestOptions requestOptions = new RequestOptions( ResponseMode.GET_FIRST, getRPCUnicastTimeout());
            TaskRouterStatusPacket response = null;
            synchronized(getIPCChannelLock()) {
                response = getRPCDispatcher().callRemoteMethod(targetAddress, "getStatusHandler", objectSet, classSet, requestOptions);
            }
            getLogger().trace(".getStatus(): Message.SEND.RESPONSE: response->{}", response);
            if(getLogger().isInfoEnabled()){
                getLogger().info(".getStatus(): Forwarding of Task Complete");
            }
            return(response);
        } catch (NoSuchMethodException e) {
            getLogger().error(".getStatus(): Error (NoSuchMethodException) ->{}", e.getMessage());
            TaskRouterStatusPacket response = new TaskRouterStatusPacket();
            response.setLocalCacheSize(-1);
            response.setActivityInstant(Instant.now());
            response.setParticipantStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_IN_ERROR);
            return(response);
        } catch (Exception e) {
            getLogger().error(".getStatus: Error (GeneralException) ->{}", e.getMessage());
            TaskRouterStatusPacket response = new TaskRouterStatusPacket();
            response.setLocalCacheSize(-1);
            response.setActivityInstant(Instant.now());
            response.setParticipantStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_IN_ERROR);
            return(response);
        }
    }
}
