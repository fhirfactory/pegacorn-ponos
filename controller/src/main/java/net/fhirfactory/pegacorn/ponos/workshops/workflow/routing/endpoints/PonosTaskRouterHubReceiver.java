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

import net.fhirfactory.pegacorn.core.interfaces.oam.metrics.PetasosTaskCacheStatusInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointFunctionTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointTopologyTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipantControlStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskSequenceNumber;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.factories.TaskIdTypeFactory;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.reason.valuesets.TaskReasonTypeEnum;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.edge.jgroups.JGroupsIntegrationPointSummary;
import net.fhirfactory.pegacorn.core.model.topology.mode.NetworkSecurityZoneEnum;
import net.fhirfactory.pegacorn.internals.fhir.r4.resources.endpoint.valuesets.EndpointPayloadTypeEnum;
import net.fhirfactory.pegacorn.petasos.core.tasks.caches.TaskSequenceNumberGenerator;
import net.fhirfactory.pegacorn.platform.edge.model.ipc.packets.InterProcessingPlantHandoverPacket;
import net.fhirfactory.pegacorn.platform.edge.model.ipc.packets.InterProcessingPlantHandoverResponsePacket;
import net.fhirfactory.pegacorn.platform.edge.model.router.TaskRouterResponsePacket;
import net.fhirfactory.pegacorn.platform.edge.model.router.TaskRouterStatusPacket;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.routing.endpoints.common.PonosTaskRouterHubCommon;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.routing.queing.TaskQueueServices;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.lang3.SerializationUtils;
import org.jgroups.Address;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class PonosTaskRouterHubReceiver extends PonosTaskRouterHubCommon {
    private static final Logger LOG = LoggerFactory.getLogger(PonosTaskRouterHubReceiver.class);

    @Produce
    private ProducerTemplate camelProducer;

    @Inject
    private TaskIdTypeFactory taskIdFactory;

    @Inject
    private TaskQueueServices taskQueueServices;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private PetasosTaskCacheStatusInterface taskCacheStatus;

    @Inject
    private TaskSequenceNumberGenerator taskSequenceNumberGenerator;

    //
    // Constructor(s)
    //

    public PonosTaskRouterHubReceiver(){
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

    protected TaskIdTypeFactory getTaskIdFactory() {
        return taskIdFactory;
    }

    public TaskQueueServices getTaskQueueServices(){
        return(taskQueueServices);
    }

    protected TaskSequenceNumberGenerator getTaskSequenceNumberGenerator(){
        return(taskSequenceNumberGenerator);
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
        return (PetasosEndpointFunctionTypeEnum.PETASOS_TASK_ROUTING_RECEIVER_ENDPOINT);
    }

    @Override
    protected EndpointPayloadTypeEnum specifyPetasosEndpointPayloadType() {
        return (EndpointPayloadTypeEnum.ENDPOINT_PAYLOAD_INTERNAL_TASK_ROUTING_RECEIVER);
    }

    @Override
    protected String specifyJGroupsChannelName() {
        getLogger().debug(".specifyJGroupsChannelName(): Entry");
        String originalChannelName = getJGroupsIntegrationPoint().getChannelName();
        String newChannelName = originalChannelName;
        if(!originalChannelName.contains(PetasosEndpointFunctionTypeEnum.PETASOS_TASK_ROUTING_RECEIVER_HUB_ENDPOINT.getDisplayName())) {
            newChannelName = originalChannelName.replace(PetasosEndpointFunctionTypeEnum.PETASOS_TASK_ROUTING_RECEIVER_ENDPOINT.getDisplayName(), PetasosEndpointFunctionTypeEnum.PETASOS_TASK_ROUTING_RECEIVER_HUB_ENDPOINT.getDisplayName());
        }
        getJGroupsIntegrationPoint().setChannelName(newChannelName);
        getLogger().debug(".specifyJGroupsChannelName(): Exit, channelName->{}", newChannelName);
        return(newChannelName);
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
    // Receive Message
    //

    public InterProcessingPlantHandoverResponsePacket receiveIPCMessage(InterProcessingPlantHandoverPacket handoverPacket){
        getLogger().debug(".receiveIPCMessage(): Entry, handoverPacket->{}",handoverPacket);
        InterProcessingPlantHandoverResponsePacket response = injectMessageIntoRoute(handoverPacket);
        getMetricsAgent().incrementInternalReceivedMessageCount();
        getLogger().debug(".receiveIPCMessage(): Exit, response->{}",response);
        return(response);
    }

    //
    // Message Senders
    //

    public TaskRouterResponsePacket receiveTask(String sourceName, PetasosActionableTask task){
        getLogger().debug(".receiveTask(): Entry, sourceName->{}, task->{}", sourceName, task);

        getLogger().trace(".receiveTask(): [Create Routing Task] Start");
        PetasosActionableTask routingTask = SerializationUtils.clone(task);
        TaskIdType routingTaskId = null;
        try {
            routingTaskId = getTaskIdFactory().newRoutingTaskId(getSubsystemParticipantName(), routingTask.getTaskId());
        } catch(Exception ex){
            getLogger().debug(".receiveTask(): [Create Routing Task] Could create new TaskId from old one..., issue->", ex);
        }
        if(routingTaskId == null){
            routingTaskId = getTaskIdFactory().newTaskId(getSubsystemParticipantName(), TaskReasonTypeEnum.TASK_REASON_TASK_ROUTING, task.getTaskWorkItem().getPayloadTopicID().getContentDescriptor());
        }
        TaskSequenceNumber taskSequenceNumber = getTaskSequenceNumberGenerator().generateNewSequenceNumber();
        routingTaskId.setTaskSequenceNumber(taskSequenceNumber);
        routingTask.setTaskId(routingTaskId);
        getLogger().trace(".receiveTask(): [Create Routing Task] Finish, routingTask->{}", routingTask);

        getLogger().warn(".receiveTask(): [Queue Task] Start");
        boolean isOK = getTaskQueueServices().queueTask(routingTask);
        getLogger().warn(".receiveTask(): [Queue Task] Finish, isOK->{}", isOK);

        getLogger().trace(".receiveTask(): [Generate Response] Start");
        TaskRouterResponsePacket response = new TaskRouterResponsePacket();
        response.setLocalCacheSize(taskCacheStatus.getActionableTaskCacheSize());
        response.setParticipantStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED);
        response.setRoutedTaskId(routingTaskId);
        response.setRoutingActivityInstant(Instant.now());
        response.setSuccessorTaskId(routingTaskId);
        getLogger().trace(".receiveTask(): [Generate Response] Finish");

        getMetricsAgent().incrementInternalReceivedMessageCount();

        getLogger().debug(".receiveTask(): Exit, response->{}",response);
        return(response);
    }

    public TaskRouterStatusPacket getStatusHandler(String sourceName){
        getLogger().debug(".receiveTask(): Entry, sourceName->{}", sourceName);

        getLogger().trace(".getStatusHandler(): [Generate Response] Start");
        TaskRouterStatusPacket response = new TaskRouterStatusPacket();
        response.setLocalCacheSize(taskCacheStatus.getActionableTaskCacheSize());
        response.setParticipantStatus(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED);
        response.setActivityInstant(Instant.now());
        getLogger().trace(".getStatusHandler(): [Generate Response] Finish");

        getLogger().debug(".receiveTask(): Exit, response->{}",response);
        return(response);
    }
}
