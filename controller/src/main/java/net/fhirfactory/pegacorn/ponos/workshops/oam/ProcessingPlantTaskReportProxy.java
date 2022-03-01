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
package net.fhirfactory.pegacorn.ponos.workshops.oam;

import net.fhirfactory.pegacorn.core.interfaces.oam.tasks.PetasosITOpsTaskReportingAgentInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantRoleSupportInterface;
import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.oam.notifications.PetasosComponentITOpsNotification;
import net.fhirfactory.pegacorn.core.model.petasos.oam.topology.valuesets.PetasosMonitoredComponentTypeEnum;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class ProcessingPlantTaskReportProxy {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingPlantTaskReportProxy.class);

    @Inject
    private PetasosITOpsTaskReportingAgentInterface taskReportingAgent;

    @Inject
    private ProcessingPlantRoleSupportInterface processingPlantFunction;

    //
    // Constructor(s)
    //

    public ProcessingPlantTaskReportProxy(){
    }

    //
    // Getters and Setters
    //

    protected PetasosITOpsTaskReportingAgentInterface getTaskReportingAgent(){
        return(this.taskReportingAgent);
    }

    protected ProcessingPlantRoleSupportInterface getProcessingPlantFunction(){
        return(this.processingPlantFunction);
    }

    protected Logger getLogger(){
        return(LOG);
    }

    //
    // Business Methods
    //

    public void sendITOpsTaskReport(String participantName, ComponentIdType participantComponentId, String content){
        getLogger().debug(".sendITOpsTaskReport(): Entry");
        try {
            PetasosComponentITOpsNotification notification = new PetasosComponentITOpsNotification();
            notification.setContent(content);
            notification.setComponentType(PetasosMonitoredComponentTypeEnum.PETASOS_MONITORED_COMPONENT_PROCESSING_PLANT);
            notification.setComponentId(participantComponentId);
            notification.setParticipantName(participantName);

            taskReportingAgent.sendTaskReport(notification);
        } catch (Exception generalException) {
            getLogger().warn(".sendITOpsTaskReport(): Problem Sending ITOps TaskReport, message->{}, stackTrace->{}", ExceptionUtils.getMessage(generalException), ExceptionUtils.getStackTrace(generalException));
        }
        getLogger().info(".sendITOpsTaskReport(): Exit");
    }

    public void sendITOpsEndpointOnlyTaskReport(String participantName, ComponentIdType participantComponentId, String content){
        getLogger().debug(".sendITOpsTaskReport(): Entry");
        try {
            PetasosComponentITOpsNotification notification = new PetasosComponentITOpsNotification();
            notification.setContent(content);
            notification.setComponentType(PetasosMonitoredComponentTypeEnum.PETASOS_MONITORED_COMPONENT_ENDPOINT);
            notification.setComponentId(participantComponentId);
            notification.setParticipantName(participantName);

            taskReportingAgent.sendTaskReport(notification);
        } catch (Exception generalException) {
            getLogger().warn(".sendITOpsTaskReport(): Problem Sending ITOps TaskReport, message->{}, stackTrace->{}", ExceptionUtils.getMessage(generalException), ExceptionUtils.getStackTrace(generalException));
        }
        getLogger().info(".sendITOpsTaskReport(): Exit");
    }

    public void sendITOpsEndpointOnlyTaskReport(String participantName, ComponentIdType participantComponentId, String content, String formattedContent){
        getLogger().debug(".sendITOpsTaskReport(): Entry");
        try {
            PetasosComponentITOpsNotification notification = new PetasosComponentITOpsNotification();
            notification.setContent(content);
            notification.setComponentType(PetasosMonitoredComponentTypeEnum.PETASOS_MONITORED_COMPONENT_ENDPOINT);
            notification.setComponentId(participantComponentId);
            notification.setParticipantName(participantName);
            notification.setFormattedContent(formattedContent);

            taskReportingAgent.sendTaskReport(notification);
        } catch (Exception generalException) {
            getLogger().warn(".sendITOpsTaskReport(): Problem Sending ITOps TaskReport, message->{}, stackTrace->{}", ExceptionUtils.getMessage(generalException), ExceptionUtils.getStackTrace(generalException));
        }
        getLogger().info(".sendITOpsTaskReport(): Exit");
    }
}
