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
package net.fhirfactory.pegacorn.ponos.reporitng.factories;

import net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants;
import net.fhirfactory.pegacorn.core.model.petasos.oam.notifications.ITOpsNotification;
import net.fhirfactory.pegacorn.core.model.petasos.oam.notifications.ITOpsNotificationContent;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.ActionableTaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityElementType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityType;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayload;
import net.fhirfactory.pegacorn.deployment.names.subsystems.SubsystemNames;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.GeneralTaskMetadataExtractor;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.HL7v2xTaskMetadataExtractor;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.PonosPetasosActionableTaskCacheServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class AggregateTaskReportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateTaskReportFactory.class);

    private DateTimeFormatter timeFormatter;

    @Inject
    private HL7v2xTaskMetadataExtractor hl7V2XTaskMetadataExtractor;

    @Inject
    private GeneralTaskMetadataExtractor generalTaskMetadataExtractor;

    @Inject
    private PonosPetasosActionableTaskCacheServices taskCacheServices;

    @Inject
    private SubsystemNames subsystemNames;

    @Inject
    private AggregateTaskReportContentExtractor taskReportContentExtractor;

    @Inject
    private HL7v2xReportContentBuilder hl7v2xReportContentBuilder;

    //
    // Constructor(s)
    //

    public AggregateTaskReportFactory(){
        timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of(PetasosPropertyConstants.DEFAULT_TIMEZONE));
    }

    //
    // Business Methods
    //


    public ITOpsNotification endOfChainReport(PetasosActionableTask lastTask) {
        getLogger().debug(".endOfChainReport(): Entry, lastTask->{}", lastTask);

        TaskTraceabilityType taskTraceability = lastTask.getTaskTraceability();
        //
        // Get the 1st
        TaskTraceabilityElementType firstTaskTraceabilityElement = taskTraceability.getTaskJourney().get(0);
        TaskIdType firstTaskId = firstTaskTraceabilityElement.getActionableTaskId();
        PetasosActionableTask firstTask = getTaskCacheServices().getPetasosActionableTask(firstTaskId);

        Instant startTime = null;
        Instant lastTaskStartTime = null;
        Instant lastTaskFinishTime = null;
        String startTimeString = null;
        String lastTaskStartTimeString = null;
        String lastTaskFinishTimeString = null;

        if(firstTask.hasTaskFulfillment()){
            if(firstTask.getTaskFulfillment().hasStartInstant()){
                startTime = firstTask.getTaskFulfillment().getStartInstant();
            }
        }
        if(startTime == null){
            startTimeString = getTimeFormatter().format(Instant.now());
        } else {
            startTimeString = getTimeFormatter().format(startTime);
        }

        if(lastTask.hasTaskFulfillment()) {
            if (lastTask.getTaskFulfillment().hasStartInstant()) {
                lastTaskStartTime = lastTask.getTaskFulfillment().getStartInstant();
            }
        }
        if(lastTaskStartTime == null) {
            lastTaskStartTimeString = "N/A";
        } else {
            lastTaskStartTimeString = getTimeFormatter().format(lastTaskStartTime);
        }

        if(lastTask.hasTaskFulfillment()) {
            if (lastTask.getTaskFulfillment().hasFinishInstant()) {
                lastTaskFinishTime = lastTask.getTaskFulfillment().getFinishInstant();
            }
        }
        if(lastTaskFinishTime == null) {
            lastTaskFinishTimeString = "N/A";
        } else {
            lastTaskFinishTimeString = getTimeFormatter().format(lastTaskStartTime);
        }

        String ingresPoint = getTaskReportContentExtractor().getIngresName(firstTask);
        String egressPoint = getTaskReportContentExtractor().getEgressName(lastTask);

        StringBuilder reportBuilder = new StringBuilder();
        StringBuilder formattedReportBuilder = new StringBuilder();

         reportBuilder.append("TriggerTask: Task Id --> " + firstTask.getTaskId().getLocalId() + "\n");
        reportBuilder.append("TriggerTask: Participant --> " + ingresPoint + "\n");
        reportBuilder.append("EndTask: Task Id --> " + lastTask.getTaskId().getLocalId() + "\n");
        reportBuilder.append("EndTask: Participant --> " + egressPoint + "\n");

        formattedReportBuilder.append("<table>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<th> Ingres </th><th>" + ingresPoint + "</th><th>"+ startTimeString + "</th>");
        formattedReportBuilder.append("</tr>");

        Integer journeySize = taskTraceability.getTaskJourney().size();
        TaskIdType previousTaskId = null;
        int count = 0;
        for (Integer counter = 0; counter < journeySize; counter += 1) {
            TaskIdType currentTaskId = taskTraceability.getTaskJourney().get(counter).getActionableTaskId();
            PetasosActionableTask currentTask = getTaskCacheServices().getPetasosActionableTask(currentTaskId);
            boolean addToReport = false;
            if (previousTaskId == null) {
                addToReport = true;
            } else {
                if (!currentTask.getTaskId().equals(previousTaskId)) {
                    addToReport = true;
                }
            }
            if (currentTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getSubsystemParticipantName().equals(subsystemNames.getITOpsIMParticipantName())) {
                addToReport = false;
            }
            if (addToReport) {
                newIndividualTaskSummaryReport(count, currentTask, formattedReportBuilder, reportBuilder);
                count += 1;
            }
            previousTaskId = currentTask.getTaskId();
        }

        String egressResponseContent = getTaskReportContentExtractor().getOutcomeContent(lastTask);

        String egressOutcomeStatus = getTaskReportContentExtractor().getOutcomeStatusDetail(lastTask);

        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<th>Egress</th><th>"+egressPoint+ "</th><th>"+ lastTaskStartTimeString + "</th>");
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<th>Outcome</th><th>"+egressOutcomeStatus+ "</th><th>"+ lastTaskFinishTimeString + "</th>");
        formattedReportBuilder.append("</tr>");

        String reportString = reportBuilder.toString();
        String formattedReportString = formattedReportBuilder.toString();

        ITOpsNotification notification = new ITOpsNotification();
        notification.setContent(reportString);
        notification.setFormattedContent(formattedReportString);

        getLogger().debug(".endOfChainReport(): Exit, notification->{}", notification);

        return (notification);
    }

    public void newIndividualTaskSummaryReport(Integer step, PetasosActionableTask actionableTask,  StringBuilder formattedReportBuilder, StringBuilder unformattedReport) {
        getLogger().debug(".newTaskSummaryReport(): Entry");

        //
        // Derive the Key Information
        String taskId = actionableTask.getTaskId().getId();
        String startTime = null;
        if (actionableTask.getTaskFulfillment().hasStartInstant()) {
            startTime = getTimeFormatter().format(actionableTask.getTaskFulfillment().getStartInstant());
        } else {
            startTime = "-";
        }
        String finishTime = null;
        if (actionableTask.getTaskFulfillment().hasFinishedDate()) {
            finishTime = getTimeFormatter().format(actionableTask.getTaskFulfillment().getFinishInstant());
        } else {
            finishTime = "-";
        }
        String fulfillerComponentName = "Not Available";
        String fulfillerComponentId = "Not Available";
        String fulfillerProcessingPlant = "N/A";
        if (actionableTask.getTaskFulfillment().hasFulfillerWorkUnitProcessor()) {
            fulfillerComponentName = actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getParticipantDisplayName();
            fulfillerProcessingPlant = actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getSubsystemParticipantName();
            fulfillerComponentId = actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getComponentID().getId();
        } else {
            getLogger().warn(".newTaskSummaryReport(): No Task Fulfiller Component Defined on Task->{}", actionableTask.getTaskId());
        }
        ActionableTaskOutcomeStatusEnum outcomeStatus = actionableTask.getTaskOutcomeStatus().getOutcomeStatus();
        if (outcomeStatus == null) {
            outcomeStatus = ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_UNKNOWN;
        }
        String taskOutcomeStatus = outcomeStatus.getDisplayName();

        List<String> metadataHeader = null;
        List<String> metadataBody = null;
        if (getHL7v2MetadataFactory().isHL7V2Payload(actionableTask.getTaskWorkItem().getIngresContent())) {
            metadataHeader = getHL7v2MetadataFactory().getHL7v2MetadataHeaderInfo(actionableTask.getTaskWorkItem().getIngresContent());
            if (!actionableTask.getTaskWorkItem().getIngresContent().getPayloadManifest().hasContainerDescriptor()) {
                metadataBody = getHL7v2MetadataFactory().extractMetadataFromHL7v2xMessage(actionableTask.getTaskWorkItem().getIngresContent().getPayload());
            } else {
                metadataBody = new ArrayList<>();
                metadataBody.add("Metadata Not Available");
            }
        } else {
            metadataHeader = new ArrayList<>();
            metadataBody = new ArrayList<>();
        }

        int outputPayloadCounter = 0;
        Map<Integer, List<String>> outputHeaders = new HashMap<>();
        Map<Integer, List<String>> outputMetadata = new HashMap<>();

        for (UoWPayload currentEgressPayload : actionableTask.getTaskWorkItem().getEgressContent().getPayloadElements()) {
            if (getHL7v2MetadataFactory().isHL7V2Payload(currentEgressPayload)) {
                List<String> currentHeaderList = getHL7v2MetadataFactory().getHL7v2MetadataHeaderInfo(currentEgressPayload);
                outputHeaders.put(outputPayloadCounter, currentHeaderList);
                if (!currentEgressPayload.getPayloadManifest().hasContainerDescriptor()) {
                    outputMetadata.put(outputPayloadCounter, getHL7v2MetadataFactory().extractMetadataFromHL7v2xMessage(currentEgressPayload.getPayload()));
                } else {
                    List<String> notOutputMetadataList = new ArrayList<>();
                    notOutputMetadataList.add("Metadata Not Available");
                    outputMetadata.put(outputPayloadCounter, notOutputMetadataList);
                }
            } else {
                List<String> currentHeaderList = getGeneralTaskMetadataExtractor().getGeneralHeaderDetail(currentEgressPayload);
                outputHeaders.put(outputPayloadCounter, currentHeaderList);
                List<String> notOutputMetadataList = new ArrayList<>();
                notOutputMetadataList.add("Metadata Not Available");
                outputMetadata.put(outputPayloadCounter, notOutputMetadataList);
            }
            outputPayloadCounter += 1;
        }

        //
        // Build the Standard Text Body

        unformattedReport.append("[" + step.toString() + "]:: -------------------- Task Report ------------------ :: \n");
        unformattedReport.append("[" + step.toString() + "]:: Task Id --> " + taskId + "\n");
        unformattedReport.append("[" + step.toString() + "]:: Start Time --> " + startTime + ":: Finish Time --> " + finishTime + "\n");
        unformattedReport.append("[" + step.toString() + "]:: Task Outcome --> " + taskOutcomeStatus + "\n");
        unformattedReport.append("[" + step.toString() + "]:: Component Name --> " + fulfillerComponentName + " \n");
        unformattedReport.append("[" + step.toString() + "]:: Component Id --> " + fulfillerComponentId + "\n");
        unformattedReport.append("[" + step.toString() + "]:: --- Input ---\n");
        for (String currentMetadataHeaderLine : metadataHeader) {
            unformattedReport.append(":: " + currentMetadataHeaderLine + "\n");
        }
        for (String currentMetadataBodyLine : metadataBody) {
            unformattedReport.append(currentMetadataBodyLine + "\n");
        }

        unformattedReport.append("[" + step.toString() + "]:: --------------------------------------------------- :: \n");

        //
        // Build the Formatted Text Body

        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<td>"+step+"</td><td>");
        formattedReportBuilder.append("Activity: " + fulfillerProcessingPlant + "(" + fulfillerComponentName + ")");
        formattedReportBuilder.append("</td><td>");
        formattedReportBuilder.append("Outcome: " + taskOutcomeStatus );
        formattedReportBuilder.append("</td>");
        formattedReportBuilder.append("</tr>");
    }

    public ITOpsNotificationContent newEndpointOnlyTaskReport(PetasosActionableTask lastTask){

        ITOpsNotificationContent notificationContent = hl7v2xReportContentBuilder.newEndpointOnlyActivityReport(lastTask, true);

        getLogger().debug(".newEndpointOnlyTaskReport(): Exit, taskNotification->{}", notificationContent);
        return(notificationContent);
    }

    //
    // Getters (and Setters)
    //

    protected PonosPetasosActionableTaskCacheServices getTaskCacheServices(){
        return(taskCacheServices);
    }

    protected Logger getLogger(){
        return(LOG);
    }

    protected DateTimeFormatter getTimeFormatter(){
        return(this.timeFormatter);
    }

    protected HL7v2xTaskMetadataExtractor getHL7v2MetadataFactory(){
        return(hl7V2XTaskMetadataExtractor);
    }

    protected GeneralTaskMetadataExtractor getGeneralTaskMetadataExtractor(){
        return(generalTaskMetadataExtractor);
    }

    protected AggregateTaskReportContentExtractor getTaskReportContentExtractor(){
        return(this.taskReportContentExtractor);
    }
}
