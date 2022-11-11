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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.factories;

import net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants;
import net.fhirfactory.pegacorn.core.model.petasos.oam.notifications.ITOpsNotificationContent;
import net.fhirfactory.pegacorn.core.model.petasos.oam.notifications.PetasosComponentITOpsNotification;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.ActionableTaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityElementType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityType;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayload;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWProcessingOutcomeEnum;
import net.fhirfactory.pegacorn.deployment.names.subsystems.SubsystemNames;
import net.fhirfactory.pegacorn.internals.hl7v2.helpers.UltraDefensivePipeParser;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.GeneralTaskMetadataExtractor;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.HL7v2xTaskMetadataExtractor;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.TaskLoggingCacheServices;
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
import net.fhirfactory.pegacorn.core.model.topology.nodes.WorkUnitProcessorSoftwareComponent;
import org.apache.commons.lang3.StringUtils;

@ApplicationScoped
public class AggregateTaskReportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateTaskReportFactory.class);

    private DateTimeFormatter timeFormatter;

    @Inject
    private HL7v2xTaskMetadataExtractor hl7V2XTaskMetadataExtractor;

    @Inject
    private GeneralTaskMetadataExtractor generalTaskMetadataExtractor;

    @Inject
    private TaskLoggingCacheServices taskCacheServices;

    @Inject
    private SubsystemNames subsystemNames;

    @Inject
    private EndpointInformationExtractor endpointInfoExtrator;

    @Inject
    private UltraDefensivePipeParser defensivePipeParser;

    //
    // Constructor(s)
    //

    public AggregateTaskReportFactory(){
        timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of(PetasosPropertyConstants.DEFAULT_TIMEZONE));
    }

    //
    // Business Methods
    //

    public ITOpsNotificationContent endOfChainReport(PetasosActionableTask lastTask){
        getLogger().debug(".endOfChainReport(): Entry, lastTask->{}", lastTask);

        TaskTraceabilityType taskTraceability = lastTask.getTaskTraceability();
        //
        // Get the 1st
        TaskTraceabilityElementType firstTaskTraceabilityElement = taskTraceability.getTaskJourney().get(0);
        TaskIdType firstTaskId = firstTaskTraceabilityElement.getActionableTaskId();
        PetasosActionableTask firstTask = getTaskCacheServices().getPetasosActionableTask(firstTaskId);

        String ingresEndpointParticipantName = null;
        if(firstTask.getTaskFulfillment().getFulfillerWorkUnitProcessor() instanceof WorkUnitProcessorSoftwareComponent){
            WorkUnitProcessorSoftwareComponent ingresWUP = (WorkUnitProcessorSoftwareComponent)firstTask.getTaskFulfillment().getFulfillerWorkUnitProcessor();
            ingresEndpointParticipantName = getEndpointInfoExtrator().getEndpointParticipantName(ingresWUP, true);
        }
        String egressEndpointParticipantName = null;
        if(lastTask.getTaskFulfillment().getFulfillerWorkUnitProcessor() instanceof WorkUnitProcessorSoftwareComponent){
            WorkUnitProcessorSoftwareComponent egressWUP = (WorkUnitProcessorSoftwareComponent)lastTask.getTaskFulfillment().getFulfillerWorkUnitProcessor();
            egressEndpointParticipantName = getEndpointInfoExtrator().getEndpointParticipantName(egressWUP, false);
        }

        String ingresName = null;
        if(StringUtils.isEmpty(ingresEndpointParticipantName)){
            ingresName = firstTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getParticipantName();
        } else {
            ingresName = ingresEndpointParticipantName;
        }

        String egressName = null;
        if(StringUtils.isEmpty(egressEndpointParticipantName)){
            egressName = lastTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getParticipantName();
        } else {
            egressName = egressEndpointParticipantName;
        }

        StringBuilder reportBuilder = new StringBuilder();
        StringBuilder formattedReportBuilder = new StringBuilder();

        reportBuilder.append("--------------------\n");
        reportBuilder.append("---(FTJ) Task Journey ---\n");
        reportBuilder.append("TriggerTask: Task Id --> " + firstTask.getTaskId().getLocalId() +"\n");
        reportBuilder.append("Ingres --> " +ingresName + "\n");
        reportBuilder.append("EndTask: Task Id --> " + lastTask.getTaskId().getLocalId() + "\n");
        reportBuilder.append("Egress --> " + egressName + "\n");

        formattedReportBuilder.append("<b>Full Task Journey Report: " + getTimeFormatter().format(Instant.now())+"</b>");
        formattedReportBuilder.append("<table>");
        formattedReportBuilder.append("<th>Ingres </th>");
        formattedReportBuilder.append("<th>"+ingresName+"</th>");
        formattedReportBuilder.append("<th></th>");
        formattedReportBuilder.append("<th>"+getTimeFormatter().format(firstTask.getTaskFulfillment().getStartInstant())+"</th>");
        formattedReportBuilder.append("</tr>");


        Integer journeySize = taskTraceability.getTaskJourney().size();
        TaskIdType previousTaskId = null;
        Integer stepCount = 0 ;
        for(Integer counter = 0; counter < journeySize; counter += 1 ){
            TaskIdType currentTaskId = taskTraceability.getTaskJourney().get(counter).getActionableTaskId();
            PetasosActionableTask currentTask = getTaskCacheServices().getPetasosActionableTask(currentTaskId);
            boolean addToReport = false;
            if(previousTaskId == null){
                addToReport = true;
            } else {
                if(!currentTask.getTaskId().equals(previousTaskId)){
                    addToReport = true;
                }
            }
            if(currentTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getSubsystemParticipantName().equals(subsystemNames.getITOpsIMParticipantName())){
                addToReport = false;
            }
            if(addToReport) {
                getLogger().debug(".endOfChainReport(): Adding Individual Task Summary Report, stepCount->{}", stepCount);
                ITOpsNotificationContent currentSummary = newIndividualTaskSummaryReport(stepCount, currentTask);
                reportBuilder.append(currentSummary.getContent());
                formattedReportBuilder.append(currentSummary.getFormattedContent());
                stepCount += 1;
            }
            previousTaskId = currentTask.getTaskId();
        }
        //ITOpsNotificationContent finalSummary = newIndividualTaskSummaryReport(journeySize, lastTask);
        //reportBuilder.append(finalSummary.getContent());
        //formattedReportBuilder.append(finalSummary.getFormattedContent());

        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<th>Egress </th>");
        formattedReportBuilder.append("<th>"+egressName+"</th>");
        formattedReportBuilder.append("<th></th>");
        formattedReportBuilder.append("<th>"+getTimeFormatter().format(lastTask.getTaskFulfillment().getFinishInstant())+"</th>");
        formattedReportBuilder.append("</tr>");

        formattedReportBuilder.append("</table>");

        ITOpsNotificationContent report = new ITOpsNotificationContent();
        report.setContent(reportBuilder.toString());
        report.setFormattedContent(formattedReportBuilder.toString());



        getLogger().debug(".endOfChainReport(): Exit, report->{}", report);

        return(report);
    }

    public ITOpsNotificationContent newIndividualTaskSummaryReport(Integer step, PetasosActionableTask actionableTask) {
        getLogger().debug(".newTaskSummaryReport(): Entry, step->{}, actionableTask->{}", step, actionableTask);

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
        if (actionableTask.getTaskFulfillment().hasFulfillerWorkUnitProcessor()) {
            fulfillerComponentName = actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getParticipantName();
            fulfillerComponentId = actionableTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getComponentID().getId();
        } else {
            getLogger().debug(".newTaskSummaryReport(): No Task Fulfiller Component Defined on Task->{}", actionableTask.getTaskId());
        }
        ActionableTaskOutcomeStatusEnum outcomeStatus = actionableTask.getTaskOutcomeStatus().getOutcomeStatus();
        if (outcomeStatus == null) {
            outcomeStatus = ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_UNKNOWN;
        }
        String taskOutcomeStatus = null;
        if(actionableTask.getTaskWorkItem() != null) {
            UoWProcessingOutcomeEnum processingOutcome = actionableTask.getTaskWorkItem().getProcessingOutcome();
            switch(processingOutcome){
                case UOW_OUTCOME_FILTERED:
                case UOW_OUTCOME_DISCARD: {
                    taskOutcomeStatus = outcomeStatus.getDisplayName() + " (" + processingOutcome.getToken() + ")";
                    break;
                }
                default:
                    break;
            }
        }

        if(StringUtils.isEmpty(taskOutcomeStatus)) {
            taskOutcomeStatus = outcomeStatus.getDisplayName();
        }

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

        boolean badOutcome = false;
        if(outcomeStatus.equals(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_FINALISED) || outcomeStatus.equals(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_FINISHED)){
            badOutcome = false;
        } else {
            badOutcome = true;
        }
        String badOutcomeReason = null;
        if(badOutcome){
            if(actionableTask.hasTaskWorkItem()){
                if(actionableTask.getTaskWorkItem().hasFailureDescription()){
                    String failureDescription = actionableTask.getTaskWorkItem().getFailureDescription();
                    if(failureDescription.length()> 64){
                        badOutcomeReason = failureDescription.substring(0, 80) + "...";
                    } else {
                        badOutcomeReason = failureDescription;
                    }
                }
            }
        }


        //
        // Build the Standard Text Body
        StringBuilder reportBuilder = new StringBuilder();
        reportBuilder.append("[" + step.toString() + "]:: -------------------- Task Report ------------------ :: \n");
        reportBuilder.append("[" + step.toString() + "]:: Task Id --> " + taskId + "\n");
        reportBuilder.append("[" + step.toString() + "]:: Start Time --> " + startTime + ":: Finish Time --> " + finishTime + "\n");
        reportBuilder.append("[" + step.toString() + "]:: Task Outcome --> " + taskOutcomeStatus + "\n");
        reportBuilder.append("[" + step.toString() + "]:: Component Name --> " + fulfillerComponentName + " \n");
        reportBuilder.append("[" + step.toString() + "]:: Component Id --> " + fulfillerComponentId + "\n");
        reportBuilder.append("[" + step.toString() + "]:: --- Input ---\n");
        for (String currentMetadataHeaderLine : metadataHeader) {
            reportBuilder.append(":: " + currentMetadataHeaderLine + "\n");
        }
        for (String currentMetadataBodyLine : metadataBody) {
            reportBuilder.append(currentMetadataBodyLine + "\n");
        }
        reportBuilder.append("[" + step.toString() + "]:: --------------------------------------------------- :: \n");

        //
        // Build the Formatted Text Body
        StringBuilder formattedReportBuilder = new StringBuilder();
        if(!badOutcome){
            formattedReportBuilder.append("<tr>");
            formattedReportBuilder.append("<td>"+step+"</td>");
            formattedReportBuilder.append("<td>"+fulfillerComponentName+"</td>");
            formattedReportBuilder.append("<td><font color=green>"+taskOutcomeStatus+"</font></td>");
            formattedReportBuilder.append("<td>"+finishTime+"</td>");
            formattedReportBuilder.append("</tr>");
        }
        if(badOutcome){
            formattedReportBuilder.append("<tr>");
            formattedReportBuilder.append("<td>"+step+"</td>");
            formattedReportBuilder.append("<td>"+fulfillerComponentName+"</td>");
            formattedReportBuilder.append("<td><font color=red>"+taskOutcomeStatus+"</font></td>");
            formattedReportBuilder.append("<td>"+finishTime+"</td>");
            formattedReportBuilder.append("</tr>");
            if(StringUtils.isNotEmpty(badOutcomeReason)){
                formattedReportBuilder.append("<tr>");
                formattedReportBuilder.append("<td></td>");
                formattedReportBuilder.append("<td><font color=red>"+badOutcomeReason+"</font></td>");
                formattedReportBuilder.append("<td></td>");
                formattedReportBuilder.append("<td></td>");
                formattedReportBuilder.append("</tr>");
            }
        }

        ITOpsNotificationContent notificationContent = new ITOpsNotificationContent();
        notificationContent.setContent(reportBuilder.toString());
        notificationContent.setFormattedContent(formattedReportBuilder.toString());

        getLogger().debug(".newIndividualTaskSummaryReport(): Exit, notificationContent->{}", notificationContent);
        return(notificationContent);
    }

    public PetasosComponentITOpsNotification newEndpointOnlyTaskReport(PetasosActionableTask lastTask){

        TaskTraceabilityType taskTraceability = lastTask.getTaskTraceability();
        //
        // Get the 1st
        TaskTraceabilityElementType firstTaskTraceabilityElement = taskTraceability.getTaskJourney().get(0);
        TaskIdType firstTaskId = firstTaskTraceabilityElement.getActionableTaskId();
        PetasosActionableTask firstTask = getTaskCacheServices().getPetasosActionableTask(firstTaskId);

        StringBuilder reportBuilder = new StringBuilder();
        StringBuilder formattedReportBuilder = new StringBuilder();

        String ingresEndpointParticipantName = null;
        if(firstTask.getTaskFulfillment().getFulfillerWorkUnitProcessor() instanceof WorkUnitProcessorSoftwareComponent){
            WorkUnitProcessorSoftwareComponent ingresWUP = (WorkUnitProcessorSoftwareComponent)firstTask.getTaskFulfillment().getFulfillerWorkUnitProcessor();
            ingresEndpointParticipantName = getEndpointInfoExtrator().getEndpointParticipantName(ingresWUP, true);
        }
        String egressEndpointParticipantName = null;
        if(lastTask.getTaskFulfillment().getFulfillerWorkUnitProcessor() instanceof WorkUnitProcessorSoftwareComponent){
            WorkUnitProcessorSoftwareComponent egressWUP = (WorkUnitProcessorSoftwareComponent)lastTask.getTaskFulfillment().getFulfillerWorkUnitProcessor();
            egressEndpointParticipantName = getEndpointInfoExtrator().getEndpointParticipantName(egressWUP, false);
        }

        String ingresName = null;
        if(StringUtils.isEmpty(ingresEndpointParticipantName)){
            ingresName = firstTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getParticipantName();
        } else {
            ingresName = ingresEndpointParticipantName;
        }

        String egressName = null;
        if(StringUtils.isEmpty(egressEndpointParticipantName)){
            egressName = lastTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getParticipantName();
        } else {
            egressName = egressEndpointParticipantName;
        }

        reportBuilder.append("-EOTR-----------------------------------------------------------\n");
        reportBuilder.append("Ingres --> " + ingresName + "\n");
        reportBuilder.append("Egress --> " + egressName + "\n");

        formattedReportBuilder.append("<table>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<th> EOTR </th><th>"+getTimeFormatter().format(Instant.now())+"</th>");
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<td> Ingres </td><td>"+ ingresName + "</td>");
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<td> Egress </td><td>"+ egressName + "</td>");
        formattedReportBuilder.append("</tr>");


        List<String> metadataHeader = null;
        String msh= null;
        String pid = null;
        if (getHL7v2MetadataFactory().isHL7V2Payload(firstTask.getTaskWorkItem().getIngresContent())) {
            metadataHeader = getHL7v2MetadataFactory().getHL7v2MetadataHeaderInfo(firstTask.getTaskWorkItem().getIngresContent());
            try {
                msh = getHL7v2MetadataFactory().getMSH(firstTask.getTaskWorkItem().getIngresContent().getPayload());
                pid = getHL7v2MetadataFactory().getPID(firstTask.getTaskWorkItem().getIngresContent().getPayload());
            } catch (Exception ex){
                getLogger().debug(".newEndpointOnlyTaskReport(): Cannot decode message->{}", firstTask.getTaskId());
            }
        } else {
            metadataHeader = new ArrayList<>();
        }

        for (String currentMetadataHeaderLine : metadataHeader) {
            reportBuilder.append(":: " + currentMetadataHeaderLine + "\n");
        }

        ActionableTaskOutcomeStatusEnum outcomeStatus = lastTask.getTaskOutcomeStatus().getOutcomeStatus();
        boolean badOutcome = false;

        if(outcomeStatus.equals(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_FINALISED) || outcomeStatus.equals(ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_FINISHED)){
            badOutcome = false;
        } else {
            badOutcome = true;
        }

        if (outcomeStatus == null) {
            outcomeStatus = ActionableTaskOutcomeStatusEnum.ACTIONABLE_TASK_OUTCOME_STATUS_UNKNOWN;
        }

        String taskOutcomeStatus = null;

        if(lastTask.getTaskWorkItem() != null) {
            UoWProcessingOutcomeEnum processingOutcome = lastTask.getTaskWorkItem().getProcessingOutcome();
            switch(processingOutcome){
                case UOW_OUTCOME_FILTERED:
                case UOW_OUTCOME_DISCARD: {
                    taskOutcomeStatus = outcomeStatus.getDisplayName() + " (" + processingOutcome.getToken() + ")";
                    break;
                }
                default:
                    break;
            }
        }

        if(StringUtils.isEmpty(taskOutcomeStatus)) {
            taskOutcomeStatus = outcomeStatus.getDisplayName();
        }

        String badOutcomeReason = null;
        String failureDescription = null;
        if(badOutcome){
            if(lastTask.hasTaskWorkItem()){
                if(lastTask.getTaskWorkItem().hasFailureDescription()){
                    failureDescription = lastTask.getTaskWorkItem().getFailureDescription();
                    if(failureDescription.length()> 64){
                        badOutcomeReason = failureDescription.substring(0, 80) + "...";
                    } else {
                        badOutcomeReason = failureDescription;
                    }
                }
            }
        }

        reportBuilder.append("Outcome -->" + taskOutcomeStatus + "\n");

        formattedReportBuilder.append("<tr>");
        if(badOutcome) {
            formattedReportBuilder.append("<td> Outcome </td><td><font color=red>" + taskOutcomeStatus + "</font></td>");
            if(StringUtils.isNotEmpty(badOutcomeReason)) {
                formattedReportBuilder.append("</tr><tr><td> Reason </td><td><font color=red>" + badOutcomeReason + "</font></td>");
            }
        } else {
            formattedReportBuilder.append("<td> Outcome </td><td><font color=green>" + taskOutcomeStatus + "</font></td>");
        }
        formattedReportBuilder.append("</tr>");

        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<td> Metadata </td><td>");
        if(StringUtils.isNotEmpty(msh)){
            reportBuilder.append(":: " + msh + "\n");
            formattedReportBuilder.append( msh + "\n");
        }
        if(StringUtils.isNotEmpty(pid)){
            reportBuilder.append(":: " + pid + "\n");
            formattedReportBuilder.append(pid);
        }
        formattedReportBuilder.append("</td>");
        formattedReportBuilder.append("</tr>");

        if(badOutcome){
            if(StringUtils.isNotEmpty(failureDescription)){
                reportBuilder.append("---Error--------------------------------------------------------\n");
                reportBuilder.append(failureDescription);
            }
        }

        reportBuilder.append("----------------------------------------------------------------\n");
        formattedReportBuilder.append("</table>");

        PetasosComponentITOpsNotification taskNotification = new PetasosComponentITOpsNotification();

        String report = reportBuilder.toString();
        String formattedReport = formattedReportBuilder.toString();

        taskNotification.setFormattedContent(formattedReport);
        taskNotification.setContent(report);

        getLogger().debug(".newEndpointOnlyTaskReport(): Exit, taskNotification->{}", taskNotification);
        return(taskNotification);
    }

    //
    // Getters (and Setters)
    //

    protected TaskLoggingCacheServices getTaskCacheServices(){
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

    protected EndpointInformationExtractor getEndpointInfoExtrator(){
        return(this.endpointInfoExtrator);
    }
}
