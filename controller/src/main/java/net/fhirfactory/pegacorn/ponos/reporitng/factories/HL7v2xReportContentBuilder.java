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
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityElementType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityType;
import net.fhirfactory.pegacorn.deployment.names.subsystems.SubsystemNames;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.GeneralTaskMetadataExtractor;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.HL7v2xTaskMetadataExtractor;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.PonosPetasosActionableTaskCacheServices;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class HL7v2xReportContentBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(HL7v2xReportContentBuilder.class);

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

    //
    // Constructor(s)
    //

    public HL7v2xReportContentBuilder(){
        timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of(PetasosPropertyConstants.DEFAULT_TIMEZONE));
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    protected PonosPetasosActionableTaskCacheServices getTaskCacheServices(){
        return(taskCacheServices);
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

    //
    // Business Methods
    //

    public ITOpsNotificationContent newEndpointOnlyActivityReport(PetasosActionableTask lastTask, boolean includePayload){
        getLogger().warn(".newEndpointOnlyActivityReport(): Entry, lastTask->{}, includePayload->{}", lastTask, includePayload);

        ITOpsNotificationContent notificationContent = new ITOpsNotificationContent();

        //
        // Get the 1st
        TaskTraceabilityType taskTraceability = lastTask.getTaskTraceability();
        TaskTraceabilityElementType firstTaskTraceabilityElement = taskTraceability.getTaskJourney().get(0);
        TaskIdType firstTaskId = firstTaskTraceabilityElement.getActionableTaskId();
        PetasosActionableTask firstTask = getTaskCacheServices().getPetasosActionableTask(firstTaskId);
        if(firstTask == null){
            getLogger().debug(".newEndpointOnlyActivityReport(): Exit, Cannot ascertain first task, returning -null-");
            return(null);
        }

        //
        // Get the ingresName and egressName of the "Endpoints" if they exist
        String ingresName = getTaskReportContentExtractor().getIngresName(firstTask);
        String egressName = getTaskReportContentExtractor().getEgressName(lastTask);

        //
        // Get the ingresContent and egressContent
        String ingresContent = getTaskReportContentExtractor().getIngresContent(firstTask);
        String egressContent = getTaskReportContentExtractor().getIngresContent(lastTask);
        String egressOutcomeContent = getTaskReportContentExtractor().getOutcomeContent(lastTask);

        //
        // Create the StringBuilder(s) (report writers)
        StringBuilder reportBuilder = new StringBuilder();
        StringBuilder formattedReportBuilder = new StringBuilder();

        //
        // Get the Task (Journey) Timestamp set
        String startTime = getTaskStartTimeAsString(firstTask);
        String lastTaskStartTime = getTaskStartTimeAsString(lastTask);
        String lastTaskFinishTime = getTaskFinishTimeAsString(lastTask);

        //
        // Get the HL7 v2 Message Metadata
        List<String> metadataHeader = null;
        String msh= null;
        String pid = null;
        if (getHL7v2MetadataFactory().isHL7V2Payload(firstTask.getTaskWorkItem().getIngresContent())) {
            metadataHeader = getHL7v2MetadataFactory().getHL7v2MetadataHeaderInfo(firstTask.getTaskWorkItem().getIngresContent());
            try {
                msh = getHL7v2MetadataFactory().getMSH(firstTask.getTaskWorkItem().getIngresContent().getPayload());
                pid = getHL7v2MetadataFactory().getPID(firstTask.getTaskWorkItem().getIngresContent().getPayload());
            } catch (Exception ex){
                getLogger().warn(".newEndpointOnlyTaskReport(): Cannot decode message->{}", firstTask.getTaskId());
            }
        } else {
            metadataHeader = new ArrayList<>();
        }

        String taskOutcomeStatus = getTaskReportContentExtractor().getOutcomeStatusDetail(lastTask);
        boolean badOutcome = getTaskReportContentExtractor().getOutcomeStatus(lastTask);

        //
        // Create the UNFORMATTED notification content
        reportBuilder.append("Ingres --> " + ingresName + "\n");
        if(includePayload){
            reportBuilder.append(ingresContent + "\n");
        }
        reportBuilder.append(", Egress --> " + egressName + "\n");
        if(includePayload) {
            reportBuilder.append(egressContent + "\n");
        }
        reportBuilder.append("Outcome -->" + taskOutcomeStatus + "\n");
        if(includePayload){
            reportBuilder.append("Data -->" + egressOutcomeContent);
        } else {
            for (String currentMetadataHeaderLine : metadataHeader) {
                reportBuilder.append(":: " + currentMetadataHeaderLine + "\n");
            }
            reportBuilder.append(msh);
            reportBuilder.append(" ");
            reportBuilder.append(pid);
        }

        //
        // Create the FORMATTED notification content
        formattedReportBuilder.append("<table>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<th> Ingres </th><th>"+ ingresName + " @ "+startTime+"</th>");
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<td> Data </td><td>");
        if(StringUtils.isNotEmpty(msh)){
            formattedReportBuilder.append("<small>"+ msh + "\n </small>");
        }
        if(StringUtils.isNotEmpty(pid)){
            formattedReportBuilder.append("<small>"+ pid + "</small>");
        }
        formattedReportBuilder.append("</td>");
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<th> Egress </th><th>"+ egressName + " @ "+lastTaskStartTime+"</th>");
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("<tr>");
        if(badOutcome) {
            formattedReportBuilder.append("<td> Outcome </td><td><font color=red>" + taskOutcomeStatus + "</font> @ "+lastTaskFinishTime+"</td>");
        } else {
            formattedReportBuilder.append("<td> Outcome </td><td><font color=green>" + taskOutcomeStatus + "</font> @ "+lastTaskFinishTime+"</td>");
        }
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<td> Data </td><td>"+ egressOutcomeContent + "</td>");
        formattedReportBuilder.append("</tr>");

        formattedReportBuilder.append("</table>");

        //
        // Populate the notificationContent object
        String report = reportBuilder.toString();
        String formattedReport = formattedReportBuilder.toString();

        notificationContent.setFormattedContent(formattedReport);
        notificationContent.setContent(report);

        getLogger().debug(".newEndpointOnlyActivityReport(): Exit, notificationContent->{}", notificationContent);
        return(notificationContent);
    }

    protected String getTaskStartTimeAsString(PetasosActionableTask task){
        getLogger().debug(".getTaskStartTimeAsString(): Entry, task->{}", task);
        Instant startTime = null;
        String startTimeString = null;

        if(task.hasTaskFulfillment()){
            if(task.getTaskFulfillment().hasStartInstant()){
                startTime = task.getTaskFulfillment().getStartInstant();
            }
        }
        if(startTime == null){
            startTimeString = getTimeFormatter().format(Instant.now());
        } else {
            startTimeString = getTimeFormatter().format(startTime);
        }
        getLogger().debug(".getTaskStartTimeAsString(): Exit, startTimeString->{}", startTimeString);
        return(startTimeString);
    }

    protected String getTaskFinishTimeAsString(PetasosActionableTask task){
        getLogger().debug(".getTaskFinishTimeAsString(): Entry, task->{}", task);
        Instant finishTime = null;
        String finishTimeString = null;

        if(task.hasTaskFulfillment()) {
            if (task.getTaskFulfillment().hasFinishInstant()) {
                finishTime = task.getTaskFulfillment().getFinishInstant();
            }
        }
        if(finishTime == null) {
            finishTimeString = "N/A";
        } else {
            finishTimeString = getTimeFormatter().format(finishTime);
        }
        getLogger().debug(".getTaskFinishTimeAsString(): Exit, startTimeString->{}", finishTimeString);
        return(finishTimeString);
    }

}
