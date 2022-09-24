/*
 * Copyright (c) 2022 Mark A. Hunter (ACT Health)
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

import net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipant;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.work.datatypes.TaskWorkItemSubscriptionType;
import net.fhirfactory.pegacorn.petasos.oam.metrics.agents.ProcessingPlantMetricsAgentAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@ApplicationScoped
public class ProcessingPlantPathwayReportProxy {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingPlantTaskReportProxy.class);

    private DateTimeFormatter timeFormatter;

    @Inject
    private ProcessingPlantMetricsAgentAccessor metricsAgentAccessor;

    //
    // Constructor(s)
    //

    public ProcessingPlantPathwayReportProxy(){
        timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of(PetasosPropertyConstants.DEFAULT_TIMEZONE));
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    protected DateTimeFormatter getTimeFormatter(){
        return(timeFormatter);
    }

    //
    // Business Methods
    //

    public void reportParticipantRegistration(PetasosParticipant participant, boolean isUpdate){
        StringBuilder reportBuilder = new StringBuilder();
        StringBuilder formattedReportBuilder = new StringBuilder();

        String nowAsString = getTimeFormatter().format(Instant.now());
        String isUpdateString = "Create";
        if(isUpdate){
            isUpdateString = "Update";
        }

        reportBuilder.append("--------------------\n");
        reportBuilder.append("---Participant Registration (" + isUpdateString + " @ " + nowAsString + ")---\n");
        reportBuilder.append("Participant.Name --> " + participant.getParticipantId() +"\n");
        reportBuilder.append("Participant.Id --> " + participant.getComponentID().getDisplayName() + "\n");
        int counter = 0;
        for(TaskWorkItemSubscriptionType currentSubscription: participant.getSubscriptions()) {
            reportBuilder.append("  Subscription[" + counter + "] --> " + condensedSubscriptionSummary(currentSubscription) + "\n");
            counter += 1;
        }
        reportBuilder.append("--------------------\n");

        formattedReportBuilder.append("<b>Participant Registration: " +  nowAsString +"</b>");
        formattedReportBuilder.append("<table>");
        formattedReportBuilder.append("<th>Participant.Name </th>");
        formattedReportBuilder.append("<th>"+participant.getParticipantId()+"</th>");
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("<tr>");
        formattedReportBuilder.append("<td>Participant.Id</td>");
        formattedReportBuilder.append("<td>"+ participant.getComponentID().getDisplayName()+"</td>");
        formattedReportBuilder.append("</tr>");
        counter = 0;
        for(TaskWorkItemSubscriptionType currentSubscription: participant.getSubscriptions()) {
            formattedReportBuilder.append("<tr>");
            if(currentSubscription.hasSourceProcessingPlantParticipantName()) {
                formattedReportBuilder.append("<td>  Subscription[" + counter + "] </td>");
                formattedReportBuilder.append("<td> SourceParticipant:" + currentSubscription.getSourceProcessingPlantParticipantName() + "</td>");
            }
            if(currentSubscription.hasContentDescriptor()){
                formattedReportBuilder.append("<td>  Subscription[" + counter + "] </td>");
                formattedReportBuilder.append("<td> Content:" + currentSubscription.getContentDescriptor().toDotString() + "</td>");
            }
            counter += 1;
        }
        formattedReportBuilder.append("</tr>");
        formattedReportBuilder.append("</table>");

        metricsAgentAccessor.getMetricsAgent().sendITOpsNotification(reportBuilder.toString(), formattedReportBuilder.toString());
    }

    public String condensedSubscriptionSummary(TaskWorkItemSubscriptionType subscription){
        StringBuilder condensedSummaryBuilder = new StringBuilder();
        if(subscription.hasSourceProcessingPlantParticipantName()){
            condensedSummaryBuilder.append("SourceParticipant:" + subscription.getSourceProcessingPlantParticipantName() + ",");
        }
        if(subscription.hasContainerDescriptor()) {
            condensedSummaryBuilder.append("Container:" + subscription.getContainerDescriptor().toDotString() + ",");
        }
        if(subscription.hasContentDescriptor()){
            condensedSummaryBuilder.append("Content:" + subscription.getContentDescriptor().toDotString() + ",");
        }
        if(subscription.hasDataParcelFlowDirection()){
            condensedSummaryBuilder.append("Direction:" + subscription.getDataParcelFlowDirection().getDisplayName() + ",");
        }
        if(subscription.hasEnforcementPointApprovalStatus()){
            condensedSummaryBuilder.append("Approval:" + subscription.getEnforcementPointApprovalStatus().getDisplayName() + ",");
        }
        if(subscription.hasNormalisationStatus()){
            condensedSummaryBuilder.append("Normalisation:" + subscription.getNormalisationStatus().getDisplayName() + ",");
        }
        if(subscription.hasValidationStatus()){
            condensedSummaryBuilder.append("Validation:" + subscription.getValidationStatus().getDisplayName() + ",");
        }
        if(subscription.hasDataParcelType()){
            condensedSummaryBuilder.append("Type:" + subscription.getDataParcelType().getDisplayName()+ ",");
        }
        condensedSummaryBuilder.append("InternallyDistributable: " + subscription.isInterSubsystemDistributable());
        return(condensedSummaryBuilder.toString());
    }

    public void touchProcessingPlantSubscriptionSynchronisationInstant(String processingPlant){
        metricsAgentAccessor.getMetricsAgent().touchPathwaySynchronisationIndicator(processingPlant);
    }
}
