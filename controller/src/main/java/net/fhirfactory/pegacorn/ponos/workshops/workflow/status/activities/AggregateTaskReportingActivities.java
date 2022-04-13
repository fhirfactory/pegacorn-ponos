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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.status.activities;

import net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants;
import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.oam.notifications.PetasosComponentITOpsNotification;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityElementType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityType;
import net.fhirfactory.pegacorn.deployment.names.subsystems.SubsystemNames;
import net.fhirfactory.pegacorn.internals.fhir.r4.internal.topics.HL7V2XTopicFactory;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.GeneralTaskMetadataExtractor;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.HL7v2xTaskMetadataExtractor;
import net.fhirfactory.pegacorn.petasos.oam.metrics.agents.ProcessingPlantMetricsAgentAccessor;
import net.fhirfactory.pegacorn.ponos.workshops.oam.ProcessingPlantTaskReportProxy;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.factories.AggregateTaskReportFactory;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.status.activities.common.TaskActivityProcessorBase;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.PonosPetasosActionableTaskCacheServices;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.factories.EndpointInformationExtractor;
import org.apache.commons.lang3.StringUtils;

@ApplicationScoped
public class AggregateTaskReportingActivities extends TaskActivityProcessorBase {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateTaskReportingActivities.class);

    private boolean initialised;

    private DateTimeFormatter timeFormatter;

    private boolean stillRunning;

    private Long CONTENT_FORWARDER_STARTUP_DELAY = 120000L;
    private Long CONTENT_FORWARDER_REFRESH_PERIOD = 15000L;

    @Inject
    private PonosPetasosActionableTaskCacheServices taskCacheServices;

    @Inject
    private HL7V2XTopicFactory hl7V2XTopicFactory;

    @Inject
    private HL7v2xTaskMetadataExtractor hl7V2XTaskMetadataExtractor;

    @Inject
    private GeneralTaskMetadataExtractor generalTaskMetadataExtractor;

    @Inject
    private ProcessingPlantMetricsAgentAccessor metricsAgentAccessor;

    @Inject
    private ProcessingPlantTaskReportProxy taskReportProxy;

    @Inject
    private AggregateTaskReportFactory aggregateTaskReportFactory;

    @Inject
    private SubsystemNames subsystemNames;

    @Inject
    private EndpointInformationExtractor endpointInfoExtrator;

    //
    // Constructor(s)
    //

    //
    // Constructor(s)
    //

    public AggregateTaskReportingActivities() {
        super();
        this.initialised = false;
        this.stillRunning = false;
        timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss.SSS").withZone(ZoneId.of(PetasosPropertyConstants.DEFAULT_TIMEZONE));

    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise() {
        getLogger().debug(".initialise(): Entry");
        if (initialised) {
            getLogger().debug(".initialise(): Exit, already initialised, nothing to do");
            return;
        }
        getLogger().info(".initialise(): Initialisation Start...");

        scheduleAggregateTaskReportingDaemon();

        this.initialised = true;

        getLogger().info(".initialise(): Initialisation Finish...");
    }

    //
    // Daemon Task Scheduler
    //

    private void scheduleAggregateTaskReportingDaemon() {
        getLogger().debug(".scheduleAggregateTaskReportingDaemon(): Entry");
        TimerTask aggregateTaskReportDaemonTask = new TimerTask() {
            public void run() {
                getLogger().debug(".aggregateTaskReportDaemonTask(): Entry");
                if (!isStillRunning()) {
                    aggregateTaskReportingDaemon();
                }
                getLogger().debug(".aggregateTaskReportDaemonTask(): Exit");
            }
        };
        Timer timer = new Timer("AggregateTaskReportDaemonTaskTimer");
        timer.schedule(aggregateTaskReportDaemonTask, CONTENT_FORWARDER_STARTUP_DELAY, CONTENT_FORWARDER_REFRESH_PERIOD);
        getLogger().debug(".scheduleAggregateTaskReportingDaemon(): Exit");
    }

    //
    // Business Methods
    //

    public void aggregateTaskReportingDaemon(){
        getLogger().debug(".aggregateTaskReportingDaemon(): Entry");

        setStillRunning(true);

        try {

            List<PetasosActionableTask> lastInChainActionableEvents = getTaskCacheServices().getLastInChainActionableEvents();
            for (PetasosActionableTask currentTask : lastInChainActionableEvents) {
                if(currentTask.hasTaskId()) {
                    if (getTaskCacheServices().hasAlreadyBeenReportedOn(currentTask.getTaskId())) {
                        // do nothing
                    } else {
                        try {
                            getLogger().debug(".aggregateTaskReportingDaemon(): Iterating, currentTask->{}", currentTask.getTaskId());
                            if (!currentTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getSubsystemParticipantName().contains(subsystemNames.getITOpsIMParticipantName())) {
                                publishEndOfChainFullTaskReport(currentTask);
                                publishEndPointOnlyTaskReport(currentTask);
                            }
                            getTaskCacheServices().archivePetasosActionableTask(currentTask);
                            getTaskCacheServices().setReportStatus(currentTask.getTaskId(), true);
                        } catch (Exception ex) {
                            getLogger().warn(".aggregateTaskReportingDaemon(): Could not generate/send task report, message->{}, stackTrace->{}", ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex));
                        }
                    }
                } else {
                    getLogger().warn(".aggregateTaskReportingDaemon(): Problem processing ActionableTask, TaskId is null, ActionableTask->{}", currentTask);
                }
            }
        } catch(Exception outerException){
            getLogger().warn(".aggregateTaskReportingDaemon(): Problem processing ActionableTask, message->{}, stackTrace->{}", ExceptionUtils.getMessage(outerException), ExceptionUtils.getStackTrace(outerException));
        }
        setStillRunning(false);

        getLogger().debug(".aggregateTaskReportingDaemon(): Exit");
    }

    public void publishEndOfChainFullTaskReport(PetasosActionableTask lastTask) {
        getLogger().debug(".publishEndOfChainTaskReport(): Entry, lastTask->{}", lastTask);

        TaskTraceabilityType taskTraceability = lastTask.getTaskTraceability();

        //
        // Get the 1st
        TaskTraceabilityElementType firstTaskTraceabilityElement = taskTraceability.getTaskJourney().get(0);
        TaskIdType firstTaskId = firstTaskTraceabilityElement.getActionableTaskId();
        PetasosActionableTask firstTask = getTaskCacheServices().getPetasosActionableTask(firstTaskId);

        //
        // Create the Report
        String reportString = getAggregateTaskReportFactory().endOfChainReport(lastTask);

        //
        // Publish to Last Participant
        String lastSubsystemName = lastTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getSubsystemParticipantName();
        ComponentIdType lastComponentId = lastTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getComponentID();
        taskReportProxy.sendITOpsTaskReport(lastSubsystemName,lastComponentId,reportString);

        //
        // Publish to First Participant
        String firstSubsystemName = firstTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getSubsystemParticipantName();
        ComponentIdType firstComponentId = firstTask.getTaskFulfillment().getFulfillerWorkUnitProcessor().getComponentID();
        taskReportProxy.sendITOpsTaskReport(firstSubsystemName,firstComponentId, reportString);

        //
        // All done
        getLogger().debug(".publishEndOfChainTaskReport(): Exit");
    }

    public void publishEndPointOnlyTaskReport(PetasosActionableTask lastTask){
        getLogger().debug(".publishEndPointOnlyTaskReport(): Entry, lastTask->{}", lastTask);

        //
        // Create the report
        PetasosComponentITOpsNotification notificationBase = getAggregateTaskReportFactory().newEndpointOnlyTaskReport(lastTask);

        //
        // Resolve Egress Endpoint Participant Name(s)
        String egressEndpointParticipantName = getEndpointInfoExtrator().getEndpointParticipantName(lastTask, false);

        //
        // Check to see if it isn't an internal (+ve) report for message forwarding
        if(StringUtils.isNotEmpty(egressEndpointParticipantName)) {
            if (egressEndpointParticipantName.contains("PetasosTaskForwarderWUP")) {
                // we don't want to send these if they are OK outcomes
                try {
                    if (lastTask.getTaskFulfillment().getStatus().equals(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_FINISHED) || lastTask.getTaskFulfillment().getStatus().equals(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_FINALISED)) {
                        // exit out
                        return;
                    }
                } catch (Exception ex) {
                    getLogger().warn(".publishEndPointOnlyTaskReport(): Couldn't resolve task outcome status, message->{}", ExceptionUtils.getMessage(ex));
                    return;
                }
            }
        }

        //
        // Get the 1st Task
        TaskTraceabilityType taskTraceability = lastTask.getTaskTraceability();
        TaskTraceabilityElementType firstTaskTraceabilityElement = taskTraceability.getTaskJourney().get(0);
        TaskIdType firstTaskId = firstTaskTraceabilityElement.getActionableTaskId();
        PetasosActionableTask firstTask = getTaskCacheServices().getPetasosActionableTask(firstTaskId);

        //
        // Resolve Ingres Endpoint Participant Name(s)
        String ingresEndpointParticipantName = getEndpointInfoExtrator().getEndpointParticipantName(firstTask, true);

        //
        // Resolve Endpoint Component ID(s)
        ComponentIdType egressComponentId = getEndpointInfoExtrator().getEndpointComponentId(lastTask);
        ComponentIdType ingresComponentId = getEndpointInfoExtrator().getEndpointComponentId(firstTask);

        if(StringUtils.isNotEmpty(egressEndpointParticipantName) && egressComponentId != null ){
            taskReportProxy.sendITOpsEndpointOnlyTaskReport(egressEndpointParticipantName,egressComponentId, notificationBase.getContent(), notificationBase.getFormattedContent());
        }

        if(StringUtils.isNotEmpty(ingresEndpointParticipantName) && ingresComponentId != null ){
            taskReportProxy.sendITOpsEndpointOnlyTaskReport(ingresEndpointParticipantName,ingresComponentId, notificationBase.getContent(), notificationBase.getFormattedContent());
        }

        getLogger().debug(".publishEndPointOnlyTaskReport(): Exit, report->{}", notificationBase);
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

    public boolean isInitialised() {
        return initialised;
    }

    public void setInitialised(boolean initialised) {
        this.initialised = initialised;
    }

    public boolean isStillRunning() {
        return stillRunning;
    }

    public void setStillRunning(boolean stillRunning) {
        this.stillRunning = stillRunning;
    }

    protected AggregateTaskReportFactory getAggregateTaskReportFactory(){
        return(aggregateTaskReportFactory);
    }

    protected EndpointInformationExtractor getEndpointInfoExtrator(){
        return(this.endpointInfoExtrator);
    }

}
