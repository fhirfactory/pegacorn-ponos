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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.tasks.activities;

import net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants;
import net.fhirfactory.pegacorn.core.model.componentid.ComponentIdType;
import net.fhirfactory.pegacorn.core.model.petasos.oam.notifications.ITOpsNotificationContent;
import net.fhirfactory.pegacorn.core.model.petasos.oam.notifications.PetasosComponentITOpsNotification;
import net.fhirfactory.pegacorn.core.model.petasos.participant.id.PetasosParticipantId;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosAggregateTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityElementType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskTraceabilityType;
import net.fhirfactory.pegacorn.deployment.names.subsystems.SubsystemNames;
import net.fhirfactory.pegacorn.internals.fhir.r4.internal.topics.HL7V2XTopicFactory;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.PetasosAggregateTaskFactory;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.GeneralTaskMetadataExtractor;
import net.fhirfactory.pegacorn.petasos.core.tasks.factories.metadata.HL7v2xTaskMetadataExtractor;
import net.fhirfactory.pegacorn.petasos.oam.metrics.agents.ProcessingPlantMetricsAgentAccessor;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.ActionableTaskCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.oam.ProcessingPlantTaskReportProxy;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.factories.AggregateTaskReportFactory;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.factories.EndpointInformationExtractor;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.tasks.activities.common.TaskActivityProcessorBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

@ApplicationScoped
public class AggregateTaskManagementActivities extends TaskActivityProcessorBase {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateTaskManagementActivities.class);

    private boolean initialised;

    private DateTimeFormatter timeFormatter;

    private boolean stillRunning;
    private Instant lastRunInstant;

    private Long CONTENT_FORWARDER_STARTUP_DELAY = 120000L;
    private Long CONTENT_FORWARDER_REFRESH_PERIOD = 15000L;

    @Inject
    private ActionableTaskCacheServices taskCacheServices;

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
    private PetasosAggregateTaskFactory aggregateTaskFactory;

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

    public AggregateTaskManagementActivities() {
        super();
        this.initialised = false;
        this.stillRunning = false;
        this.lastRunInstant = Instant.EPOCH;
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

        scheduledAggregateTaskManagementDaemon();

        this.initialised = true;

        getLogger().info(".initialise(): Initialisation Finish...");
    }

    //
    // Daemon Task Scheduler
    //

    private void scheduledAggregateTaskManagementDaemon() {
        getLogger().debug(".scheduledAggregateTaskManagementDaemon(): Entry");
        TimerTask aggregateTaskDaemonTask = new TimerTask() {
            public void run() {
                getLogger().debug(".aggregateTaskDaemonTask(): Entry");
                if (!isStillRunning()) {
                    aggregateTaskDaemon();
                }
                getLogger().debug(".aggregateTaskDaemonTask(): Exit");
            }
        };
        Timer timer = new Timer("AggregateTaskDaemonTaskTimer");
        timer.schedule(aggregateTaskDaemonTask, CONTENT_FORWARDER_STARTUP_DELAY, CONTENT_FORWARDER_REFRESH_PERIOD);
        getLogger().debug(".scheduledAggregateTaskManagementDaemon(): Exit");
    }

    public void aggregateTaskDaemon(){
        getLogger().debug(".aggregateTaskDaemon(): Entry");

        setStillRunning(true);

        try {

        } catch(Exception outerException){
            getLogger().warn(".aggregateTaskDaemon(): Problem processing ActionableTask, message->{}, stackTrace->{}", ExceptionUtils.getMessage(outerException), ExceptionUtils.getStackTrace(outerException));
        }
        setStillRunning(false);

        getLogger().debug(".aggregateTaskDaemon(): Exit");
    }


    //
    // Business Methods
    //

    public PetasosAggregateTask createAggregateTask(PetasosActionableTask finalisedTask){
        getLogger().debug(".createAggregateTask(): Entry, finalisedTask->{}", finalisedTask);
        if(finalisedTask == null){
            getLogger().debug(".createAggregateTask(): Exit, finalisedTask is null");
            return(null);
        }
        PetasosAggregateTask aggregateTask = getAggregateTaskFactory().newAggregateTask(finalisedTask);
        return(aggregateTask);
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    protected ActionableTaskCacheServices getTaskCacheServices(){
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

    protected EndpointInformationExtractor getEndpointInfoExtractor(){
        return(this.endpointInfoExtrator);
    }

    protected Instant getLastRunInstant() {
        return lastRunInstant;
    }

    protected void setLastRunInstant(Instant lastRunInstant) {
        this.lastRunInstant = lastRunInstant;
    }

    protected PetasosAggregateTaskFactory getAggregateTaskFactory(){
        return(aggregateTaskFactory);
    }
}
