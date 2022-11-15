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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid;

import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridElementKeyInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.TaskCompletionSummaryType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.TaskFulfillmentType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.datatypes.TaskExecutionControl;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.valuesets.TaskExecutionCommandEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.datatypes.TaskOutcomeStatusType;
import net.fhirfactory.pegacorn.core.model.petasos.uow.UoWPayloadSet;
import net.fhirfactory.pegacorn.core.model.petasos.wup.valuesets.PetasosTaskExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.edge.jgroups.JGroupsIntegrationPointSummary;
import net.fhirfactory.pegacorn.petasos.oam.metrics.agents.ProcessingPlantMetricsAgentAccessor;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.TaskGridCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.queues.CentralTaskQueueMap;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.taskgrid.queing.TaskQueueServices;
import net.fhirfactory.pegacorn.services.tasks.manager.PetasosTaskServicesManagerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

@ApplicationScoped
public class TaskGridServer extends PetasosTaskServicesManagerHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TaskGridServer.class);

    private boolean initialised;

    private boolean firstRunComplete;

    private boolean daemonIsStillRunning;
    private Instant daemonLastRunTime;

    private static Long TASK_STATUS_MANAGEMENT_DAEMON_STARTUP_DELAY = 60000L;
    private static Long TASK_STATUS_MANAGEMENT_DAEMON_CHECK_PERIOD = 10000L;
    private static Long TASK_STATUS_MANAGEMENT_DAEMON_RESET_PERIOD = 180L;
    private static Long TASK_AGE_BEFORE_FORCED_RETIREMENT = 120L;

    @Inject
    private CentralTaskQueueMap taskQueueMap;

    @Inject
    private TaskGridCacheServices taskCacheServices;

    @Inject
    private ProcessingPlantMetricsAgentAccessor metricsAgentAccessor;

    @Inject
    private TaskQueueServices taskQueueServices;

    //
    // Constructor(s)
    //

    public TaskGridServer(){
        super();
        this.initialised = false;
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

        scheduleTaskStatusManagementDaemon();

        this.initialised = true;

        getLogger().info(".initialise(): Initialisation Finish...");
    }

    //
    // Daemon Scheduler
    //

    private void scheduleTaskStatusManagementDaemon() {
        getLogger().debug(".scheduleTaskStatusManagementDaemon(): Entry");
        TimerTask taskStatusManagementDaemonTimerTask = new TimerTask() {
            public void run() {
                getLogger().debug(".taskStatusManagementDaemonTimerTask(): Entry");
                if (!daemonIsStillRunning) {
                    taskStatusManagementDaemon();
                    setDaemonLastRunTime(Instant.now());
                } else {
                    Long ageSinceRun = Instant.now().getEpochSecond() - getDaemonLastRunTime().getEpochSecond();
                    if (ageSinceRun > getTaskStatusManagementDaemonResetPeriod()) {
                        taskStatusManagementDaemon();
                        setDaemonLastRunTime(Instant.now());
                    }
                }
                getLogger().debug(".taskStatusManagementDaemonTimerTask(): Exit");
            }
        };
        Timer timer = new Timer("TaskStatusManagementDaemonTimer");
        timer.schedule(taskStatusManagementDaemonTimerTask, getTaskStatusManagementDaemonStartupDelay(), getTaskStatusManagementDaemonCheckPeriod());
        getLogger().debug(".scheduleTaskStatusManagementDaemon(): Exit");
    }

    //
    // Daemon
    //

    public void taskStatusManagementDaemon(){
        int cacheSize = taskCacheServices.getTaskCacheSize();
        metricsAgentAccessor.getMetricsAgent().updateLocalCacheStatus("ActionableTaskCache", cacheSize);
        int registrationCacheSize = taskCacheServices.getTaskRegistrationCacheSize();
        metricsAgentAccessor.getMetricsAgent().updateLocalCacheStatus("ActionableTaskRegistrationCache", registrationCacheSize);

        Set<DatagridElementKeyInterface> agedCacheContent = taskCacheServices.getAgedCacheContent(TASK_AGE_BEFORE_FORCED_RETIREMENT);
        for(DatagridElementKeyInterface currentKey: agedCacheContent){
            taskCacheServices.clearTaskFromCache(currentKey);
        }
    }

    //
    // Business Methods
    //

    @Override
    public TaskIdType queueTask(PetasosActionableTask actionableTask, JGroupsIntegrationPointSummary integrationPoint) {
        getLogger().debug(".queueTask(): Entry, actionableTask->{}, integrationPoint->{}", actionableTask, integrationPoint);
        Boolean queued = getTaskQueueServices().queueTask(actionableTask);
        actionableTask.setRegistered(queued);
        getLogger().debug(".queueTask(): Exit, actionableTask->{}", actionableTask);
        return(actionableTask.getTaskId());
    }

    @Override
    public PetasosActionableTask getNextPendingTask(String participantName, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        return null;
    }

    @Override
    public TaskExecutionControl notifyTaskStart(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskStart(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}", participantName, taskId, taskFulfillmentDetail);
        TaskExecutionCommandEnum taskControl = getTaskCache().updateTaskStatusToStarted(participantName, taskId, taskFulfillmentDetail);
        TaskExecutionControl executionControl = new TaskExecutionControl();
        executionControl.setExecutionCommand(taskControl);
        getLogger().debug(".notifyTaskStart(): Exit");
        return(executionControl);
    }

    @Override
    public TaskExecutionControl notifyTaskFinish(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskFinish(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome );
        TaskExecutionCommandEnum taskControl = getTaskCache().updateTaskStatusToFinish(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome);
        TaskExecutionControl executionControl = new TaskExecutionControl();
        executionControl.setExecutionCommand(taskControl);
        getLogger().debug(".notifyTaskFinish(): Exit");
        return(executionControl);
    }

    @Override
    public TaskExecutionControl notifyTaskCancellation(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskCancellation(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome );
        TaskExecutionCommandEnum taskControl = getTaskCache().updateTaskStatusToCancelled(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome);
        TaskExecutionControl executionControl = new TaskExecutionControl();
        executionControl.setExecutionCommand(taskControl);
        getLogger().debug(".notifyTaskCancellation(): Exit");
        return(executionControl);
    }

    @Override
    public TaskExecutionControl notifyTaskFailure(String participantName, TaskIdType taskId, TaskFulfillmentType taskFulfillmentDetail, UoWPayloadSet egressPayload, TaskOutcomeStatusType taskOutcome, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskCancellation(): Entry, participantName->{}, taskId->{}, taskFulfillmentDetail->{}, egressPayload->{}, taskOutcome->{}", participantName, taskId, taskFulfillmentDetail,egressPayload, taskOutcome );
        TaskExecutionCommandEnum taskControl = getTaskCache().updateTaskStatusToFailed(participantName, taskId, taskFulfillmentDetail, egressPayload, taskOutcome);
        TaskExecutionControl executionControl = new TaskExecutionControl();
        executionControl.setExecutionCommand(taskControl);
        getLogger().debug(".notifyTaskCancellation(): Exit");
        return(executionControl);
    }

    @Override
    public TaskExecutionControl notifyTaskFinalisation(String participantName, TaskIdType taskId, TaskCompletionSummaryType completionSummary, JGroupsIntegrationPointSummary requesterEndpointSummary) {
        getLogger().debug(".notifyTaskCancellation(): Entry, participantName->{}, taskId->{}, completionSummary->{}", participantName, taskId, completionSummary);
        TaskExecutionCommandEnum taskControl = getTaskCache().updateTaskStatusToFinalised(participantName, taskId, completionSummary);
        TaskExecutionControl executionControl = new TaskExecutionControl();
        executionControl.setExecutionCommand(taskControl);
        getLogger().debug(".notifyTaskCancellation(): Exit");
        return(executionControl);
    }

    //
    // Getters (and Setters)
    //

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    protected TaskGridCacheServices getTaskCache() {
        return (taskCacheServices);
    }

    protected Instant getDaemonLastRunTime() {
        return (daemonLastRunTime);
    }

    protected void setDaemonLastRunTime(Instant instant){
        this.daemonLastRunTime = instant;
    }

    public static Long getTaskStatusManagementDaemonStartupDelay() {
        return TASK_STATUS_MANAGEMENT_DAEMON_STARTUP_DELAY;
    }

    public static Long getTaskStatusManagementDaemonCheckPeriod() {
        return TASK_STATUS_MANAGEMENT_DAEMON_CHECK_PERIOD;
    }

    public static Long getTaskStatusManagementDaemonResetPeriod() {
        return TASK_STATUS_MANAGEMENT_DAEMON_RESET_PERIOD;
    }

    protected TaskQueueServices getTaskQueueServices(){
        return(taskQueueServices);
    }

    protected CentralTaskQueueMap getTaskQueueMap(){
        return(taskQueueMap);
    }

}
