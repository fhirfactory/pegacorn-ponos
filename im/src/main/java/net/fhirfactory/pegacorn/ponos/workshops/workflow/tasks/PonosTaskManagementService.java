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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.tasks;

import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridElementKeyInterface;
import net.fhirfactory.pegacorn.core.interfaces.tasks.PetasosTaskBrokerInterface;
import net.fhirfactory.pegacorn.core.model.petasos.jobcard.PetasosTaskJobCard;
import net.fhirfactory.pegacorn.core.model.petasos.participant.id.PetasosParticipantId;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.collections.PetasosActionableTaskSet;
import net.fhirfactory.pegacorn.core.model.petasos.task.collections.PetasosTaskIdSet;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.FulfillmentTrackingIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.datatypes.TaskFulfillmentType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.schedule.valuesets.TaskExecutionCommandEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.TaskOutcomeStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.datatypes.TaskStorageType;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.traceability.valuesets.TaskStorageStatusEnum;
import net.fhirfactory.pegacorn.petasos.endpoints.services.tasking.PetasosTaskServicesEndpoint;
import net.fhirfactory.pegacorn.petasos.oam.metrics.agents.ProcessingPlantMetricsAgentAccessor;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.ActionableTaskCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.ActionableTaskPersistenceService;
import net.fhirfactory.pegacorn.services.tasks.datatypes.PetasosActionableTaskRegistrationType;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.Date;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

@ApplicationScoped
public class PonosTaskManagementService extends PetasosTaskServicesEndpoint implements PetasosTaskBrokerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(PonosTaskManagementService.class);

    private boolean daemonInitialised;

    private boolean firstRunComplete;

    private boolean daemonIsStillRunning;
    private Instant daemonLastRunTime;

    private static Long TASK_STATUS_MANAGEMENT_DAEMON_STARTUP_DELAY = 60000L;
    private static Long TASK_STATUS_MANAGEMENT_DAEMON_CHECK_PERIOD = 10000L;
    private static Long TASK_STATUS_MANAGEMENT_DAEMON_RESET_PERIOD = 180L;
    private static Long TASK_AGE_BEFORE_FORCED_RETIREMENT = 120L;

    @Inject
    private ActionableTaskCacheServices taskCacheServices;

    @Inject
    private ProcessingPlantMetricsAgentAccessor metricsAgentAccessor;

    @Inject
    private ActionableTaskPersistenceService taskSaveActivity;

    @Inject
    private ActionableTaskPersistenceService taskPersistenceService;

    //
    // Constructor(s)
    //

    public PonosTaskManagementService(){
        super();
        this.daemonInitialised = false;
    }

    //
    // Post Construct
    //

    public void executePostConstructActivities() {
        getLogger().debug(".initialise(): Entry");
        if (daemonInitialised) {
            getLogger().debug(".initialise(): Exit, already initialised, nothing to do");
            return;
        }
        getLogger().info(".initialise(): Initialisation Start...");

        scheduleTaskStatusManagementDaemon();

        this.daemonInitialised = true;

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
        getLogger().debug(".taskStatusManagementDaemon(): Entry");

        getLogger().debug(".taskStatusManagementDaemon(): [Perform Some Metrics Updates] Start");
        int cacheSize = taskCacheServices.getTaskCacheSize();
        metricsAgentAccessor.getMetricsAgent().updateLocalCacheStatus("ActionableTaskCache", cacheSize);
        int registrationCacheSize = taskCacheServices.getTaskRegistrationCacheSize();
        metricsAgentAccessor.getMetricsAgent().updateLocalCacheStatus("ActionableTaskRegistrationCache", registrationCacheSize);
        getLogger().debug(".taskStatusManagementDaemon(): [Perform Some Metrics Updates] Finish");

        getLogger().debug(".taskStatusManagementDaemon(): [Check Task Status] Start");
        Set<DatagridElementKeyInterface> agedCacheContent = taskCacheServices.getAgedCacheContent(TASK_AGE_BEFORE_FORCED_RETIREMENT);
        for(DatagridElementKeyInterface currentKey: agedCacheContent){
            taskCacheServices.clearTaskFromCache(currentKey);
        }
        getLogger().debug(".taskStatusManagementDaemon(): [Check Task Status] Finish");
    }

    //
    // Business Methods
    //


    //
    // Getters (and Setters)
    //

    @Override
    protected Logger specifyLogger() {
        return (LOG);
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

    protected ActionableTaskCacheServices getActionableTaskCache(){
        return(taskCacheServices);
    }

    protected ActionableTaskPersistenceService getTaskPersistenceService(){
        return(taskPersistenceService);
    }

    protected ActionableTaskPersistenceService getTaskSaveActivity(){
        return(taskSaveActivity);
    }

    //
    // RMI Handlers
    //

    public PetasosTaskJobCard registerTaskHandler(String sourceComponentName, String sourceParticipantName, PetasosActionableTask actionableTask, PetasosTaskJobCard jobCard){
        getLogger().debug(".registerTaskHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, actionableTask->{}, jobCard->{}", sourceComponentName, sourceParticipantName, actionableTask, jobCard);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".registerTaskHandler(): [Add ActionableTask to Central Cache] Start");
        PetasosTaskJobCard resultantJobCard = registerTask(actionableTask, jobCard);
        getLogger().trace(".registerTaskHandler(): [Add ActionableTask to Central Cache] Finish");

        getLogger().debug(".registerTaskHandler(): Exit, resultantJobCard->{}", resultantJobCard);
        return(resultantJobCard);
    }

    public PetasosTaskJobCard registerTaskOutcomeHandler(String sourceComponentName, String sourceParticipantName, PetasosActionableTask actionableTask, PetasosTaskJobCard jobCard){
        getLogger().debug(".registerTaskOutcomeHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, actionableTask->{}, jobCard->{}", sourceComponentName, sourceParticipantName, actionableTask, jobCard);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".registerTaskOutcomeHandler(): [Update ActionableTask to Central Cache] Start");
        PetasosTaskJobCard resultantJobCard = registerTaskOutcome(actionableTask, jobCard);
        getLogger().trace(".registerTaskOutcomeHandler(): [Update ActionableTask to Central Cache] Finish");

        getLogger().debug(".registerTaskOutcomeHandler(): Exit, resultantJobCard->{}", resultantJobCard);
        return(resultantJobCard);
    }

    public PetasosTaskJobCard registerTaskWaitingHandler(String sourceComponentName, String sourceParticipantName, PetasosTaskJobCard jobCard){
        getLogger().debug(".registerTaskWaitingHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, jobCard->{}", sourceComponentName, sourceParticipantName, jobCard);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".registerTaskWaitingHandler(): [Update JobCard in Central Cache] Start");
        PetasosTaskJobCard resultantJobCard = registerTaskWaiting(jobCard);
        getLogger().trace(".registerTaskWaitingHandler(): [Update JobCard in Central Cache] Finish");

        getLogger().debug(".registerTaskWaitingHandler(): Exit, resultantJobCard->{}", resultantJobCard);
        return(resultantJobCard);
    }

    public PetasosTaskJobCard registerTaskStartHandler(String sourceComponentName, String sourceParticipantName, PetasosTaskJobCard jobCard){
        getLogger().debug(".registerTaskStartHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, jobCard->{}", sourceComponentName, sourceParticipantName, jobCard);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".registerTaskStartHandler(): [Update JobCard in Central Cache] Start");
        PetasosTaskJobCard resultantJobCard = registerTaskStart(jobCard);
        getLogger().trace(".registerTaskStartHandler(): [Update JobCard in Central Cache] Finish");

        getLogger().debug(".registerTaskStartHandler(): Exit, resultantJobCard->{}", resultantJobCard);
        return(resultantJobCard);
    }

    public PetasosTaskJobCard registerTaskFailureHandler(String sourceComponentName, String sourceParticipantName, PetasosTaskJobCard jobCard){
        getLogger().debug(".registerTaskFailureHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, jobCard->{}", sourceComponentName, sourceParticipantName, jobCard);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".registerTaskFailureHandler(): [Update JobCard in Central Cache] Start");
        PetasosTaskJobCard resultantJobCard = registerTaskFailure(jobCard);
        getLogger().trace(".registerTaskFailureHandler(): [Update JobCard in Central Cache] Finish");

        getLogger().debug(".registerTaskFailureHandler(): Exit, resultantJobCard->{}", resultantJobCard);
        return(resultantJobCard);
    }

    public PetasosTaskJobCard registerTaskFinishHandler(String sourceComponentName, String sourceParticipantName, PetasosTaskJobCard jobCard){
        getLogger().debug(".registerTaskFinishHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, jobCard->{}", sourceComponentName, sourceParticipantName, jobCard);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".registerTaskFinishHandler(): [Update JobCard in Central Cache] Start");
        PetasosTaskJobCard resultantJobCard = registerTaskFinish(jobCard);
        getLogger().trace(".registerTaskFinishHandler(): [Update JobCard in Central Cache] Finish");

        getLogger().debug(".registerTaskFinishHandler(): Exit, resultantJobCard->{}", resultantJobCard);
        return(resultantJobCard);
    }

    public PetasosTaskJobCard registerTaskFinalisationHandler(String sourceComponentName, String sourceParticipantName, PetasosTaskJobCard jobCard){
        getLogger().debug(".registerTaskFinalisationHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, jobCard->{}", sourceComponentName, sourceParticipantName, jobCard);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".registerTaskFinalisationHandler(): [Update JobCard in Central Cache] Start");
        PetasosTaskJobCard resultantJobCard = registerTaskFinalisation(jobCard);
        getLogger().trace(".registerTaskFinalisationHandler(): [Update JobCard in Central Cache] Finish");

        getLogger().debug(".registerTaskFinalisationHandler(): Exit, resultantJobCard->{}", resultantJobCard);
        return(resultantJobCard);
    }

    public PetasosTaskJobCard registerTaskCancellationHandler(String sourceComponentName, String sourceParticipantName, PetasosTaskJobCard jobCard){
        getLogger().debug(".registerTaskCancellationHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, jobCard->{}", sourceComponentName, sourceParticipantName, jobCard);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".registerTaskCancellationHandler(): [Update JobCard in Central Cache] Start");
        PetasosTaskJobCard resultantJobCard = registerTaskCancellation(jobCard);
        getLogger().trace(".registerTaskCancellationHandler(): [Update JobCard in Central Cache] Finish");

        getLogger().debug(".registerTaskCancellationHandler(): Exit, resultantJobCard->{}", resultantJobCard);
        return(resultantJobCard);
    }

    public PetasosActionableTaskSet getOffloadedPendingTasksHandler(String sourceComponentName, String sourceParticipantName, PetasosParticipantId participantId, Integer maxNumber){
        getLogger().debug(".getAdditionalPendingTasksHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantId->{}, maxNumber->{}", sourceComponentName, sourceParticipantName, participantId, maxNumber);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".getAdditionalPendingTasksHandler(): [Retrieve Tasks from Central Cache] Start");
        PetasosActionableTaskSet taskSet = getOffloadedPendingTasks(participantId, maxNumber);
        getLogger().trace(".getAdditionalPendingTasksHandler(): [Retrieve Tasks from Central Cache] Finish");

        getLogger().debug(".getAdditionalPendingTasksHandler(): Exit, taskSet->{}", taskSet);
        return(taskSet);
    }

    public PetasosActionableTask getOffloadedPendingTaskHandler(String sourceComponentName, String sourceParticipantName, PetasosParticipantId participantId, TaskIdType additionalPendingTaskId){
        getLogger().debug(".getAdditionalPendingTaskHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantId->{}, additionalPendingTaskId->{}", sourceComponentName, sourceParticipantName, participantId, additionalPendingTaskId);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".getAdditionalPendingTaskHandler(): [Retrieve Task from Central Cache] Start");
        PetasosActionableTask task = getOffloadedPendingTask(participantId, additionalPendingTaskId);
        getLogger().trace(".getAdditionalPendingTaskHandler(): [Retrieve Task from Central Cache] Finish");

        getLogger().debug(".getAdditionalPendingTaskHandler(): Exit, task->{}", task);
        return(task);
    }

    public Boolean hasOffloadedPendingTasksHandler(String sourceComponentName, String sourceParticipantName, PetasosParticipantId participantId){
        getLogger().debug(".hasAdditionalPendingTasksHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantId->{}", sourceComponentName, sourceParticipantName, participantId);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".hasAdditionalPendingTasksHandler(): [Check if Additional Pending Tasks in Central Cache] Start");
        Boolean hasTasks = hasOffloadedPendingTasks(participantId);
        getLogger().trace(".hasAdditionalPendingTasksHandler(): [Check if Additional Pending Tasks in Central Cache] Finish");

        getLogger().debug(".hasAdditionalPendingTasksHandler(): Exit, hasTasks->{}", hasTasks);
        return(hasTasks);
    }

    public Integer offloadPendingTasksHandler(String sourceComponentName, String sourceParticipantName, PetasosParticipantId participantId, PetasosTaskIdSet tasksToBeOffloaded){
        getLogger().debug(".offloadPendingTasksHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantId->{}", sourceComponentName, sourceParticipantName, participantId);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".offloadPendingTasksHandler(): [Offload Additional Pending Tasks to Central Cache] Start");
        Integer offloadedTaskCount = offloadPendingTasks(participantId, tasksToBeOffloaded);
        getLogger().trace(".offloadPendingTasksHandler(): [Offload Additional Pending Tasks to Central Cache] Finish");

        getLogger().debug(".offloadPendingTasksHandler(): Exit, offloadedTaskCount->{}", offloadedTaskCount);
        return(offloadedTaskCount);
    }

    public PetasosTaskIdSet synchronisePendingTasksHandler(String sourceComponentName, String sourceParticipantName, PetasosParticipantId participantId, PetasosTaskIdSet tasksToBeOffloaded){
        getLogger().debug(".synchronisePendingTasksHandler(): Entry, sourceComponentName->{}, sourceParticipantName->{}, participantId->{}", sourceComponentName, sourceParticipantName, participantId);
        getMetricsAgent().incrementRPCInvocationCount(sourceParticipantName);

        getLogger().trace(".synchronisePendingTasksHandler(): [Check Pending Tasks in Central Cache] Start");
        PetasosTaskIdSet amalgamatedPendingTaskSet = synchronisePendingTasks(participantId, tasksToBeOffloaded);
        getLogger().trace(".synchronisePendingTasksHandler(): [Check Pending Tasks in Central Cache] Finish");

        getLogger().debug(".synchronisePendingTasksHandler(): Exit, amalgamatedPendingTaskSet->{}", amalgamatedPendingTaskSet);
        return(amalgamatedPendingTaskSet);
    }

    //
    // Local (Server) Business Methods
    //

    @Override
    public PetasosTaskJobCard registerTask(PetasosActionableTask actionableTask, PetasosTaskJobCard jobCard) {
        getLogger().debug(".registerTask(): Entry, actionableTask->{}, integrationPoint->{}", actionableTask);

        getLogger().trace(".registerTask(): [Add ActionableTask to Central Cache] Start");
        PetasosActionableTask registeredActionableTask = getActionableTaskCache().addTask(actionableTask);
        getLogger().trace(".registerTask(): [Add ActionableTask to Central Cache] Finish");

        PetasosTaskJobCard responseJobCard = new PetasosTaskJobCard(jobCard);
        if(!responseJobCard.hasPersistenceStatus()){
            responseJobCard.setPersistenceStatus(new TaskStorageType());
        }
        responseJobCard.getPersistenceStatus().setCentralStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
        responseJobCard.getPersistenceStatus().setCentralStorageLocation(getProcessingPlant().getTopologyNode().getParticipant().getParticipantId().getName());
        responseJobCard.setGrantedStatus(TaskExecutionCommandEnum.TASK_COMMAND_WAIT);
        responseJobCard.setUpdateInstant(Instant.now());
        getLogger().debug(".registerActionableTask(): Exit, responseJobCard->{}", responseJobCard);
        return(responseJobCard);
    }

    @Override
    public PetasosTaskJobCard registerTaskOutcome(PetasosActionableTask actionableTask, PetasosTaskJobCard jobCard) {
        getLogger().debug(".registerTaskOutcome(): Entry, actionableTask->{}, jobCard->{}", actionableTask, jobCard);
        PetasosActionableTask registeredActionableTask = getActionableTaskCache().updateTask(actionableTask, true);
        PetasosTaskJobCard responseJobCard = SerializationUtils.clone(jobCard);
        if(!responseJobCard.hasPersistenceStatus()){
            responseJobCard.setPersistenceStatus(new TaskStorageType());
        }
        responseJobCard.getPersistenceStatus().setCentralStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
        responseJobCard.getPersistenceStatus().setCentralStorageLocation(getProcessingPlant().getTopologyNode().getParticipant().getParticipantId().getName());
        responseJobCard.setGrantedStatus(TaskExecutionCommandEnum.TASK_COMMAND_CLEAN_UP);
        responseJobCard.getPersistenceStatus().setCentralStorageInstant(Instant.now());
        responseJobCard.setUpdateInstant(Instant.now());
        getLogger().debug(".registerTaskOutcome(): Entry, responseJobCard->{}", jobCard);
        return(responseJobCard);
    }

    @Override
    public PetasosTaskJobCard registerTaskWaiting( PetasosTaskJobCard jobCard) {
        getLogger().debug(".registerTaskWaiting(): Entry, jobCard->{}", jobCard);

        getLogger().trace(".registerTaskWaiting(): [Update Cached Task Status] Start");
        PetasosActionableTask cachedActionableTask = getActionableTaskCache().setTaskStatus(jobCard.getTaskId(), TaskOutcomeStatusEnum.OUTCOME_STATUS_WAITING, FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_REGISTERED, jobCard.getTaskFulfillmentCard().getFulfillmentStartInstant());
        getLogger().trace(".registerTaskWaiting(): [Update Cached Task Status] Finish");

        getLogger().trace(".registerTaskWaiting(): [Clone and Update JobCard Status] Start");
        PetasosTaskJobCard responseJobCard = SerializationUtils.clone(jobCard);
        if(!responseJobCard.hasPersistenceStatus()){
            responseJobCard.setPersistenceStatus(new TaskStorageType());
        }
        responseJobCard.getPersistenceStatus().setCentralStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
        responseJobCard.getPersistenceStatus().setCentralStorageLocation(getProcessingPlant().getTopologyNode().getParticipant().getParticipantId().getName());
        responseJobCard.setGrantedStatus(TaskExecutionCommandEnum.TASK_COMMAND_WAIT);
        responseJobCard.setUpdateInstant(Instant.now());
        getLogger().trace(".registerTaskWaiting(): [Clone and Update JobCard Status] Finish");

        getLogger().debug(".registerTaskWaiting(): Exit, responseJobCard->{}", responseJobCard);
        return(responseJobCard);
    }

    @Override
    public PetasosTaskJobCard registerTaskStart( PetasosTaskJobCard jobCard) {
        getLogger().debug(".registerTaskStart(): Entry, jobCard->{}", jobCard);

        getLogger().trace(".registerTaskStart(): [Update Cached Task Status] Start");
        PetasosActionableTask cachedActionableTask = getActionableTaskCache().setTaskStatus(jobCard.getTaskId(), TaskOutcomeStatusEnum.OUTCOME_STATUS_ACTIVE, FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_ACTIVE, jobCard.getTaskFulfillmentCard().getFulfillmentStartInstant());
        getLogger().trace(".registerTaskStart(): [Update Cached Task Status] Finish");

        getLogger().trace(".registerTaskStart(): [Clone and Update JobCard Status] Start");
        PetasosTaskJobCard responseJobCard = SerializationUtils.clone(jobCard);
        if(!responseJobCard.hasPersistenceStatus()){
            responseJobCard.setPersistenceStatus(new TaskStorageType());
        }
        responseJobCard.getPersistenceStatus().setCentralStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
        responseJobCard.getPersistenceStatus().setCentralStorageLocation(getProcessingPlant().getTopologyNode().getParticipant().getParticipantId().getName());
        responseJobCard.setGrantedStatus(TaskExecutionCommandEnum.TASK_COMMAND_EXECUTE);
        responseJobCard.setUpdateInstant(Instant.now());
        getLogger().trace(".registerTaskStart(): [Clone and Update JobCard Status] Finish");

        getLogger().debug(".registerTaskStart(): Exit, responseJobCard->{}", responseJobCard);
        return(responseJobCard);
    }

    @Override
    public PetasosTaskJobCard registerTaskFailure(PetasosTaskJobCard jobCard) {
        getLogger().debug(".registerTaskFailure(): Entry, jobCard->{}", jobCard);

        getLogger().trace(".registerTaskFailure(): [Update Cached Task Status] Start");
        PetasosActionableTask cachedActionableTask = getActionableTaskCache().setTaskStatus(jobCard.getTaskId(), TaskOutcomeStatusEnum.OUTCOME_STATUS_FAILED, FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_FAILED, jobCard.getUpdateInstant());
        getLogger().trace(".registerTaskFailure(): [Update Cached Task Status] Finish");

        getLogger().trace(".registerTaskFailure(): [Clone and Update JobCard Status] Start");
        PetasosTaskJobCard responseJobCard = SerializationUtils.clone(jobCard);
        if(!responseJobCard.hasPersistenceStatus()){
            responseJobCard.setPersistenceStatus(new TaskStorageType());
        }
        responseJobCard.getPersistenceStatus().setCentralStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
        responseJobCard.getPersistenceStatus().setCentralStorageLocation(getProcessingPlant().getTopologyNode().getParticipant().getParticipantId().getName());
        responseJobCard.setOutcomeStatus(cachedActionableTask.getTaskOutcomeStatus().getOutcomeStatus());
        responseJobCard.setGrantedStatus(TaskExecutionCommandEnum.TASK_COMMAND_FAIL);
        responseJobCard.setUpdateInstant(Instant.now());
        getLogger().trace(".registerTaskFailure(): [Clone and Update JobCard Status] Finish");

        getLogger().debug(".registerTaskFailure(): Exit, responseJobCard->{}", responseJobCard);
        return(responseJobCard);
    }

    @Override
    public PetasosTaskJobCard registerTaskFinish( PetasosTaskJobCard jobCard) {
        getLogger().debug(".registerTaskFinish(): Entry, jobCard->{}", jobCard);

        getLogger().trace(".registerTaskFinish(): [Update Cached Task Status] Start");
        PetasosActionableTask cachedActionableTask = getActionableTaskCache().setTaskStatus(jobCard.getTaskId(), TaskOutcomeStatusEnum.OUTCOME_STATUS_FINISHED, FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_FINISHED, jobCard.getUpdateInstant());
        getLogger().trace(".registerTaskFinish(): [Update Cached Task Status] Finish");

        getLogger().trace(".registerTaskFinish(): [Clone and Update JobCard Status] Start");
        PetasosTaskJobCard responseJobCard = SerializationUtils.clone(jobCard);
        if(!responseJobCard.hasPersistenceStatus()){
            responseJobCard.setPersistenceStatus(new TaskStorageType());
        }
        responseJobCard.getPersistenceStatus().setCentralStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
        responseJobCard.getPersistenceStatus().setCentralStorageLocation(getProcessingPlant().getTopologyNode().getParticipant().getParticipantId().getName());
        responseJobCard.setOutcomeStatus(cachedActionableTask.getTaskOutcomeStatus().getOutcomeStatus());
        responseJobCard.setGrantedStatus(TaskExecutionCommandEnum.TASK_COMMAND_FINISH);
        responseJobCard.setUpdateInstant(Instant.now());
        getLogger().trace(".registerTaskFinish(): [Clone and Update JobCard Status] Finish");

        getLogger().debug(".registerTaskFinish(): Exit, responseJobCard->{}", responseJobCard);
        return(responseJobCard);
    }

    @Override
    public PetasosTaskJobCard registerTaskFinalisation(PetasosTaskJobCard jobCard) {
        getLogger().debug(".registerTaskFinalisation(): Entry, jobCard->{}", jobCard);

        getLogger().trace(".registerTaskFinalisation(): [Update Cached Task Status] Start");
        PetasosActionableTask cachedActionableTask = getActionableTaskCache().setTaskStatus(jobCard.getTaskId(), TaskOutcomeStatusEnum.OUTCOME_STATUS_FINALISED, FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_FINALISED, jobCard.getUpdateInstant());
        getLogger().trace(".registerTaskFinalisation(): [Update Cached Task Status] Finish");

        getLogger().trace(".registerTaskFinalisation(): [Clone and Update JobCard Status] Start");
        PetasosTaskJobCard responseJobCard = SerializationUtils.clone(jobCard);
        if(!responseJobCard.hasPersistenceStatus()){
            responseJobCard.setPersistenceStatus(new TaskStorageType());
        }
        responseJobCard.getPersistenceStatus().setCentralStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
        responseJobCard.getPersistenceStatus().setCentralStorageLocation(getProcessingPlant().getTopologyNode().getParticipant().getParticipantId().getName());
        responseJobCard.setOutcomeStatus(cachedActionableTask.getTaskOutcomeStatus().getOutcomeStatus());
        responseJobCard.setGrantedStatus(TaskExecutionCommandEnum.TASK_COMMAND_CLEAN_UP);
        responseJobCard.setUpdateInstant(Instant.now());
        getLogger().trace(".registerTaskFinalisation(): [Clone and Update JobCard Status] Finish");

        getLogger().debug(".registerTaskFinalisation(): Exit, responseJobCard->{}", responseJobCard);
        return(responseJobCard);
    }



    @Override
    public PetasosTaskJobCard registerTaskCancellation(PetasosTaskJobCard jobCard) {
        getLogger().debug(".registerTaskCancellation(): Entry, jobCard->{}", jobCard);

        getLogger().trace(".registerTaskCancellation(): [Update Cached Task Status] Start");
        PetasosActionableTask cachedActionableTask = getActionableTaskCache().setTaskStatus(jobCard.getTaskId(), TaskOutcomeStatusEnum.OUTCOME_STATUS_CANCELLED, FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_CANCELLED, jobCard.getUpdateInstant());
        getLogger().error(".registerTaskCancellation(): [Update Cached Task Status] Finish, cachedActionableTask->{}", cachedActionableTask);

        getLogger().trace(".registerTaskCancellation(): [Clone and Update JobCard Status] Start");
        PetasosTaskJobCard responseJobCard = SerializationUtils.clone(jobCard);
        if(!responseJobCard.hasPersistenceStatus()){
            responseJobCard.setPersistenceStatus(new TaskStorageType());
        }
        responseJobCard.getPersistenceStatus().setCentralStorageStatus(TaskStorageStatusEnum.TASK_SAVED);
        responseJobCard.getPersistenceStatus().setCentralStorageLocation(getProcessingPlant().getTopologyNode().getParticipant().getParticipantId().getName());
        responseJobCard.setOutcomeStatus(cachedActionableTask.getTaskOutcomeStatus().getOutcomeStatus());
        responseJobCard.setGrantedStatus(TaskExecutionCommandEnum.TASK_COMMAND_CANCEL);
        responseJobCard.setUpdateInstant(Instant.now());
        getLogger().trace(".registerTaskCancellation(): [Clone and Update JobCard Status] Finish");

        getLogger().debug(".registerTaskCancellation(): Exit, responseJobCard->{}", responseJobCard);
        return(responseJobCard);
    }

    @Override
    public PetasosActionableTaskSet getOffloadedPendingTasks(PetasosParticipantId participantId, Integer maxNumber) {
        getLogger().debug(".getOffloadedPendingTasks(): Entry, participantId->{}", participantId);
        if (participantId == null) {
            getLogger().debug(".getOffloadedPendingTasks(): Exit, participantId is null, returning empty list");
            return (new PetasosActionableTaskSet());
        }
        PetasosActionableTaskSet waitingActionableTasksForComponent = getActionableTaskCache().getAllOffloadedPendingActionableTasks(participantId);
        getLogger().info(".getOffloadedPendingTasks(): Exit");
        return(waitingActionableTasksForComponent);
    }



    public PetasosActionableTask getOffloadedPendingTask(PetasosParticipantId participantId, TaskIdType additionalPendingTaskId) {
        return null;
    }

    @Override
    public Boolean hasOffloadedPendingTasks(PetasosParticipantId participantId) {
        getLogger().debug(".hasOffloadedPendingTasks(): Entry, participantId->{}", participantId);
        if (participantId == null) {
            getLogger().debug(".hasOffloadedPendingTasks(): Exit, participantId is null, returning empty list");
            return (false);
        }
        Boolean hasOffloadedTasks = getActionableTaskCache().hasOffloadedPendingActionableTasks(participantId);
        getLogger().info(".hasOffloadedPendingTasks(): Exit, hasOffloadedTasks->{}", hasOffloadedTasks);
        return(hasOffloadedTasks);
    }

    @Override
    public Integer offloadPendingTasks(PetasosParticipantId participantId, PetasosTaskIdSet tasksToBeOffloaded) {
        getLogger().debug(".offloadPendingTasks(): Entry, participantId->{}", participantId);
        if (participantId == null) {
            getLogger().debug(".offloadPendingTasks(): Exit, participantId is null, returning empty list");
            return (0);
        }
        Integer countOfOffloadedTasks = getActionableTaskCache().markTasksAsOffloaded(participantId, tasksToBeOffloaded);
        getLogger().info(".offloadPendingTasks(): Exit");
        return(countOfOffloadedTasks);
    }

    @Override
    public PetasosTaskIdSet synchronisePendingTasks(PetasosParticipantId participantId, PetasosTaskIdSet localPendingTaskSet) {
        getLogger().debug(".synchronisePendingTasks(): Entry, participantId->{}", participantId);
        if (participantId == null) {
            getLogger().debug(".synchronisePendingTasks(): Exit, participantId is null, returning empty list");
            return (new PetasosTaskIdSet());
        }
        PetasosTaskIdSet actualOffloadedTasks = getActionableTaskCache().synchroniseOffloadedTaskSet(participantId, localPendingTaskSet);
        getLogger().info(".synchronisePendingTasks(): Exit");
        return(actualOffloadedTasks);
    }
}
