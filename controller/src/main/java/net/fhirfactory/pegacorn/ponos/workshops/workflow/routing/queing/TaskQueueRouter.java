package net.fhirfactory.pegacorn.ponos.workshops.workflow.routing.queing;

import net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants;
import net.fhirfactory.pegacorn.core.model.petasos.participant.PetasosParticipantControlStatusEnum;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.completion.datatypes.TaskCompletionSummaryType;
import net.fhirfactory.pegacorn.core.model.petasos.task.queue.ParticipantTaskQueueEntry;
import net.fhirfactory.pegacorn.petasos.core.tasks.management.daemon.DaemonBase;
import net.fhirfactory.pegacorn.platform.edge.model.router.TaskRouterResponsePacket;
import net.fhirfactory.pegacorn.platform.edge.model.router.TaskRouterStatusPacket;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.ParticipantCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.TaskRoutingCacheServices;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.queues.CentralTaskQueueMap;
import net.fhirfactory.pegacorn.ponos.workshops.workflow.routing.endpoints.PonosTaskRouterHubSender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import static net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants.PARTICIPANT_QUEUE_TASK_BATCH_SIZE;

@ApplicationScoped
public class TaskQueueRouter extends DaemonBase {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueueRouter.class);

    private static Integer MAX_CACHE_SIZE=500;
    private boolean initialised;

    private boolean taskForwarderDaemonBusy;
    private Instant taskForwarderDaemonActivityInstant;
    private Long taskForwarderDaemonStartupDelay;
    private Long taskForwarderDaemonPeriod;
    private Long taskForwarderDaemonResetPeriod;
    private Long maxTasksPerDaemonRun;
    private Long retryOnErrorDelay;



    @Inject
    private PonosTaskRouterHubSender taskSender;

    @Inject
    private TaskRoutingCacheServices taskCacheServices;

    @Inject
    private ParticipantCacheServices participantCacheServices;

    @Inject
    private CentralTaskQueueMap taskQueueMap;

    //
    // Constructor(s)
    //

    public TaskQueueRouter(){
        this.taskForwarderDaemonPeriod = PetasosPropertyConstants.PONOS_TASK_FORWARDER_DAEMON_PERIOD;
        this.taskForwarderDaemonResetPeriod = PetasosPropertyConstants.PONOS_TASK_FORWARDER_DAEMON_RESET_PERIOD;
        this.taskForwarderDaemonStartupDelay = PetasosPropertyConstants.PONOS_TASK_FORWARDER_DAEMON_STARTUP_DELAY;
        this.retryOnErrorDelay = PetasosPropertyConstants.PONOS_TASK_FORWARDER_RETRY_DELAY;
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise(){
        getLogger().debug(".initialise(): Entry");
        if(this.initialised){
            getLogger().debug(".initialise(): Exit, already initialised, nothing to do!");
            return;
        } else {
            getLogger().info("GlobalPetasosTaskContinuityWatchdog::initialise(): Starting initialisation");
            scheduleTaskForwarderDaemon();
            getLogger().info("GlobalPetasosTaskContinuityWatchdog::initialise(): Finished initialisation");
            this.initialised = true;
            getLogger().debug(".initialise(): Exit");
        }
    }
    //
    // Getters and Setters
    //

    protected Logger getLogger(){
        return(LOG);
    }

    protected PonosTaskRouterHubSender getTaskSender(){
        return(taskSender);
    }

    protected ParticipantCacheServices getParticipantCacheServices(){
        return(participantCacheServices);
    }

    protected CentralTaskQueueMap getTaskQueueMap(){
        return(taskQueueMap);
    }

    protected TaskRoutingCacheServices getTaskCacheServices(){
        return(taskCacheServices);
    }

    public boolean isTaskForwarderDaemonBusy() {
        return taskForwarderDaemonBusy;
    }

    public void setTaskForwarderDaemonBusy(boolean taskForwarderDaemonBusy) {
        this.taskForwarderDaemonBusy = taskForwarderDaemonBusy;
    }

    public Instant getTaskForwarderDaemonActivityInstant() {
        return taskForwarderDaemonActivityInstant;
    }

    public void setTaskForwarderDaemonActivityInstant(Instant taskForwarderDaemonActivityInstant) {
        this.taskForwarderDaemonActivityInstant = taskForwarderDaemonActivityInstant;
    }

    public Long getTaskForwarderDaemonStartupDelay() {
        return taskForwarderDaemonStartupDelay;
    }

    public void setTaskForwarderDaemonStartupDelay(Long taskForwarderDaemonStartupDelay) {
        this.taskForwarderDaemonStartupDelay = taskForwarderDaemonStartupDelay;
    }

    public Long getTaskForwarderDaemonPeriod() {
        return taskForwarderDaemonPeriod;
    }

    public void setTaskForwarderDaemonPeriod(Long taskForwarderDaemonPeriod) {
        this.taskForwarderDaemonPeriod = taskForwarderDaemonPeriod;
    }

    public Long getTaskForwarderDaemonResetPeriod() {
        return taskForwarderDaemonResetPeriod;
    }

    public void setTaskForwarderDaemonResetPeriod(Long taskForwarderDaemonResetPeriod) {
        this.taskForwarderDaemonResetPeriod = taskForwarderDaemonResetPeriod;
    }

    public Long getRetryOnErrorDelay() {
        return retryOnErrorDelay;
    }

    public void setRetryOnErrorDelay(Long retryOnErrorDelay) {
        this.retryOnErrorDelay = retryOnErrorDelay;
    }

    //
    // Business Methods
    //

    /**
     * This method "forwards" the given PetasosActionableTask (actionableTask) into the task distribution service.
     * @param actionableTask
     */
    public boolean sendQueuedTask(String subsystemName, PetasosActionableTask actionableTask){
        getLogger().debug(".sendQueuedTask(): Entry, subsystemName->{}, actionableTask->{}", subsystemName, actionableTask);
        TaskRouterResponsePacket taskRouterResponsePacket = null;
        if(StringUtils.isEmpty(subsystemName)){
            getLogger().error(".sendQueuedTask(): Cannot send task, target subsystemName is empty, task->{}", actionableTask);
            return(false);
        }
        if(actionableTask == null){
            getLogger().debug(".sendQueuedTask(): Cannot send task, no task to send.... ");
            return(true);
        }
        boolean messageSent = false;
        taskRouterResponsePacket = getTaskSender().forwardTask(subsystemName, actionableTask);
        if(taskRouterResponsePacket != null) {
            if (taskRouterResponsePacket.getRoutedTaskId() != null && taskRouterResponsePacket.getSuccessorTaskId() != null && taskRouterResponsePacket.getParticipantStatus().equals(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED)) {
                if (!actionableTask.hasTaskCompletionSummary()) {
                    actionableTask.setTaskCompletionSummary(new TaskCompletionSummaryType());
                }
                actionableTask.getTaskCompletionSummary().addDownstreamTask(taskRouterResponsePacket.getSuccessorTaskId());
                actionableTask.getTaskCompletionSummary().setLastInChain(false);
                actionableTask.getTaskCompletionSummary().setFinalised(true);
                getTaskCacheServices().finaliseTask(subsystemName, actionableTask);
                messageSent = true;
            }
            getParticipantCacheServices().setParticipantStatus(subsystemName, taskRouterResponsePacket.getParticipantStatus());
            getParticipantCacheServices().setParticipantCacheSize(subsystemName, taskRouterResponsePacket.getLocalCacheSize());
            getParticipantCacheServices().touchParticipantActivityTimestamp(subsystemName);
        } else {
            getParticipantCacheServices().setParticipantStatus(subsystemName, PetasosParticipantControlStatusEnum.PARTICIPANT_IS_IN_ERROR);
        }
        getParticipantCacheServices().touchParticipantActivityTimestamp(subsystemName);

        getLogger().debug(".sendQueuedTask(): Exit");
        return(messageSent);
    }


    //
    // Router Daemon
    //

    public void scheduleTaskForwarderDaemon() {
        getLogger().debug(".scheduleTaskForwarderDaemon(): Entry");
        TimerTask taskForwarderDaemonTask = new TimerTask() {
            public void run() {
                getLogger().debug(".taskForwarderDaemonTask(): Entry");
                if(isTaskForwarderDaemonBusy()){
                    // do nothing
                } else {
                    taskForwarderDaemon();
                }
                getLogger().debug(".taskForwarderDaemonTask(): Exit");
            }
        };
        Timer timer = new Timer("actionableTaskCleanupActivityTimer");
        timer.schedule(taskForwarderDaemonTask, getTaskForwarderDaemonStartupDelay(), getTaskForwarderDaemonPeriod());
        getLogger().debug(".scheduleTaskForwarderDaemon(): Exit");
    }

    public void taskForwarderDaemon(){
        getLogger().debug(".taskForwarderDaemon(): Entry");
        setTaskForwarderDaemonBusy(true);
        try {
            Set<String> participantNameList = getTaskQueueMap().getParticipants();

            boolean hasMoreTasksToForward = true;
            while (hasMoreTasksToForward) {
                setTaskForwarderDaemonActivityInstant(Instant.now());
                hasMoreTasksToForward = false;
                boolean couldForward = false;
                for (String currentParticipant : participantNameList) {
                    PetasosParticipantControlStatusEnum participantStatus = getParticipantCacheServices().getParticipantStatus(currentParticipant);
                    if(participantStatus == null) {
                        participantStatus = PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED;
                        getParticipantCacheServices().setParticipantStatus(currentParticipant, participantStatus);
                    }
                    switch (getParticipantCacheServices().getParticipantStatus(currentParticipant)) {
                        case PARTICIPANT_IS_ENABLED:
                            if (getParticipantCacheServices().getParticipantCacheSize(currentParticipant) > MAX_CACHE_SIZE) {
                                TaskRouterStatusPacket status = getTaskSender().getStatus(currentParticipant);
                                getParticipantCacheServices().setParticipantStatus(currentParticipant, status.getParticipantStatus());
                                getParticipantCacheServices().setParticipantCacheSize(currentParticipant, status.getLocalCacheSize());
                            }
                            if (getParticipantCacheServices().getParticipantCacheSize(currentParticipant) < MAX_CACHE_SIZE) {
                                forwardParticipantTasks(currentParticipant);
                                couldForward = true;
                            }
                            break;
                        case PARTICIPANT_IS_IN_ERROR:
                            if (Instant.now().isAfter(getParticipantCacheServices().getParticipantActivityTimestamp(currentParticipant).plusMillis(getRetryOnErrorDelay()))) {
                                TaskRouterStatusPacket status = getTaskSender().getStatus(currentParticipant);
                                if (status.getParticipantStatus().equals(PetasosParticipantControlStatusEnum.PARTICIPANT_IS_ENABLED)) {
                                    forwardParticipantTasks(currentParticipant);
                                    couldForward = true;
                                } else {
                                    getParticipantCacheServices().touchParticipantActivityTimestamp(currentParticipant);
                                }
                            }
                            break;
                        case PARTICIPANT_IS_SUSPENDED:
                            break;
                        case PARTICIPANT_IS_DISABLED:
                            break;
                    }
                    if (couldForward) {
                        if (getParticipantCacheServices().getParticipantCacheSize(currentParticipant) > 0) {
                            hasMoreTasksToForward = true;
                        }
                    }
                }
            }
        } catch (Exception ex){
            getLogger().error(".taskForwarderDaemon(): An Exception Occurred ->", ex);
        }
        setTaskForwarderDaemonBusy(false);
        getLogger().debug(".taskForwarderDaemon(): Entry");
    }

    public Integer forwardParticipantTasks(String participantName){
        getLogger().debug(".forwardParticipantTasks(): Entry, participantName->{}", participantName);

        int forwardedTaskCount = 0;
        while(forwardedTaskCount < PARTICIPANT_QUEUE_TASK_BATCH_SIZE){
            ParticipantTaskQueueEntry currentQueueEntry = getTaskQueueMap().peekNextTask(participantName);
            if(currentQueueEntry == null){
                break;
            }
            PetasosActionableTask task = getTaskCacheServices().getTask(participantName, currentQueueEntry.getTaskId(), currentQueueEntry);
            if(task != null) {
                boolean taskSent = sendQueuedTask(participantName, task);
                if(taskSent){
                    getTaskQueueMap().pollNextTask(participantName);
                }
            }
            forwardedTaskCount += 1;
        }

        getLogger().debug(".forwardParticipantTasks(): Exit, Number of Tasks Forwarded->{}",forwardedTaskCount);
        return(forwardedTaskCount);
    }



}
