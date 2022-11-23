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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.workflow;

import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridElementKeyInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.petasos.oam.metrics.collectors.ProcessingPlantMetricsAgentAccessor;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.TaskLoggingCacheServices;
import net.fhirfactory.pegacorn.services.tasks.datatypes.PetasosActionableTaskRegistrationType;
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
public class ActionableTaskResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(ActionableTaskResourceManager.class);

    private boolean initialised;

    private boolean firstRunComplete;

    private boolean daemonIsStillRunning;
    private Instant daemonLastRunTime;

    private static Long TASK_PERSISTENCE_LIFECYCLE_DAEMON_STARTUP_DELAY = 60000L;
    private static Long TASK_PERSISTENCE_LIFECYCLE_MANAGEMENT_DAEMON_CHECK_PERIOD = 10000L;
    private static Long TASK_PERSISTENCE_LIFECYCLE_MANAGEMENT_DAEMON_RESET_PERIOD = 180L;
    private static Long TASK_AGE_BEFORE_FORCED_RETIREMENT = 120L;

    @Inject
    private TaskLoggingCacheServices taskCacheServices;

    @Inject
    private ProcessingPlantMetricsAgentAccessor metricsAgentAccessor;

//    @Inject
//    private PetasosActionableTaskLoadActivity actionableTaskLoadActivity;

//    @Inject
//    private PetasosActionableTaskSaveActivity actionableTaskSaveActivity;

    //
    // Constructor(s)
    //

    public ActionableTaskResourceManager(){
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

        scheduleTaskPersistenceLifecycleDaemon();

        this.initialised = true;

        getLogger().info(".initialise(): Initialisation Finish...");
    }

    //
    // Daemon Scheduler
    //

    private void scheduleTaskPersistenceLifecycleDaemon() {
        getLogger().debug(".scheduleTaskPersistenceLifecycleDaemon(): Entry");
        TimerTask taskPersitenceLifecycleDaemonTimerTask = new TimerTask() {
            public void run() {
                getLogger().debug(".taskStatusManagementDaemonTimerTask(): Entry");
                if (!daemonIsStillRunning) {
                    taskPersistenceLifecycleDaemon();
                    setDaemonLastRunTime(Instant.now());
                } else {
                    Long ageSinceRun = Instant.now().getEpochSecond() - getDaemonLastRunTime().getEpochSecond();
                    if (ageSinceRun > getTaskPersistenceLifecycleManagementDaemonResetPeriod()) {
                        taskPersistenceLifecycleDaemon();
                        setDaemonLastRunTime(Instant.now());
                    }
                }
                getLogger().debug(".taskStatusManagementDaemonTimerTask(): Exit");
            }
        };
        Timer timer = new Timer("TaskPersistenceLifecycleDaemonTimer");
        timer.schedule(taskPersitenceLifecycleDaemonTimerTask, getTaskPersistenceLifecycleDaemonStartupDelay(), getTaskPersistenceLifecycleManagementDaemonCheckPeriod());
        getLogger().debug(".scheduleTaskPersistenceLifecycleDaemon(): Exit");
    }

    //
    // Daemon
    //

    public void taskPersistenceLifecycleDaemon(){
        getLogger().debug(".taskPersistenceLifecycleDaemon(): Start");
        setDaemonIsStillRunning(true);


        getLogger().info(".taskPersistenceLifecycleDaemon(): Update Metrics: Start");
        int cacheSize = getTaskCache().getTaskCacheSize();
        metricsAgentAccessor.getMetricsAgent().updateLocalCacheStatus("ActionableTaskCache", cacheSize);
        getLogger().info(".taskPersistenceLifecycleDaemon(): Update Metrics: cacheSize->{}", cacheSize);
        int registrationCacheSize = taskCacheServices.getTaskRegistrationCacheSize();
        metricsAgentAccessor.getMetricsAgent().updateLocalCacheStatus("ActionableTaskRegistrationCache", registrationCacheSize);
        getLogger().info(".taskPersistenceLifecycleDaemon(): Update Metrics: registrationCacheSize->{}", registrationCacheSize);
        getLogger().info(".taskPersistenceLifecycleDaemon(): Update Metrics: Finish");

        Set<DatagridElementKeyInterface> agedCacheContent = getTaskCache().getAgedCacheContent(TASK_AGE_BEFORE_FORCED_RETIREMENT);
        for(DatagridElementKeyInterface currentKey: agedCacheContent){
            taskCacheServices.clearTaskFromCache(currentKey);
        }
        setDaemonIsStillRunning(false);
        setDaemonLastRunTime(Instant.now());
        getLogger().debug(".taskPersistenceLifecycleDaemon(): Finish");
    }

    //
    // Persist Task Lifecycle

    protected void persistTask(DatagridElementKeyInterface taskId){

    }

    protected void retireTask(DatagridElementKeyInterface taskId){

    }

    //
    // Load Task

    protected PetasosActionableTaskRegistrationType loadTask(TaskIdType taskId){

        return(null);
    }

    protected PetasosActionableTaskRegistrationType loadTask(PetasosActionableTask taskId){

        return(null);
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger() {
        return (LOG);
    }

    protected TaskLoggingCacheServices getTaskCache() {
        return (taskCacheServices);
    }

    protected Instant getDaemonLastRunTime() {
        return (daemonLastRunTime);
    }

    protected void setDaemonLastRunTime(Instant instant){
        this.daemonLastRunTime = instant;
    }

    protected static Long getTaskPersistenceLifecycleDaemonStartupDelay() {
        return TASK_PERSISTENCE_LIFECYCLE_DAEMON_STARTUP_DELAY;
    }

    protected static Long getTaskPersistenceLifecycleManagementDaemonCheckPeriod() {
        return TASK_PERSISTENCE_LIFECYCLE_MANAGEMENT_DAEMON_CHECK_PERIOD;
    }

    protected static Long getTaskPersistenceLifecycleManagementDaemonResetPeriod() {
        return TASK_PERSISTENCE_LIFECYCLE_MANAGEMENT_DAEMON_RESET_PERIOD;
    }

    protected static Long getTaskAgeBeforeForcedRetirement() {
        return TASK_AGE_BEFORE_FORCED_RETIREMENT;
    }

    protected boolean isInitialised() {
        return initialised;
    }

    protected void setInitialised(boolean initialised) {
        this.initialised = initialised;
    }

    protected boolean isFirstRunComplete() {
        return firstRunComplete;
    }

    protected void setFirstRunComplete(boolean firstRunComplete) {
        this.firstRunComplete = firstRunComplete;
    }

    protected boolean isDaemonIsStillRunning() {
        return daemonIsStillRunning;
    }

    protected void setDaemonIsStillRunning(boolean daemonIsStillRunning) {
        this.daemonIsStillRunning = daemonIsStillRunning;
    }
}
