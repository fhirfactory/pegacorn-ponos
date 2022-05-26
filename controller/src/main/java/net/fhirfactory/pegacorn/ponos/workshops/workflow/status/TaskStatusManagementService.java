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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.status;

import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridElementKeyInterface;
import net.fhirfactory.pegacorn.petasos.oam.metrics.agents.ProcessingPlantMetricsAgent;
import net.fhirfactory.pegacorn.petasos.oam.metrics.agents.ProcessingPlantMetricsAgentAccessor;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.PonosPetasosActionableTaskCacheServices;
import net.fhirfactory.pegacorn.services.tasks.cache.PetasosActionableTaskDM;
import net.fhirfactory.pegacorn.services.tasks.manager.PetasosTaskServicesManagerHandler;
import org.apache.camel.LoggingLevel;
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
public class TaskStatusManagementService extends PetasosTaskServicesManagerHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStatusManagementService.class);

    private boolean initialised;

    private boolean firstRunComplete;

    private boolean daemonIsStillRunning;
    private Instant daemonLastRunTime;

    private static Long TASK_STATUS_MANAGEMENT_DAEMON_STARTUP_DELAY = 60000L;
    private static Long TASK_STATUS_MANAGEMENT_DAEMON_CHECK_PERIOD = 10000L;
    private static Long TASK_STATUS_MANAGEMENT_DAEMON_RESET_PERIOD = 180L;
    private static Long TASK_AGE_BEFORE_FORCED_RETIREMENT = 120L;

    @Inject
    private PonosPetasosActionableTaskCacheServices taskCacheServices;

    @Inject
    private ProcessingPlantMetricsAgentAccessor metricsAgentAccessor;

    //
    // Constructor(s)
    //

    public TaskStatusManagementService(){
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



    //
    // Getters (and Setters)
    //

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    @Override
    protected PetasosActionableTaskDM specifyActionableTaskCache() {
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

}
