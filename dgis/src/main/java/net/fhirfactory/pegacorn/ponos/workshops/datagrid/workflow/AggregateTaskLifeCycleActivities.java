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

import net.fhirfactory.dricats.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.dricats.model.petasos.task.PetasosAggregateTask;
import net.fhirfactory.dricats.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.cache.PonosPetasosActionableTaskCacheServices;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;

@ApplicationScoped
public class AggregateTaskLifeCycleActivities extends RouteBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateTaskLifeCycleActivities.class);

    private boolean initialised;

    @Inject
    private PonosPetasosActionableTaskCacheServices taskCacheServices;

    //
    // Constructor(s)
    //

    public AggregateTaskLifeCycleActivities(){
        this.initialised = false;
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise(){

    }

    //
    // Business Methods
    //

    public PetasosAggregateTask buildAggregateTask(PetasosActionableTask triggerEvent){
        PetasosAggregateTask aggregateTask = new PetasosAggregateTask();

        return(aggregateTask);
    }

    public Instant saveAggregateTask(PetasosAggregateTask aggregateTask){


        Instant saveInstant = Instant.now();
        return(saveInstant);
    }

    public PetasosAggregateTask loadAggregateTask(TaskIdType aggregateTaskId){
        PetasosAggregateTask aggregateTask = new PetasosAggregateTask();

        return(aggregateTask);
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

    //
    // Mechanism to ensure Startup
    //

    @Override
    public void configure() throws Exception {
        String className = getClass().getSimpleName();

        from("timer://" + className + "?delay=1000&repeatCount=1")
                .routeId("DaemonTaskClass::" + className)
                .log(LoggingLevel.DEBUG, "Starting....");
    }
}
