/*
 * Copyright (c) 2020 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common;

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.gclient.ICriterion;
import ca.uhn.fhir.rest.gclient.TokenClientParam;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.fulfillment.valuesets.FulfillmentExecutionStatusEnum;
import net.fhirfactory.pegacorn.internals.fhir.r4.resources.task.factories.TaskStatusReasonFactory;
import net.fhirfactory.pegacorn.ponos.common.PonosNames;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.base.ResourceDataManagerClient;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;


public abstract class TaskDataManagerClient extends ResourceDataManagerClient {
    private static final Logger LOG = LoggerFactory.getLogger(TaskDataManagerClient.class);

    @Inject
    private TaskStatusReasonFactory businessStatusFactory;

    @Inject
    private PonosNames ponosNames;

    //
    // Constructor(s)
    //

    public TaskDataManagerClient(){
        super();
    }

    //
    // Getters (and Setters)
    //

    protected TaskStatusReasonFactory getBusinessStatusFactory(){
        return(businessStatusFactory);
    }

    //
    // Business Methods
    //

    public MethodOutcome createTask(String taskJSONString){
        getLogger().debug(".postTask(): Entry, taskJSONString->{}", taskJSONString);
        Task task = getFHIRParser().parseResource(Task.class, taskJSONString);
        MethodOutcome outcome = createResource(task);
        getLogger().debug(".postTask(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public MethodOutcome createTask(Task task){
        getLogger().debug(".postTask(): Entry, task->{}", task);
        MethodOutcome outcome = createResource(task);
        getLogger().debug(".postTask(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public MethodOutcome updateTask(Task task){
        getLogger().debug(".updateTask(): Entry, task->{}", task);
        MethodOutcome outcome = updateResource(task);
        getLogger().debug(".updateTask(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    //
    // Searches
    //

    public Bundle getWaitingTasks( Integer count, Integer offSet){
        getLogger().debug(".getWaitingTasks(): count->{}, offSet->{}", count, offSet);
        TokenClientParam waitingTaskParam = new TokenClientParam("status");
        ICriterion<TokenClientParam> statusIsOnHold = waitingTaskParam.exactly().code(Task.TaskStatus.ONHOLD.toCode());
        LOG.trace(".getWaitingTasks(): Search statusIsOnHold->{}", statusIsOnHold);
        Bundle response = getClient()
                .search()
                .forResource(Task.class)
                .where(statusIsOnHold)
                .count(count)
                .offset(offSet)
                .returnBundle(Bundle.class)
                .prettyPrint()
                .execute();
        getLogger().debug(".getWaitingTasks(): Exit, response->{}", response);
        return(response);
    }

    public Bundle getFinishedButNotFinalisedTasks( Integer count, Integer offSet){
        getLogger().debug(".getFinishedButNotFinalisedTasks(): count->{}, offSet->{}", count, offSet);
        TokenClientParam completedTaskParam = new TokenClientParam("status");
        ICriterion<TokenClientParam> statusIsOnHold = completedTaskParam.exactly().code(Task.TaskStatus.COMPLETED.toCode());
        TokenClientParam unfinalisedBusinesStatusTaskParam = new TokenClientParam("business-status");
        CodeableConcept finishedBusinessStatus = getBusinessStatusFactory().newTaskStatusReason(FulfillmentExecutionStatusEnum.FULFILLMENT_EXECUTION_STATUS_FINISHED);
        ICriterion<TokenClientParam> businessStatusFinished = unfinalisedBusinesStatusTaskParam.exactly().systemAndCode(getBusinessStatusFactory().getPegacornTaskStatusReasonSystem(), finishedBusinessStatus.getCodingFirstRep().getCode());
        LOG.trace(".getFinishedButNotFinalisedTasks(): Search statusIsOnHold->{}, finishedBusinessStatus->{}", statusIsOnHold, finishedBusinessStatus);
        Bundle response = getClient()
                .search()
                .forResource(Task.class)
                .where(statusIsOnHold).and(businessStatusFinished)
                .count(count)
                .offset(offSet)
                .returnBundle(Bundle.class)
                .prettyPrint()
                .execute();
        getLogger().debug(".getFinishedButNotFinalisedTasks(): Exit, response->{}", response);
        return(response);
    }
}
