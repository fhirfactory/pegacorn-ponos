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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.services;

import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.ProvenanceDataManagerClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.TaskDataManagerClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.tasklogstore.TaskLogDeviceDMClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.tasklogstore.TaskLogProvenanceDMClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.tasklogstore.TaskLogTaskDMClient;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.services.common.ActionableTaskPersistenceServiceCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class TaskLoggerFHIRTaskService extends ActionableTaskPersistenceServiceCommon {
    private final static Logger LOG = LoggerFactory.getLogger(TaskLoggerFHIRTaskService.class);

    @Inject
    private TaskLogDeviceDMClient deviceFHIRClient;

    @Inject
    private TaskLogTaskDMClient taskFHIRClient;

    @Inject
    private TaskLogProvenanceDMClient provenanceFHIRClient;


    //
    // Constructor(s)
    //

    public TaskLoggerFHIRTaskService(){
        super();

    }

    //
    // Post Construct
    //

     //
     // Getters (and Setters)
     //

    @Override
    protected Logger getLogger(){
        return(LOG);
    }

    @Override
    protected ProvenanceDataManagerClient getProvenanceFHIRClient() {
        return (provenanceFHIRClient);
    }

    @Override
    protected TaskDataManagerClient getTaskFHIRClient() {
        return (taskFHIRClient);
    }
}
