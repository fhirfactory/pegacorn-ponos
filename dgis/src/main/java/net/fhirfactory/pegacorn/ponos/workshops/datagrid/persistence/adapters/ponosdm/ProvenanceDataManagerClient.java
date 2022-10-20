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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm;

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.gclient.ICriterion;
import ca.uhn.fhir.rest.gclient.ReferenceClientParam;
import net.fhirfactory.pegacorn.ponos.common.PonosNames;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm.base.ResourceDataManagerClient;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class ProvenanceDataManagerClient extends ResourceDataManagerClient {
    private static final Logger LOG = LoggerFactory.getLogger(ProvenanceDataManagerClient.class);

    private static final int MAX_NUMBER_OF_PROVENANCE_INSTANCES = 100;

    @Inject
    private PonosNames ponosNames;

    //
    // Constructor(s)
    //

    public ProvenanceDataManagerClient(){
        super();
    }

    //
    // Getters (and Setters)
    //

    @Override
    protected Logger getLogger() {
        return (LOG);
    }

    @Override
    protected String specifyEndpointName(){
        return(ponosNames.getPonosDMClientName());
    }

    //
    // Business Methods
    //

    public MethodOutcome createProvenance(String provenanceJSONString){
        getLogger().debug(".createProvenance(): Entry, provenanceJSONString->{}", provenanceJSONString);
        Provenance provenance = getFHIRParser().parseResource(Provenance.class, provenanceJSONString);
        MethodOutcome outcome = createResource(provenance);
        getLogger().debug(".createProvenance(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public MethodOutcome createProvenance(Provenance provenance){
        getLogger().debug(".createProvenance(): Entry, provenance->{}", provenance);
        MethodOutcome outcome = createResource(provenance);
        getLogger().debug(".createProvenance(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public MethodOutcome updateProvenance(Provenance provenance){
        getLogger().debug(".updateProvenance(): Entry, provenance->{}", provenance);
        MethodOutcome outcome = updateResource(provenance);
        getLogger().debug(".updateProvenance(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public List<Provenance> findByReferenceToTask(IIdType taskId){
        getLogger().debug(".findByReferenceToTask(): Entry, taskId->{}", taskId);

        Bundle provenanceForTask = getProvenanceForTask(taskId, MAX_NUMBER_OF_PROVENANCE_INSTANCES, 0);

        List<Provenance> provenanceList = new ArrayList<>();
        if(provenanceForTask == null){
            return(provenanceList);
        }

        if(provenanceForTask.hasEntry()) {
            for (Bundle.BundleEntryComponent bundleEntry : provenanceForTask.getEntry()) {
                if (bundleEntry.getResource().getResourceType().equals(ResourceType.Provenance)) {
                    Provenance currentProvenanceEntry = (Provenance) bundleEntry.getResource();
                    provenanceList.add(currentProvenanceEntry);
                }
            }
        }

        getLogger().debug(".findByReferenceToTask(): Exit, provenanceList->{}", provenanceList);
        return(provenanceList);
    }

    //
    // Searches
    //

    public Bundle getProvenanceForTask(IIdType taskId, Integer count, Integer offSet){
        getLogger().debug(".getWaitingTasks(): taskId->{}, count->{}, offSet->{}", taskId, count, offSet);
        ReferenceClientParam targetParam = new ReferenceClientParam("target");
        ICriterion<ReferenceClientParam> targetTask = targetParam.hasId(taskId);
        LOG.trace(".getWaitingTasks(): Search targetTask->{}", targetTask);
        Bundle response = getClient()
                .search()
                .forResource(Provenance.class)
                .where(targetTask)
                .count(count)
                .offset(offSet)
                .returnBundle(Bundle.class)
                .prettyPrint()
                .execute();
        getLogger().debug(".getWaitingTasks(): Exit, response->{}", response);
        return(response);
    }

    public Bundle getProvenanceForTask( String taskId, Integer count, Integer offSet){
        getLogger().debug(".getWaitingTasks(): taskId->{}, count->{}, offSet->{}", taskId, count, offSet);
        ReferenceClientParam targetParam = new ReferenceClientParam("target");
        ICriterion<ReferenceClientParam> targetTask = targetParam.hasId(taskId);
        LOG.trace(".getWaitingTasks(): Search targetTask->{}", targetTask);
        Bundle response = getClient()
                .search()
                .forResource(Provenance.class)
                .where(targetTask)
                .count(count)
                .offset(offSet)
                .returnBundle(Bundle.class)
                .prettyPrint()
                .execute();
        getLogger().debug(".getWaitingTasks(): Exit, response->{}", response);
        return(response);
    }
}
