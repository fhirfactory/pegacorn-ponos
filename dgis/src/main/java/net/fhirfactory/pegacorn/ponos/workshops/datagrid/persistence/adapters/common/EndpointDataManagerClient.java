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
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.base.ResourceDataManagerClient;
import org.hl7.fhir.r4.model.Endpoint;


public abstract class EndpointDataManagerClient extends ResourceDataManagerClient {

    //
    // Constructor(s)
    //

    public EndpointDataManagerClient(){
        super();
    }

    //
    // Getters (and Setters)
    //


    //
    // Business Methods
    //

    public MethodOutcome createEndpoint(String endpointJSONString){
        getLogger().debug(".createEndpoint(): Entry, endpointJSONString->{}", endpointJSONString);
        Endpoint endpoint = getFHIRParser().parseResource(Endpoint.class, endpointJSONString);
        MethodOutcome outcome = createResource(endpoint);
        getLogger().debug(".createEndpoint(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public MethodOutcome createEndpoint(Endpoint endpoint){
        getLogger().debug(".createEndpoint(): Entry, endpoint->{}", endpoint);
        MethodOutcome outcome = createResource(endpoint);
        getLogger().debug(".createEndpoint(): Exit, outcome->{}", outcome);
        return(outcome);
    }
}
