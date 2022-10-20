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
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm.base.ResourceDataManagerClient;
import org.hl7.fhir.r4.model.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class LocationDataManagerClient extends ResourceDataManagerClient {
    private static final Logger LOG = LoggerFactory.getLogger(LocationDataManagerClient.class);

    //
    // Constructor(s)
    //

    public LocationDataManagerClient(){
        super();
    }

    //
    // Getters (and Setters)
    //

    @Override
    protected Logger getLogger() {
        return (LOG);
    }

    //
    // Business Methods
    //

    public MethodOutcome createLocation(String locationJSONString){
        getLogger().debug(".createLocation(): Entry, locationJSONString->{}", locationJSONString);
        Location location = getFHIRParser().parseResource(Location.class, locationJSONString);
        MethodOutcome outcome = createResource(location);
        getLogger().debug(".createLocation(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public MethodOutcome createLocation(Location location){
        getLogger().debug(".createLocation(): Entry, location->{}", location);
        MethodOutcome outcome = createResource(location);
        getLogger().debug(".createLocation(): Exit, outcome->{}", outcome);
        return(outcome);
    }
}
