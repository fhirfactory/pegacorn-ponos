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
import org.hl7.fhir.r4.model.Patient;


public abstract class PatientDataManagerClient extends ResourceDataManagerClient {


    //
    // Constructor(s)
    //


    //
    // Getters (and Setters)
    //

    //
    // Business Methods
    //

    public MethodOutcome createPatient(String patientJSONString){
        getLogger().debug(".createPatient(): Entry, patientJSONString->{}", patientJSONString);
        Patient patient = getFHIRParser().parseResource(Patient.class, patientJSONString);
        MethodOutcome outcome = createResource(patient);
        getLogger().debug(".createPatient(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    public MethodOutcome createPatient(Patient patient){
        getLogger().debug(".createPatient(): Entry, patient->{}", patient);
        MethodOutcome outcome = createResource(patient);
        getLogger().debug(".createPatient(): Exit, outcome->{}", outcome);
        return(outcome);
    }
}
