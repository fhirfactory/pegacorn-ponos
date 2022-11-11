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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.base.http;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import ca.uhn.fhir.rest.client.interceptor.AdditionalRequestHeadersInterceptor;
import net.fhirfactory.pegacorn.util.FHIRContextUtility;
import net.fhirfactory.pegacorn.util.PegacornEnvironmentProperties;
import org.slf4j.Logger;

import javax.inject.Inject;

public abstract class DataManagerSecureProxy {
    public static final String API_KEY_HEADER_NAME = "x-api-key";
    public static final String DEFAULT_API_KEY_PROPERTY_NAME = "HAPI_API_KEY";
    private IGenericClient client;

    @Inject
    private FHIRContextUtility fhirContextUtility;

    @Inject
    private PegacornEnvironmentProperties pegacornProperties;

    //
    // Constructor(s)
    //

    public DataManagerSecureProxy(){
    }

    //
    // Abstract Methods
    //

    protected abstract Logger getLogger();

    //
    // Getters (and Setters)
    //
    
    protected FHIRContextUtility getFHIRContextUtility() {
        return(fhirContextUtility);
    }

    /**
     * @return the name of the PegacornProperties to lookup to get the value of the API Key.  Subclasses can override
     *         if they don't want to use the default value of API_KEY
     */
    protected String getApiKeyPropertyName() {
        return DEFAULT_API_KEY_PROPERTY_NAME;
    }

    protected int getSocketTimeoutInSecs() {
        int socketTimeout = pegacornProperties.getIntegerProperty("HAPI_SOCKET_TIMEOUT_IN_SECS", 30);
        return socketTimeout;
    }

    public IGenericClient getClient(){
        return client;
    }

    //
    // Business Methods
    //

    protected IGenericClient newRestfulGenericClient(String theServerBase) {
        getLogger().debug(".newRestfulGenericClient(): Entry, theServerBase --> {}", theServerBase);
        getLogger().trace(".newRestfulGenericClient(): Get the FHIRContext!");
        FhirContext contextR4 = fhirContextUtility.getFhirContext();
        getLogger().trace(".newRestfulGenericClient(): Set the ValidationMode to -NEVER-");
        contextR4.getRestfulClientFactory().setServerValidationMode(ServerValidationModeEnum.NEVER);
        contextR4.getRestfulClientFactory().setSocketTimeout(getSocketTimeoutInSecs() * 1000);
        getLogger().trace(".newRestfulGenericClient(): Get the Client");
        client = contextR4.newRestfulGenericClient(theServerBase);
        if(theServerBase.startsWith("https")) {
            getLogger().trace(".newRestfulGenericClient(): Grab the API Key from the Properties");
            String apiKey = pegacornProperties.getMandatoryProperty(getApiKeyPropertyName());
                // From https://hapifhir.io/hapi-fhir/docs/interceptors/built_in_client_interceptors.html#misc-add-headers-to-request
            getLogger().trace(".newRestfulGenericClient(): Create a new Interceptor");
            AdditionalRequestHeadersInterceptor interceptor = new AdditionalRequestHeadersInterceptor();
            getLogger().trace(".newRestfulGenericClient(): Add the API Key to the Interceptor");
            interceptor.addHeaderValue(API_KEY_HEADER_NAME, apiKey);
            getLogger().trace(".newRestfulGenericClient(): Register the Interceptor with the Client");
            client.registerInterceptor(interceptor);
        }
        getLogger().debug(".newRestfulGenericClient(): Exit, client created!");
        return client;
    }

}