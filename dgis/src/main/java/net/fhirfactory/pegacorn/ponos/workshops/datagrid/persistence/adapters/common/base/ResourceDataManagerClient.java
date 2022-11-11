/*
 * Copyright (c) 2021 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.base;

import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.MethodOutcome;
import net.fhirfactory.pegacorn.internals.fhir.r4.resources.bundle.BundleContentHelper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hl7.fhir.r4.model.*;

import javax.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public abstract class ResourceDataManagerClient extends DataManagerClientBase {

    @Inject
    private BundleContentHelper bundleContentHelper;

    //
    // Constructor(s)
    //

    public ResourceDataManagerClient(){
        super();
    }

    //
    // Getters (and Setters)
    //

    protected BundleContentHelper getBundleContentHelper(){
        return(bundleContentHelper);
    }

    //
    // Business Method(s)
    //

    //
    // Create Resource
    public MethodOutcome  createResource(Resource resourceToCreate){
        getLogger().debug(".createResource(): Entry, resourceToCreate->{}", resourceToCreate);
        MethodOutcome outcome = null;
        try {
            outcome = getClient().create()
                    .resource(resourceToCreate)
                    .prettyPrint()
                    .encodedJson()
                    .execute();
        } catch (Exception ex){
            getLogger().error(".createResource(): Error creating resource, stack->{}", ExceptionUtils.getStackTrace(ex));
            outcome = new MethodOutcome();
            outcome.setCreated(false);
        }
        getLogger().debug(".createResource(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    //
    // Update Resource
    public MethodOutcome updateResource(Resource resourceToUpdate){
        getLogger().debug(".updateResource(): Entry, resourceToUpdate->{}", resourceToUpdate);
        MethodOutcome outcome = null;
        try {
            outcome = getClient().update()
                    .resource(resourceToUpdate)
                    .prettyPrint()
                    .encodedJson()
                    .execute();
        } catch (Exception ex){
            getLogger().error(".updateResource(): Error updating resource, stack->{}", ExceptionUtils.getStackTrace(ex));
            outcome = new MethodOutcome();
            outcome.setCreated(false);
        }
        getLogger().debug(".updateResource(): Exit, outcome->{}", outcome);
        return(outcome);
    }


    //
    // Find Resource(s)
    /**
     *
     * @param resourceReference
     * @return
     */
    public Resource findResourceByReference(Reference resourceReference){
        CodeableConcept identifierType = resourceReference.getIdentifier().getType();
        Coding identifierCode = identifierType.getCodingFirstRep();
        String identifierCodeValue = identifierCode.getCode();
//        String identifierSystem = identifierCode.getSystem();

        Resource response = findResourceByIdentifier(resourceReference.getType(), resourceReference.getIdentifier().getSystem(), identifierCodeValue, resourceReference.getIdentifier().getValue());
        return (response);
    }

    /**
     *
     * @param resourceType
     * @param identifier
     * @return
     */
    public Resource findResourceByIdentifier(ResourceType resourceType, Identifier identifier){
        CodeableConcept identifierType = identifier.getType();
        Coding identifierCode = identifierType.getCodingFirstRep();
        String identifierCodeValue = identifierCode.getCode();
        String identifierSystem = identifierCode.getSystem();
        String identifierValue = identifier.getValue();

//        Resource response = findResourceByIdentifier(resourceType.toString(), identifierSystem, identifierCodeValue, identifierValue);
        Resource response = findResourceByIdentifier(resourceType.toString(), identifierValue);
        return (response);
    }

    /**
     *
     * @param resourceType
     * @param identifier
     * @return
     */
    public Resource findResourceByIdentifier(String resourceType, Identifier identifier){
        getLogger().debug(".findResourceByIdentifier(): Entry, resourceType --> {}", resourceType);
        String identifierValue = identifier.getValue();
        String identifierSystem = identifier.getSystem();
        String identifierCode = null;
        if(identifier.hasType()) {
            CodeableConcept identifierType = identifier.getType();
            Coding identifierTypeCode = identifierType.getCodingFirstRep();
            identifierCode = identifierTypeCode.getCode();
        }
        Resource response = findResourceByIdentifier(resourceType, identifierSystem, identifierCode, identifierValue);
        getLogger().debug(".findResourceByIdentifier(): Exit");
        return (response);
    }

    /**
     *
     * @param resourceType
     * @param identifierSystem
     * @param identifierCode
     * @param identifierValue
     * @return
     */
    public Resource findResourceByIdentifier(ResourceType resourceType, String identifierSystem, String identifierCode, String identifierValue){
        Resource response = findResourceByIdentifier(resourceType.toString(), identifierSystem, identifierCode, identifierValue);
        return (response);
    }

    /**
     *
     * @param resourceType
     * @param identifierSystem
     * @param identifierCode
     * @param identifierValue
     * @return
     */
    public Resource findResourceByIdentifier(String resourceType, String identifierSystem, String identifierCode, String identifierValue){
        getLogger().debug(".findResourceByIdentifier(): Entry, resourceType --> {}, identfierSystem --> {}, identifierCode --> {}, identifierValue -->{}", resourceType, identifierSystem, identifierCode, identifierValue);
        String urlEncodedString = null;
        if(identifierCode == null ) {
            String rawSearchString = identifierSystem + /* "|" + identifierCode + */ "|" + identifierValue;
            urlEncodedString = "identifier=" + URLEncoder.encode(rawSearchString, StandardCharsets.UTF_8);
        } else {
            String rawSearchString = identifierSystem + "|" + identifierCode + "|" + identifierValue;
            urlEncodedString = "identifier:of_type=" + URLEncoder.encode(rawSearchString, StandardCharsets.UTF_8);
        }
        String searchURL = resourceType + "?" + urlEncodedString;
        getLogger().debug(".findResourceByIdentifier(): URL --> {}", searchURL);
        Bundle response = getClient().search()
                .byUrl(searchURL)
                .returnBundle(Bundle.class)
                .execute();
        IParser r4Parser = getFHIRParser().setPrettyPrint(true);
        if(getLogger().isInfoEnabled()) {
            if(response != null) {
                getLogger().debug(".findResourceByIdentifier(): Retrieved Bundle --> {}", r4Parser.encodeResourceToString(response));
            }
        }
        Resource resource = bundleContentHelper.extractFirstRepOfType(response, resourceType);
        getLogger().debug(".findResourceByIdentifier(): Retrieved Resource --> {}", resource);
        return (resource);
    }

    public Resource findResourceByIdentifier(String resourceType, String identifierValue){
        getLogger().debug(".findResourceByIdentifier(): Entry, resourceType --> {},identifierValue -->{}", resourceType, identifierValue);
        String urlEncodedString = null;
        String rawSearchString = identifierValue;
        urlEncodedString = "identifier=" + URLEncoder.encode(rawSearchString, StandardCharsets.UTF_8);
        String searchURL = resourceType + "?" + urlEncodedString;
        getLogger().warn(".findResourceByIdentifier(): URL --> {}", searchURL);
        Bundle response = getClient()
                .search()
                .byUrl(searchURL)
                .returnBundle(Bundle.class)
                .execute();
        IParser r4Parser = getFHIRParser().setPrettyPrint(true);
        if(getLogger().isInfoEnabled()) {
            if(response != null) {
                getLogger().debug(".findResourceByIdentifier(): Retrieved Bundle --> {}", r4Parser.encodeResourceToString(response));
            }
        }
        Resource resource = bundleContentHelper.extractFirstRepOfType(response, resourceType);
        getLogger().debug(".findResourceByIdentifier(): Retrieved Resource --> {}", resource);
        return (resource);
    }
}
