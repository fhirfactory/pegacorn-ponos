package net.fhirfactory.pegacorn.ponos.metadata;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.parser.IParser;
import io.github.linuxforhealth.hl7.HL7ToFHIRConverter;
import net.fhirfactory.pegacorn.util.FHIRContextUtility;

@ApplicationScoped
public class TaskMetadataService {
    
    private static final Logger LOG = LoggerFactory.getLogger(TaskMetadataService.class);

    @Inject
    private FHIRContextUtility fhirContextUtility;
    
    private HL7ToFHIRConverter converter;
    private IParser fhirParser;
    
    @PostConstruct
    private void initialise() {
        //TODO check the ConverterOptions and see if we should change any
        converter = new HL7ToFHIRConverter();
        fhirParser = fhirContextUtility.getJsonParser();
    }
    
    public List<Resource> extractTaskMetadata(String hl7TriggerEvent) {
        LOG.debug(".extractTaskMetadata(): Entry");
        
        List<Resource> fhirResources = new ArrayList<>();
        
        try {
            // use the HL7-FHIR-Converter to convert out input HL7 into FHIR JSON
            String fhirBundleJson = converter.convert(hl7TriggerEvent);
            
            // convert our JSON into a FHIR Bundle
            Bundle bundle = fhirParser.parseResource(Bundle.class, fhirBundleJson);
        
            // pull our collection out of the FHIR Bundle to return as a list
            for (BundleEntryComponent entry : bundle.getEntry()) {
                fhirResources.add(entry.getResource());
            }
        } catch (UnsupportedOperationException e) {
            LOG.warn(".extractTaskMetadata(): Mapping of HL7 message type not supported", e);
        } catch (DataFormatException e) {
            LOG.error(".extractTaskMetadata(): Could not parse output for HL7 to FHIR Conversion into FHIR Bundle", e);
        } catch (Exception e) {
            // just log all remaining exceptions
            LOG.error(".extractTaskMetadata(): Unexpected exception", e);
        }
        
        LOG.debug(".extractTaskMetadata(): Exit");
        return fhirResources;
    }
}
