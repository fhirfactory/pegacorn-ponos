package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.base.http.resourcespecific;

import ca.uhn.fhir.rest.param.DateRangeParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.util.DateUtils;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.common.base.DataManagerClientBase;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class ProvenanceDataManagerSearches {
    private static final Logger LOG = LoggerFactory.getLogger(ProvenanceDataManagerSearches.class);

    public Bundle getProceduresWithSubjectAndDate(DataManagerClientBase accessor, Map<Property, Serializable> parameterSet) {
        boolean hasPatientIdentifierParam = false;
        boolean hasDataRangeParam = false;
        TokenParam patientIdentifierParam = null;
        Set<Property> propertyList = parameterSet.keySet();
        for (Property currentProperty : propertyList) {
            if (currentProperty.getName().contentEquals("subject")) {
                Serializable currentElement = parameterSet.get(currentProperty);
                if (currentElement instanceof TokenParam) {
                    patientIdentifierParam = (TokenParam) currentElement;
                    hasPatientIdentifierParam = true;
                    break;
                }
            }
        }
        DateRangeParam dateRangeParam = null;
        for (Property currentProperty : propertyList) {
            if (currentProperty.getName().contentEquals("performed")) {
                Serializable currentElement = parameterSet.get(currentProperty);
                if (currentElement instanceof DateRangeParam) {
                    dateRangeParam = (DateRangeParam) currentElement;
                    hasDataRangeParam = true;
                    break;
                }
            }
        }
        if (!(hasDataRangeParam && hasPatientIdentifierParam)) {
            return(null);
        }
        // Building the Search String
        String identifierValue = null;
        String identifierTypeCode = null;
        String identifierValueRaw = patientIdentifierParam.getValue();
        if(identifierValueRaw.contains("|")){
            String[] splitString = identifierValueRaw.split("\\|");
            identifierValue = splitString[1];
            identifierTypeCode = splitString[0];
        } else {
            identifierValue = identifierValueRaw;
        }
        String identifierTypeSystem = patientIdentifierParam.getSystem();
        Date startDate = dateRangeParam.getLowerBoundAsInstant();
        Date endDate = dateRangeParam.getUpperBoundAsInstant();
        Bundle response = getProceduresWithSubjectAndDate(accessor, identifierTypeSystem, identifierTypeCode, identifierValue, startDate, endDate);
        return(response);
    }

    public Bundle getProceduresWithSubjectAndDate(DataManagerClientBase accessor, String subjectIdentifierSystem, String subjectIdentifierTypeCode, String subjectIdentifierValue, Date startDate, Date endDate){
        String resourceType = "Procedure";
        String searchQueryName = "_query=searchProceduresForPatientDuringPeriod";
        // 1st, the Subject
        String searchSubject = "subject="+ URLEncoder.encode(subjectIdentifierSystem, StandardCharsets.UTF_8) + "%7C" + URLEncoder.encode(subjectIdentifierTypeCode,StandardCharsets.UTF_8) + "%7C" + URLEncoder.encode(subjectIdentifierValue, StandardCharsets.UTF_8);
        // Now the Date Range
        Date upperDate;
            if (endDate == null) {
            upperDate = Date.from(Instant.now());
        } else {
            upperDate = endDate;
        }
        String dateRange = "date=gt" + URLEncoder.encode(DateUtils.convertDateToIso8601String(startDate),StandardCharsets.UTF_8) + "&date=lt" + URLEncoder.encode(DateUtils.convertDateToIso8601String(upperDate));
        // URL Encode
        String parameterString =  "&" + searchSubject + "&" + dateRange;
        String finalString = resourceType + "?" + searchQueryName + parameterString;
        LOG.trace(".getProceduresWithSubjectAndDate(): Search String --> {}", finalString);
        Bundle response = accessor.getClient()
                .search()
                .byUrl(finalString)
                .returnBundle(Bundle.class)
                .execute();
            return(response);
    }

    public Bundle getProceduresWithSubjectAndDate(DataManagerClientBase accessor, Identifier identifier, DateRangeParam dateRangeParam){
        String identifierTypeCode = null;
        String identifierTypeSystem = null;
        String identifierValue = null;
        Date startDate = null;
        Date endDate = null;
        if(identifier.hasType()){
            identifierTypeCode = identifier.getType().getCodingFirstRep().getCode();
            identifierTypeSystem = identifier.getType().getCodingFirstRep().getSystem();
        } else {
            identifierTypeSystem = identifier.getSystem();
        }
        identifierValue = identifier.getValue();
        startDate = dateRangeParam.getLowerBoundAsInstant();
        endDate = dateRangeParam.getUpperBoundAsInstant();
        Bundle result = getProceduresWithSubjectAndDate(accessor, identifierTypeSystem, identifierTypeCode, identifierValue, startDate, endDate);
        return(result);
    }
}
