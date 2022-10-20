package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm.base.http.resourcespecific;

import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenParam;
import net.fhirfactory.pegacorn.internals.fhir.r4.resources.identifier.SearchSupportHelper;
import net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.adapters.ponosdm.base.DataManagerClientBase;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@ApplicationScoped
public class PatientDataManagerSearches {
    private static final Logger LOG = LoggerFactory.getLogger(PatientDataManagerSearches.class);

    @Inject
    SearchSupportHelper searchHelper;

    public Bundle doQRYA19(DataManagerClientBase accessor, String subjectIdentifierSystem, String subjectIdentifierTypeCode, String subjectIdentifierValue, String queryString){
        String resourceType = "Patient";
        String searchQueryName = "_query=patientQRYA19";
        // 1st, the Subject
        String searchSubject = "identifier="+ URLEncoder.encode(subjectIdentifierSystem, StandardCharsets.UTF_8) + "%7C" + URLEncoder.encode(subjectIdentifierTypeCode,StandardCharsets.UTF_8) + "%7C" + URLEncoder.encode(subjectIdentifierValue, StandardCharsets.UTF_8);
        // Now the QueryString
        String encodedQueryString = "qrya19=" + URLEncoder.encode(queryString, StandardCharsets.UTF_8);
        // URL Encode
        String parameterString =  "&" + searchSubject + "&" + encodedQueryString;
        String finalString = resourceType + "?" + searchQueryName + parameterString;
        LOG.trace(".doQRYA19(): Search String --> {}", finalString);
        Bundle response = accessor.getClient()
                .search()
                .byUrl(finalString)
                .returnBundle(Bundle.class)
                .execute();
        return(response);
    }

    public Bundle doQRYA19(DataManagerClientBase accessor, TokenParam identifierParam, StringParam stringParam){
        Identifier identifier = searchHelper.tokenParam2Identifier(identifierParam);
        if(identifier.hasType()){
            String identifierSystem = identifier.getType().getCodingFirstRep().getSystem();
            String identifierValue = identifier.getValue();
            String identifierTypeCode = identifier.getType().getCodingFirstRep().getCode();
            Bundle response = doQRYA19(accessor, identifierSystem, identifierTypeCode, identifierValue, stringParam.getValue());
            return(response);
        } else {
            String identifierSystem = identifier.getSystem();
            String identifierValue = identifier.getValue();
            Bundle response = doQRYA19(accessor, identifierSystem, "RI", identifierValue, stringParam.getValue());
            return(response);
        }
    }
}
