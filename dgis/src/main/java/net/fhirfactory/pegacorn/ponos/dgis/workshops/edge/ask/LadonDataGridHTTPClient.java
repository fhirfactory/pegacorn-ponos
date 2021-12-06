package net.fhirfactory.pegacorn.ponos.dgis.workshops.edge.ask;

import ca.uhn.fhir.rest.api.MethodOutcome;
import net.fhirfactory.pegacorn.core.constants.systemwide.PegacornReferenceProperties;
import net.fhirfactory.pegacorn.core.interfaces.topology.PegacornTopologyFactoryInterface;
import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.componentid.TopologyNodeFDN;
import net.fhirfactory.pegacorn.core.model.petasos.ipc.PegacornCommonInterfaceNames;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.base.IPCTopologyEndpoint;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.interact.ExternalSystemIPCEndpoint;
import net.fhirfactory.pegacorn.core.model.topology.endpoints.interact.StandardInteractClientTopologyEndpointPort;
import net.fhirfactory.pegacorn.core.model.topology.nodes.external.ConnectedExternalSystemTopologyNode;
import net.fhirfactory.pegacorn.deployment.topology.manager.TopologyIM;
import net.fhirfactory.pegacorn.petasos.core.moa.wup.MessageBasedWUPEndpoint;
import net.fhirfactory.pegacorn.platform.edge.ask.base.http.InternalFHIRClientProxy;
import org.hl7.fhir.r4.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;

@ApplicationScoped
public class LadonDataGridHTTPClient extends InternalFHIRClientProxy {
    private static final Logger LOG = LoggerFactory.getLogger(LadonDataGridHTTPClient.class);

    private boolean resolvedAuditPersistenceValue;
    private boolean auditPersistence;

    @Inject
    private PegacornTopologyFactoryInterface topologyFactory;

    @Override
    protected Logger getLogger() {
        return (LOG);
    }

    @Inject
    private TopologyIM topologyIM;

    @Inject
    private PegacornReferenceProperties systemWideProperties;

    @Inject
    private ProcessingPlantInterface processingPlant;

    @Inject
    private PegacornCommonInterfaceNames interfaceNames;

    public LadonDataGridHTTPClient(){
        super();
        resolvedAuditPersistenceValue = false;
        auditPersistence = false;
    }

    @Override
    protected String deriveTargetEndpointDetails(){
        getLogger().debug(".deriveTargetEndpointDetails(): Entry");
        MessageBasedWUPEndpoint endpoint = new MessageBasedWUPEndpoint();
        StandardInteractClientTopologyEndpointPort clientTopologyEndpoint = (StandardInteractClientTopologyEndpointPort) getTopologyEndpoint(interfaceNames.getEdgeAskEndpointName());
        ConnectedExternalSystemTopologyNode targetSystem = clientTopologyEndpoint.getTargetSystem();
        ExternalSystemIPCEndpoint externalSystemIPCEndpoint = (ExternalSystemIPCEndpoint)targetSystem.getTargetPorts().get(0);
        String http_type = null;
        if(externalSystemIPCEndpoint.isEncrypted()) {
            http_type = "https";
        } else {
            http_type = "http";
        }
        String dnsName = externalSystemIPCEndpoint.getTargetPortDNSName();
        String portNumber = Integer.toString(externalSystemIPCEndpoint.getTargetPortValue());
        String endpointDetails = http_type + "://" + dnsName + ":" + portNumber + systemWideProperties.getPegacornInternalFhirResourceR4Path();
        getLogger().info(".deriveTargetEndpointDetails(): Exit, endpointDetails --> {}", endpointDetails);
        return(endpointDetails);
    }

    protected IPCTopologyEndpoint getTopologyEndpoint(String topologyEndpointName){
        getLogger().debug(".getTopologyEndpoint(): Entry, topologyEndpointName->{}", topologyEndpointName);
        ArrayList<TopologyNodeFDN> endpointFDNs = processingPlant.getProcessingPlantNode().getEndpoints();
        for(TopologyNodeFDN currentEndpointFDN: endpointFDNs){
            IPCTopologyEndpoint endpointTopologyNode = (IPCTopologyEndpoint)topologyIM.getNode(currentEndpointFDN);
            if(endpointTopologyNode.getEndpointConfigurationName().contentEquals(topologyEndpointName)){
                getLogger().debug(".getTopologyEndpoint(): Exit, node found -->{}", endpointTopologyNode);
                return(endpointTopologyNode);
            }
        }
        getLogger().debug(".getTopologyEndpoint(): Exit, Could not find node!");
        return(null);
    }

    public MethodOutcome writeAuditEvent(String auditEventJSONString){
        getLogger().debug(".writeAuditEvent(): Entry, auditEventJSONString->{}", auditEventJSONString);
        MethodOutcome outcome = null;
        if(persistAuditEvent()){
            getLogger().info(".writeAuditEvent(): Writing to Hestia-Audit-DM");
            // write the event to the Persistence service
            AuditEvent auditEvent = getFHIRContextUtility().getJsonParser().parseResource(AuditEvent.class, auditEventJSONString);
            try {
                outcome = getClient().create()
                        .resource(auditEvent)
                        .prettyPrint()
                        .encodedJson()
                        .execute();
            } catch (Exception ex){
                getLogger().error(".writeAuditEvent(): ", ex);
                outcome = new MethodOutcome();
                outcome.setCreated(false);
            }
        } else {
            getLogger().info(auditEventJSONString);
        }
        getLogger().info(".writeAuditEvent(): Exit, outcome->{}", outcome);
        return(outcome);
    }

    private boolean persistAuditEvent(){
        if(!this.resolvedAuditPersistenceValue){
            String auditEventPersistenceValue = processingPlant.getProcessingPlantNode().getOtherConfigurationParameter("AUDIT_EVENT_PERSISTENCE");
            if (auditEventPersistenceValue.equalsIgnoreCase("true")) {
                this.auditPersistence = true;
            } else {
                this.auditPersistence = false;
            }
            this.resolvedAuditPersistenceValue = true;
        }
        return(this.auditPersistence);
    }

    @Override
    protected void postConstructActivities() {

    }
}
