package net.fhirfactory.pegacorn.ponos.subsystem.processingplant.configuration;

import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointFunctionTypeEnum;
import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointTopologyTypeEnum;
import net.fhirfactory.pegacorn.core.model.topology.nodes.*;
import net.fhirfactory.pegacorn.core.model.topology.nodes.common.EndpointProviderInterface;
import net.fhirfactory.pegacorn.deployment.properties.configurationfilebased.common.segments.ports.http.HTTPClientPortSegment;
import net.fhirfactory.pegacorn.deployment.properties.configurationfilebased.common.segments.ports.jgroups.JGroupsInterZoneRepeaterClientPortSegment;
import net.fhirfactory.pegacorn.deployment.topology.factories.archetypes.fhirpersistence.im.FHIRIMSubsystemTopologyFactory;
import net.fhirfactory.pegacorn.ponos.common.PonosNames;
import net.fhirfactory.pegacorn.util.PegacornEnvironmentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class PonosAcolyteTopologyFactory extends FHIRIMSubsystemTopologyFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PonosAcolyteTopologyFactory.class);

    @Inject
    private PonosNames ponosNames;

    @Inject
    private PegacornEnvironmentProperties pegacornEnvironmentProperties;

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    @Override
    protected Class specifyPropertyFileClass() {
        return (PonosAcolyteConfigurationFile.class);
    }

    @Override
    protected ProcessingPlantSoftwareComponent buildSubsystemTopology() {
        SubsystemTopologyNode subsystemTopologyNode = addSubsystemNode(getTopologyIM().getSolutionTopology());
        BusinessServiceTopologyNode businessServiceTopologyNode = addBusinessServiceNode(subsystemTopologyNode);
        DeploymentSiteTopologyNode deploymentSiteTopologyNode = addDeploymentSiteNode(businessServiceTopologyNode);
        ClusterServiceTopologyNode clusterServiceTopologyNode = addClusterServiceNode(deploymentSiteTopologyNode);

        PlatformTopologyNode platformTopologyNode = addPlatformNode(clusterServiceTopologyNode);
        ProcessingPlantSoftwareComponent processingPlantSoftwareComponent = addPegacornProcessingPlant(platformTopologyNode);
        addPrometheusPort(processingPlantSoftwareComponent);
        addJolokiaPort(processingPlantSoftwareComponent);
        addKubeLivelinessPort(processingPlantSoftwareComponent);
        addKubeReadinessPort(processingPlantSoftwareComponent);
        addEdgeAnswerPort(processingPlantSoftwareComponent);
        addEdgeAskPort(processingPlantSoftwareComponent);
        addAllJGroupsEndpoints(processingPlantSoftwareComponent);
        addHTTPClientPorts(processingPlantSoftwareComponent);

        return(processingPlantSoftwareComponent);
    }

    protected void addHTTPClientPorts(EndpointProviderInterface endpointProvider){
        getLogger().debug(".addHTTPClientPorts(): Entry, endpointProvider->{}", endpointProvider);

        HTTPClientPortSegment taskRouterDMSegment = ((PonosAcolyteConfigurationFile) getPropertyFile()).getTaskRouterDM();
        getHTTPTopologyEndpointFactory().newHTTPClientTopologyEndpoint(getPropertyFile(),endpointProvider,ponosNames.getPonosTaskRouterCacheDMName(),taskRouterDMSegment );

        HTTPClientPortSegment taskLoggerDMSegment = ((PonosAcolyteConfigurationFile) getPropertyFile()).getTaskLoggerDM();
        getHTTPTopologyEndpointFactory().newHTTPClientTopologyEndpoint(getPropertyFile(),endpointProvider,ponosNames.getPonosTaskLoggerDMName(),taskLoggerDMSegment );

        HTTPClientPortSegment taskReporterDMSegment = ((PonosAcolyteConfigurationFile) getPropertyFile()).getTaskReportDM();
        getHTTPTopologyEndpointFactory().newHTTPClientTopologyEndpoint(getPropertyFile(),endpointProvider,ponosNames.getPonosTaskReporterDMName(),taskReporterDMSegment );

        //
        // Petasos Task Grid Services Client Endpoint (Uno)
        JGroupsInterZoneRepeaterClientPortSegment interZoneTasking = ((PonosAcolyteConfigurationFile) getPropertyFile()).getPetasosTaskServicesEndpoint();
        String taskDataGridServerOne = getInterfaceNames().getEndpointName(PetasosEndpointTopologyTypeEnum.JGROUPS_INTEGRATION_POINT, PetasosEndpointFunctionTypeEnum.PETASOS_TASK_DISTRIBUTION_GRID_ENDPOINT_ONE.getDisplayName());
        getJGroupsTopologyEndpointFactory().newInterZoneRepeaterJGroupsIntegrationPoint(getProcessingPlantNode(), getPropertyFile(), endpointProvider, interZoneTasking, PetasosEndpointTopologyTypeEnum.JGROUPS_INTEGRATION_POINT, PetasosEndpointFunctionTypeEnum.PETASOS_TASK_DISTRIBUTION_GRID_ENDPOINT_ONE, taskDataGridServerOne, getResilienceMode(), getConcurrenceMode() );
        // Petasos Task Grid Services Client Endpoint (Duo)
        String taskDataGridServerTwo = getInterfaceNames().getEndpointName(PetasosEndpointTopologyTypeEnum.JGROUPS_INTEGRATION_POINT, PetasosEndpointFunctionTypeEnum.PETASOS_TASK_DISTRIBUTION_GRID_ENDPOINT_TWO.getDisplayName());
        getJGroupsTopologyEndpointFactory().newInterZoneRepeaterJGroupsIntegrationPoint(getProcessingPlantNode(), getPropertyFile(), endpointProvider, interZoneTasking, PetasosEndpointTopologyTypeEnum.JGROUPS_INTEGRATION_POINT, PetasosEndpointFunctionTypeEnum.PETASOS_TASK_DISTRIBUTION_GRID_ENDPOINT_TWO, taskDataGridServerTwo, getResilienceMode(), getConcurrenceMode() );


        getLogger().debug(".addHTTPClientPorts(): Exit");
    }


    protected String specifyPropertyFileName() {
        getLogger().info(".specifyPropertyFileName(): Entry");
        String configurationFileName = pegacornEnvironmentProperties.getMandatoryProperty("DEPLOYMENT_CONFIG_FILE");
        if(configurationFileName == null){
            throw(new RuntimeException("Cannot load configuration file!!!! (SUBSYSTEM-CONFIG_FILE="+configurationFileName+")"));
        }
        getLogger().info(".specifyPropertyFileName(): Exit, filename->{}", configurationFileName);
        return configurationFileName;
    }
}
