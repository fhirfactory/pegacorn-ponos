package net.fhirfactory.pegacorn.ponos.subsystem.processingplant.configuration;

import net.fhirfactory.pegacorn.core.model.topology.nodes.*;
import net.fhirfactory.pegacorn.core.model.topology.nodes.common.EndpointProviderInterface;
import net.fhirfactory.pegacorn.deployment.properties.configurationfilebased.common.segments.ports.http.HTTPClientPortSegment;
import net.fhirfactory.pegacorn.deployment.topology.factories.archetypes.fhirpersistence.im.FHIRIMSubsystemTopologyFactory;
import net.fhirfactory.pegacorn.ponos.common.PonosNames;
import net.fhirfactory.pegacorn.util.PegacornEnvironmentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class PonosIMTopologyFactory extends FHIRIMSubsystemTopologyFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PonosIMTopologyFactory.class);

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
        return (PonosIMConfigurationFile.class);
    }

    @Override
    protected ProcessingPlantSoftwareComponent buildSubsystemTopology() {
        SubsystemTopologyNode subsystemTopologyNode = buildSubsystemNodeFromConfigurationFile();
        BusinessServiceTopologyNode businessServiceTopologyNode = buildBusinessServiceNode(subsystemTopologyNode);
        DeploymentSiteTopologyNode deploymentSiteTopologyNode = buildDeploymentSiteNode(businessServiceTopologyNode);
        ClusterServiceTopologyNode clusterServiceTopologyNode = buildClusterServiceNode(deploymentSiteTopologyNode, businessServiceTopologyNode);

        PlatformTopologyNode platformTopologyNode = buildPlatformNode(clusterServiceTopologyNode);
        ProcessingPlantSoftwareComponent processingPlantSoftwareComponent = buildProcessingPlant(platformTopologyNode, clusterServiceTopologyNode);
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
        HTTPClientPortSegment ponosDMSegment = ((PonosIMConfigurationFile) getPropertyFile()).getPonosDM();
        getHTTPTopologyEndpointFactory().newHTTPClientTopologyEndpoint(getPropertyFile(),endpointProvider,ponosNames.getPonosDMClientName(),ponosDMSegment );
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
