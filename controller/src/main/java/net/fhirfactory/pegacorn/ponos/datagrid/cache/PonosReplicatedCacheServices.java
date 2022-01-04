/*
 * Copyright (c) 2021 Mark A. Hunter
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
package net.fhirfactory.pegacorn.ponos.datagrid.cache;

import net.fhirfactory.pegacorn.ponos.subsystem.processingplant.configuration.PonosAcolyteConfigurationFile;
import net.fhirfactory.pegacorn.ponos.subsystem.processingplant.configuration.PonosAcolyteTopologyFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class PonosReplicatedCacheServices {
    private static final Logger LOG = LoggerFactory.getLogger(PonosReplicatedCacheServices.class);

    private boolean initialised;

    private DefaultCacheManager cacheManager;
    private ConfigurationBuilder cacheConfigurationBuilder;
    private Configuration cacheConfigurationBuild;

    @Inject
    private PonosAcolyteTopologyFactory ponosAcolyteTopologyFactory;

    //
    // Constructor(s)
    //

    public PonosReplicatedCacheServices(){
        this.initialised = false;
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise() {
        getLogger().debug(".initialise(): Entry");
        if (!initialised) {
            getLogger().info(".initialise(): Initialisation Start");
            //
            // Get the Infinispan-Task-JGroups-Configuration-File
            getLogger().info(".initialise(): [Retrieve JGroups Configuration File Name] Start");
            PonosAcolyteConfigurationFile propertyFile = (PonosAcolyteConfigurationFile) ponosAcolyteTopologyFactory.getPropertyFile();
            getLogger().trace(".initialise(): [Retrieve JGroups Configuration File Name] propertyFile->{}", propertyFile);
            String configurationFileName = propertyFile.getDeploymentMode().getMultiuseInfinispanStackConfigFile();
            getLogger().debug(".initialise(): [Retrieve JGroups Configuration File Name] configurationFileName->{}", configurationFileName);
            getLogger().info(".initialise(): [Retrieve JGroups Configuration File Name] End");

            getLogger().info(".initialise(): [Initialising Infinispan Cache Manager] Start");
            GlobalConfiguration globalConfig = new GlobalConfigurationBuilder().transport()
                    .defaultTransport()
                    .clusterName("petasos-task-cache")
                    //Uses a custom JGroups stack for cluster transport.
                    .addProperty("configurationFile", configurationFileName)
                    .build();
            cacheManager = new DefaultCacheManager(globalConfig);
            // Create a distributed cache with synchronous replication.
            cacheConfigurationBuilder = new ConfigurationBuilder();
            cacheConfigurationBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
            cacheConfigurationBuild = cacheConfigurationBuilder.build();
            getLogger().info(".initialise(): [Initialising Infinispan Cache Manager] End");

            getLogger().info(".initialise(): Initialisation Finished...");
            this.initialised = true;
        }
    }

    //
    // Getters (and Setters)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    public DefaultCacheManager getCacheManager(){
        return(cacheManager);
    }

    public Configuration getCacheConfigurationBuild(){
        return(cacheConfigurationBuild);
    }
}
