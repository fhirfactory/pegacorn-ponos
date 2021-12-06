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
package net.fhirfactory.pegacorn.ponos.im.cache;

import net.fhirfactory.pegacorn.core.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.PetasosActionableTask;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;
import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class PetasosTaskSharedCache {
    private static final Logger LOG = LoggerFactory.getLogger(PetasosTaskSharedCache.class);

    private DefaultCacheManager cacheManager;
    private ConfigurationBuilder cacheConfigurationBuilder;

    private Cache<TaskIdType, PetasosActionableTask> taskCache;

    @Inject
    private ProcessingPlantInterface processingPlant;

    //
    // Constructor(s)
    //

    public PetasosTaskSharedCache(){
        super();
        GlobalConfiguration globalConfig = new GlobalConfigurationBuilder().transport()
                .defaultTransport()
                .clusterName("petasos-task-cache")
                //Uses a custom JGroups stack for cluster transport.
                .addProperty("configurationFile", "petasos-task-cache.xml")
                .build();
        DefaultCacheManager cacheManager = new DefaultCacheManager(globalConfig);
        // Create a distributed cache with synchronous replication.
        cacheConfigurationBuilder = new ConfigurationBuilder();
        cacheConfigurationBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
    }

    //
    // PostConstruct
    //

    public void cacheInitialise(){
        taskCache = getCacheManager().administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).getOrCreateCache("taskCache", getCacheConfigurationBuilder().build());
    }

    //
    // Business Methods
    //


    //
    // Getters (and Setters and Specify's)
    //

    protected Logger getLogger(){
        return(LOG);
    }

    @Override
    protected String specifyConfigurationFile() {
        return null;
    }
}
