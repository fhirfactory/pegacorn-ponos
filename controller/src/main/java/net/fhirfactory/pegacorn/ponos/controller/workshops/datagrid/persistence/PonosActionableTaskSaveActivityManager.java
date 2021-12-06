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
package net.fhirfactory.pegacorn.ponos.controller.workshops.datagrid.persistence;

import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridElementKeyInterface;
import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridEntrySaveRequestInterface;
import net.fhirfactory.pegacorn.ponos.controller.workshops.datagrid.PonosTaskCacheServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class PonosActionableTaskSaveActivityManager implements DatagridEntrySaveRequestInterface {
    private static final Logger LOG = LoggerFactory.getLogger(PonosActionableTaskSaveActivityManager.class);

    private static Long PONOS_DATAGRID_ENTRY_SAVE_SERVICE_CHECK_PERIOD = 500L;
    private static Long PONOS_DATAGRID_ENTRY_SAVE_SERVICE_START_DELAY = 30000L;

    private boolean initialised;

    private Set<DatagridElementKeyInterface> saveActivityQueue;

    @Inject
    private PonosTaskCacheServices ponosTaskCache;

    //
    // Constructor
    //

    public PonosActionableTaskSaveActivityManager(){
        this.initialised = false;
        saveActivityQueue = new HashSet<>();
    }

    //
    // Post Construct
    //

    @PostConstruct
    public void initialise(){
        getLogger().debug(".initialise(): Entry");
        if(!initialised){

            this.initialised = true;
        }
        getLogger().debug(".initialise(): Exit");
    }

    //
    // Business Methods
    //

    @Override
    public void requestDatagridEntrySave(DatagridElementKeyInterface element) {
        if(element != null){
            if(getSaveActivityQueue().contains(element)){
                // do nothing, it's already in queue
            } else {
                getSaveActivityQueue().add(element);
            }
        }

    }

    //
    // The Save Daemon
    //

    protected void datagridSaveDaemon(){
        getLogger().debug(".datagridSaveDaemon(): Entry");
        if(getSaveActivityQueue().isEmpty()){
            getLogger().debug(".datagridSaveDaemon(): Exit, SaveActivityQueue is empty");
            return;
        }

    }

    //
    // Getters (and Setters)
    //

    public Logger getLogger(){
        return(LOG);
    }

    public PonosTaskCacheServices getPonosTaskCache(){
        return(this.ponosTaskCache);
    }

    public static Long getPonosDatagridEntrySaveServiceCheckPeriod() {
        return PONOS_DATAGRID_ENTRY_SAVE_SERVICE_CHECK_PERIOD;
    }

    public static Long getPonosDatagridEntrySaveServiceStartDelay() {
        return PONOS_DATAGRID_ENTRY_SAVE_SERVICE_START_DELAY;
    }

    protected Set<DatagridElementKeyInterface> getSaveActivityQueue() {
        return saveActivityQueue;
    }
}
