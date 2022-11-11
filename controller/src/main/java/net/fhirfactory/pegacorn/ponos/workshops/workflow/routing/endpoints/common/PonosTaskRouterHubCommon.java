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
package net.fhirfactory.pegacorn.ponos.workshops.workflow.routing.endpoints.common;

import net.fhirfactory.pegacorn.core.model.petasos.endpoint.valuesets.PetasosEndpointFunctionTypeEnum;
import net.fhirfactory.pegacorn.petasos.endpoints.technologies.jgroups.JGroupsIntegrationPointBase;
import org.jgroups.Address;

import java.util.List;

public abstract class PonosTaskRouterHubCommon extends JGroupsIntegrationPointBase {




    //
    // Routing Helper Methods
    //

    protected Address resolveTargetAddressForTaskReceiver(String participantName){
        getLogger().debug(".resolveTargetAddressForTaskReceiver(): Entry, participantName->{}", participantName);

        if(getIPCChannel() == null){
            getLogger().debug(".resolveTargetAddressForTaskReceiver(): IPCChannel is null, exit returning (null)");
            return(null);
        }
        getLogger().trace(".resolveTargetAddressForTaskReceiver(): IPCChannel is NOT null, get updated Address set via view");
        List<Address> addressList = getAllViewMembers();
        Address foundAddress = null;
        synchronized (this.getCurrentScannedMembershipLock()) {
            getLogger().debug(".getCandidateTargetServiceAddress(): Got the Address set via view, now iterate through and see if one is suitable");
            for (Address currentAddress : addressList) {
                getLogger().debug(".resolveTargetAddressForTaskReceiver(): Iterating through Address list, current element->{}", currentAddress);
                String currentService = deriveIntegrationPointSubsystemName(currentAddress.toString());
                if (currentService.contentEquals(participantName)) {
                    if(currentAddress.toString().contains(PetasosEndpointFunctionTypeEnum.PETASOS_TASK_ROUTING_RECEIVER_ENDPOINT.getDisplayName())) {
                        getLogger().debug(".resolveTargetAddressForTaskReceiver(): Exit, A match!");
                        foundAddress = currentAddress;
                        break;
                    }
                }
            }
        }
        getLogger().debug(".resolveTargetAddressForTaskReceiver(): Exit, foundAddress->{}",foundAddress );
        return(foundAddress);
    }
}
