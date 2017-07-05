#!/usr/bin/python

'''
   Copyright 2011 Daniel Arndt

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   Contributors:

   @author: Daniel Arndt <danielarndt@gmail.com>
'''
import logging
import math
import sys, traceback

# PLEASE READ tcp_set.py BEFORE READING THIS SOURCE, AS IT IS IMPORTED
# DIRECTLY INTO THIS NAMESPACE FOR CONVIENCE.
from tcp_state import *

# Retrieve the default logger, should have been initialized by the Flowtbag.
log = logging.getLogger()

#---------------------------------------------------------------------- Settings
FLOW_TIMEOUT = 600000000 # Flow timeout in seconds 
IDLE_THRESHOLD = 1000000
#----------------------------------------------------------------- End: Settings

def stddev(sqsum, sum, count):
    return int(math.sqrt((sqsum - (sum ** 2 / count)) / (count - 1)))

#==============================================================================#
# Begin code for Flow class                                                    #
#==============================================================================#
features = [
    'srcip',
    'srcport',
    'dstip',
    'dstport',
    'proto',
    'total_fpackets',
    'total_fvolume',
    'total_bpackets',
    'total_bvolume',
    'min_fpktl',
    'mean_fpktl',
    'max_fpktl',
    'std_fpktl',
    'min_bpktl',
    'mean_bpktl',
    'max_bpktl',
    'std_bpktl',
    'min_fiat',
    'mean_fiat',
    'max_fiat',
    'std_fiat',
    'min_biat',
    'mean_biat',
    'max_biat',
    'std_biat',
    'duration',
    'min_active',
    'mean_active',
    'max_active',
    'std_active',
    'min_idle',
    'mean_idle',
    'max_idle',
    'std_idle',
    'sflow_fpackets',
    'sflow_fbytes',
    'sflow_bpackets',
    'sflow_bbytes',
    'fpsh_cnt',
    'bpsh_cnt',
    'furg_cnt',
    'burg_cnt',
    'total_fhlen',
    'total_bhlen',
    'dscp'
]

counters = [
    'fpktl_sqsum',
    'bpktl_sqsum',
    'fiat_sum',
    'fiat_sqsum',
    'fiat_count',
    'biat_sum',
    'biat_sqsum',
    'biat_count',
    'active_start',
    'active_time',
    'active_sqsum',
    'active_count',
    'idle_time',
    'idle_sqsum',
    'idle_count',
]

class Flow:
    '''
    Represents one flow to be stored in a flowtbag.
    
    An object of this class represents one flow in a flowtbag. The Flow object 
    contains several statistics about the flow as well as stores the first 
    packet of the flow for reference.
    
    Variable naming conventions:
        Prefix - desc
        _  - Instance variable used for storing information about the flow which
             is important for calculations or identification purposes but is not
             part of the output.
    '''
    def __init__(self, pkt, id):
        '''
        Constructor. Initialize all values.
        '''
        # Set initial values
        self._id = id
        self._first_packet = pkt
        self._valid = False
        self._pdir = "f"
        self._first = pkt['time']
        self._flast = pkt['time']
        self._blast = 0
        f = { x:0 for x in features }
        c = { x:0 for x in counters }
        #------------------------------------ Basic flow identification criteria
        f['srcip'] = pkt['srcip']
        f['srcport'] = pkt['srcport']
        f['dstip'] = pkt['dstip']
        f['dstport'] = pkt['dstport']
        f['proto'] = pkt['proto']
        f['dscp'] = pkt['dscp']
        #--------------------------------------------------------------------- #
        f['total_fpackets'] = 1
        f['total_fvolume'] = pkt['len']
        f['min_fpktl'] = pkt['len']
        f['max_fpktl'] = pkt['len']
        c['fpktl_sqsum'] = (pkt['len'] ** 2)
        c['active_start'] = self._first
        if pkt['proto'] == 6:
            # TCP specific
            # Create state machines for the client and server 
            self._cstate = STATE_TCP_START() # Client state
            self._sstate = STATE_TCP_START() # Server state
            # Set TCP flag stats
            if (tcp_set(pkt['flags'], TCP_PSH)):
                f['fpsh_cnt'] = 1
            if (tcp_set(pkt['flags'], TCP_URG)):
                f['furg_cnt'] = 1
        f['total_fhlen'] = pkt['iphlen'] + pkt['prhlen']
        self.f = f
        self.c = c
        self.update_status(pkt)

    def __repr__(self):
        return "[%d:(%s,%d,%s,%d,%d)]" % \
            (self._id, 
             self.f['srcip'], 
             self.f['srcport'], 
             self.f['dstip'],
             self.f['dstport'], 
             self.f['proto'])

    def __str__(self):
        '''
        Exports the stats collected.
        '''
        # Count the last active time
        f = self.f
        c = self.c
        diff = self.get_last_time() - c['active_start']
        if diff > f['max_active']:
            f['max_active'] = diff
        if (diff < self.f['min_active'] or 
            f['min_active'] == 0
            ):
            f['min_active'] = diff
        c['active_time'] += diff
        c['active_sqsum'] += (diff ** 2)
        c['active_count'] += 1

        assert(f['total_fpackets'] > 0)
        f['mean_fpktl'] = f['total_fvolume'] / f['total_fpackets']
        # Standard deviation of packets in the forward direction
        if f['total_fpackets'] > 1:
            f['std_fpktl'] = stddev(c['fpktl_sqsum'],
                                    f['total_fvolume'],
                                    f['total_fpackets'])
        else:
            f['std_fpktl'] = 0
        # Mean packet length of packets in the packward direction
        if f['total_bpackets'] > 0:
            f['mean_bpktl'] = f['total_bvolume'] / f['total_bpackets']
        else:
            f['mean_bpktl'] = -1
        # Standard deviation of packets in the backward direction
        if f['total_bpackets'] > 1:
            f['std_bpktl'] = stddev(c['bpktl_sqsum'],
                                    f['total_bvolume'],
                                    f['total_bpackets'])
        else:
            f['std_bpktl'] = 0
        # Mean forward inter-arrival time
        # TODO: Check if we actually need c_fiat_count ?
        if c['fiat_count'] > 0:
            f['mean_fiat'] = c['fiat_sum'] / c['fiat_count']
        else:
            f['mean_fiat'] = 0
        # Standard deviation of forward inter-arrival times
        if c['fiat_count'] > 1:
            f['std_fiat'] = stddev(c['fiat_sqsum'],
                                   c['fiat_sum'],
                                   c['fiat_count'])
        else:
            f['std_fiat'] = 0
        # Mean backward inter-arrival time
        if c['biat_count'] > 0:
            f['mean_biat'] = c['biat_sum'] / c['biat_count']
        else:
            f['mean_biat'] = 0
        # Standard deviation of backward inter-arrival times
        if c['biat_count'] > 1:
            f['std_biat'] = stddev(c['biat_sqsum'],
                                   c['biat_sum'],
                                   c['biat_count'])
        else:
            f['std_biat'] = 0
        # Mean active time of the sub-flows
        if c['active_count'] > 0:
            f['mean_active'] = \
                c['active_time'] / c['active_count']
        else:
            # There should be packets in each direction if we're exporting 
            log.debug("ERR: This shouldn't happen")
            raise Exception
        # Standard deviation of active times of sub-flows
        if c['active_count'] > 1:
            f['std_active'] = stddev(c['active_sqsum'],
                                     c['active_time'],
                                     c['active_count'])
        else:
            f['std_active'] = 0
        # Mean of idle times between sub-flows
        if c['idle_count'] > 0:
            f['mean_idle'] = c['idle_time'] / c['idle_count']
        else:
            f['mean_idle'] = 0
        # Standard deviation of idle times between sub-flows
        if c['idle_count'] > 1:
            f['std_idle'] = stddev(c['idle_sqsum'],
                                   c['idle_time'],
                                   c['idle_count'])
        else:
            f['std_idle'] = 0
        # More sub-flow calculations
        if c['active_count'] > 0:
            f['sflow_fpackets'] = f['total_fpackets'] / c['active_count']
            f['sflow_fbytes']   = f['total_fvolume']  / c['active_count']
            f['sflow_bpackets'] = f['total_bpackets'] / c['active_count']
            f['sflow_bbytes']   = f['total_bvolume']  / c['active_count']
        f['duration'] = self.get_last_time() - self._first
        assert (f['duration'] >= 0)

        export = []
        append = export.append
        for feat in features:
            append(f[feat])
        return ','.join(map(str, export))
        
    def update_tcp_state(self, pkt):
        '''
        Updates the TCP connection state
        
        Checks to see if a valid TCP connection has been made. The function uses
        a finite state machine implemented through the TCP_STATE class and its 
        sub-classes.
        
        Args:
            pkt - the packet to be analyzed to update the TCP connection state

                  for the flow.
        '''
        # Update client state
        self._cstate = self._cstate.update(pkt['flags'], "f", self._pdir)
        # Update server state
        self._sstate = self._sstate.update(pkt['flags'], "b", self._pdir)

    def update_status(self, pkt):
        '''
        Updates the status of a flow, checking if the flow is a valid flow.
        
        In the case of UDP, this is a simple check upon whether at least one
        packet has been sent in each direction.
        
        In the case of TCP, the validity check is a little more complex. A valid
        TCP flow requires that a TCP connection is established in the usual way.
        Furthermore, the TCP flow is terminated when a TCP connection is closed,
        or upon a timeout defined by FLOW_TIMEOUT.
        
        Args:
            pkt - the packet to be analyzed for updating the status of the flow.
        '''
        if pkt['proto'] == 17:
            # UDP
            # Skip if already labelled valid
            if self._valid: return
            # If packet length is over 8 (size of a UDP header), then we have
            # at least one byte of data
            if pkt['len'] > 8:
                self.has_data = True
            if self.has_data and self.f['total_bpackets'] > 0:
                self._valid = True
        elif pkt['proto'] == 6:
            # TCP
            if isinstance(self._cstate, STATE_TCP_ESTABLISHED):
                hlen = pkt['iphlen'] + pkt['prhlen']
                if pkt['len'] > hlen:
                    #TODO: Why would we need a hasdata variable such as in NM?
                    self._valid = True
            if not self._valid:
                #Check validity
                pass
            self.update_tcp_state(pkt)
        else:
            raise NotImplementedError

    def get_last_time(self):
        '''
        Returns the time stamp of the most recent packet in the flow, be it the
        last packet in the forward direction, or the last packet in the backward
        direction.
        
        Reimplementation of the NetMate flowstats method 
        getLast(struct flowData_t). 
        
        Returns:
            The timestamp of the last packet.
        '''
        if (self._blast == 0):
            return self._flast
        elif (self._flast == 0):
            return self._blast
        else:
            return self._flast if (self._flast > self._blast) else self._blast

    def dumpFlow(self):
        '''
        Dumps a flow, regardless of status.

        Dumps all a flow's contents for debugging purposes.
        '''
        log.error("Dumping flow to flow_dump")

    def add(self, pkt):
        '''
        Add a packet to the current flow.
        
        This function adds the packet, provided as an argument, to the flow.
        
        Args:
            pkt: The packet to be added
        Returns:
            0 - the packet is successfully added to the flow
            1 - the flow is complete with this packet (ie. TCP connect closed)
            2 - the packet is not part of this flow. (ie. flow timeout exceeded) 
        '''
        # TODO: Robust check of whether or not the packet is part of the flow.
        now = pkt['time']
        last = self.get_last_time()
        diff = now - last
        if diff > FLOW_TIMEOUT:
            return 2
        # Ignore re-ordered packets
        if (now < last):
            log.info("Flow: ignoring reordered packet. %d < %d" %
                      (now, last))
            return 0
            #raise NotImplementedError
        # Add debugging info
#        if log.isEnabledFor(logging.DEBUG):
#            self.d_packets.append(pkt['num'])
        # OK - we're serious about this packet. Lets add it.
        #Gather some statistics
        f = self.f
        c = self.c
        len = pkt['len']
        hlen = pkt['iphlen'] + pkt['prhlen']
        assert (now >= self._first)
        # Update the global variable _pdir which holds the direction of the
        # packet currently in question.  
        if (pkt['srcip'] == self._first_packet['srcip']):
            self._pdir = "f"
        else:
            self._pdir = "b"
        # Set attributes.
        if diff > IDLE_THRESHOLD:
            # The flow has been idle previous to this packet, so calc idle time 
            # stats
            if diff > f['max_idle']:
                f['max_idle'] = diff
            if (diff < f['min_idle'] or f['min_idle'] == 0):
                f['min_idle'] = diff
            c['idle_time'] += diff
            c['idle_sqsum'] += (diff ** 2)
            c['idle_count'] += 1
            # Active time stats - calculated by looking at the previous packet
            # time and the packet time for when the last idle time ended.
            diff = last - c['active_start']
            if diff > f['max_active']:
                f['max_active'] = diff
            if diff < f['min_active'] or f['min_active'] == 0:
                f['min_active'] = diff
            c['active_time'] += diff
            c['active_sqsum'] += (diff ** 2)
            c['active_count'] += 1
            self._flast = 0
            self._blast = 0
            c['active_start'] = now
        # Set bi-directional attributes.
        if self._pdir == "f":
            # Packet is travelling in the forward direction
            # Calculate some statistics
            # Packet length
            if len < f['min_fpktl'] or f['min_fpktl'] == 0:
                f['min_fpktl'] = len
            if len > f['max_fpktl']:
                f['max_fpktl'] = len
            f['total_fvolume'] += len # Doubles up as c_fpktl_sum from NM
            c['fpktl_sqsum'] += (len ** 2)
            f['total_fpackets'] += 1
            f['total_fhlen'] += hlen
            # Interarrival time
            if self._flast > 0:
                diff = now - self._flast
                if diff < f['min_fiat'] or f['min_fiat'] == 0:
                    f['min_fiat'] = diff
                if diff > f['max_fiat']:
                    f['max_fiat'] = diff
                c['fiat_sum'] += diff
                c['fiat_sqsum'] += (diff ** 2)
                c['fiat_count'] += 1
            if pkt['proto'] == 6:
                # Packet is using TCP protocol
                if (tcp_set(pkt['flags'], TCP_PSH)):
                    f['fpsh_cnt'] += 1
                if (tcp_set(pkt['flags'], TCP_URG)):
                    f['furg_cnt'] += 1
            # Update the last forward packet time stamp
            self._flast = now
        else:
            # Packet is travelling in the backward direction, check if dscp is
            # set in this direction
            if self._blast == 0 and f['dscp'] == 0:
                # Check only first packet in backward dir, and make sure it has
                # not been set already.
                f['dscp'] = pkt['dscp']
            # Calculate some statistics
            # Packet length
            if len < f['min_bpktl'] or f['min_bpktl'] == 0:
                f['min_bpktl'] = len
            if len > f['max_bpktl']:
                f['max_bpktl'] = len
            f['total_bvolume'] += len # Doubles up as c_bpktl_sum from NM
            c['bpktl_sqsum'] += (len ** 2)
            f['total_bpackets'] += 1
            f['total_bhlen'] += hlen
            # Inter-arrival time
            if self._blast > 0:
                diff = now - self._blast
                if diff < f['min_biat'] or f['min_biat'] == 0:
                    f['min_biat'] = diff
                if diff > f['max_biat']:
                    f['max_biat'] = diff
                c['biat_sum'] += diff
                c['biat_sqsum'] += (diff ** 2)
                c['biat_count'] += 1
            if pkt['proto'] == 6:
                # Packet is using TCP protocol
                if (tcp_set(pkt['flags'], TCP_PSH)):
                    f['bpsh_cnt'] += 1
                if (tcp_set(pkt['flags'], TCP_URG)):
                    f['burg_cnt'] += 1
            # Update the last backward packet time stamp
            self._blast = now

        # Update the status (validity, TCP connection state) of the flow.
        self.update_status(pkt)            

        if (pkt['proto'] == 6 and
            isinstance(self._cstate, STATE_TCP_CLOSED) and
            isinstance(self._sstate, STATE_TCP_CLOSED)):
            return 1
        else:
            return 0
    
    def checkidle(self, time):
        return True if time - self.get_last_time() > FLOW_TIMEOUT else False
        
    def export(self):
        if self._valid:
            try:
                print self
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                log.error("Error printing flow %d which starts with packet %d" %
                          (self._id, self._first_packet['num']))
                log.error("First packet: %f Last: %f" % 
                          (self._first, self.get_last_time()))
                log.error(repr(traceback.format_exception(exc_type, 
                                                          exc_value, 
                                                          exc_traceback)))
                raise e
#--------------------------------------------------------------------- End: Flow
