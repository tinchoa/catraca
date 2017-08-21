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

   dependencies:
    pip install netifaces
    apt-get install python-libpcap

'''

import sys, traceback
import argparse
import logging
import time
import binascii as ba
import socket
import struct
import string
import pcap
from flow import Flow

#Set up default logging system.
log = logging.getLogger()
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s;%(levelname)s:: "
                              "%(message)s :: %(filename)s:%(lineno)s",
                              "%H:%M:%S")
ch.setFormatter(formatter)
log.addHandler(ch)

def sort_by_IP(t):
    '''
    Re-arrange a flow tuple to have lowest IP first, for lookup
    '''
    return (t[2], t[3], t[0], t[1], t[4]) if t[2] < t[0] else t

def dumphex(s):
    bytes = map(lambda x: '%.2x' % x, map(ord, s))
    for i in xrange(0,len(bytes)/16):
        log.error('    %s' % string.join(bytes[i*16:(i+1)*16],' '))
    log.error('    %s' % string.join(bytes[(i+1)*16:],' '))

class Flowtbag:
    '''
    classdocs
    '''
    def __init__(self, packets):
        try:
            self.count = 0
            self.flow_count = 0
            self.active_flows = {}
            for pkt in packets: self.callback(*pkt)
        except KeyboardInterrupt:
            exit(0)

    def __repr__(self):
        raise NotImplementedError()

    def __str__(self):
        return "I am a Flowtbag of size %s" % (len(self.active_flows))

    def exportAll(self):
        for flow in self.active_flows.values():
            flow.export()

    def create_flow(self, pkt, flow_tuple):
        self.flow_count += 1
        flow = Flow(pkt, self.flow_count)
        self.active_flows[flow_tuple] = flow

    def cleanup_active(self, time):
        count = 0
        for flow_tuple in self.active_flows.keys():
            flow = self.active_flows[flow_tuple]
            if flow.checkidle(time):
                #flow.export()
                del self.active_flows[flow_tuple]
                count += 1
        log.info("Cleaned up %d idle flows" % count)

    def decode_IP_layer(self, data, pkt):
        pkt['version'] = (ord(data[0]) & 0xf0) >> 4
        pkt['iphlen']  = (ord(data[0]) & 0x0f) * 4
        pkt['dscp']    = ord(data[1]) >> 2
        pkt['len']     = socket.ntohs(struct.unpack('H',data[2:4])[0])
        pkt['proto']   = ord(data[9])
        pkt['srcip']   = pcap.ntoa(struct.unpack('i',data[12:16])[0])
        pkt['dstip']   = pcap.ntoa(struct.unpack('i',data[16:20])[0])
        pkt['data']    = data[pkt['iphlen']:]

    def decode_TCP_layer(self, data, pkt):
        pkt['srcport'] = socket.ntohs(struct.unpack('H', data[0:2])[0])
        pkt['dstport'] = socket.ntohs(struct.unpack('H', data[2:4])[0])
        pkt['prhlen']  = ((ord(data[12]) & 0xf0) >> 4) * 4
        pkt['flags'] = ord(data[13]) & 0x3f

    def decode_UDP_layer(self, data, pkt):
        pkt['srcport'] = socket.ntohs(struct.unpack('H', data[0:2])[0])
        pkt['dstport'] = socket.ntohs(struct.unpack('H', data[2:4])[0])
        pkt['prhlen']  = socket.ntohs(struct.unpack('H', data[4:6])[0])

    def callback(self, pktlen, data, ts):
        '''
        The callback function to be used to process each packet

        This function is applied to each individual packet in the capture via a
        loop function in the construction of the Flowtbag.

        Args:
            pktlen -- The length of the packet
            data -- The packet payload
            ts -- The timestamp of the packet
        '''
        self.count += 1
        if not data:
            # I don't know when this happens, so I wanna know.
            raise Exception
        pkt={}
        # Check if the packet is an IP packet
        if not data[12:14] == '\x08\x00':
            #log.debug('Ignoring non-IP packet')
            return
        pkt['num'] = self.count
        if len(data) < 34:
            #Hmm, IP header seems to be too short
            raise Exception
        self.decode_IP_layer(data[14:], pkt)
        if pkt['version'] != 4:
            #Ignore non-IPv4
            return
        if pkt['proto'] == 6:
            if len(pkt['data']) < 20:
                log.info("Ignoring malformed TCP header on packet %d" %
                          (pkt['num']))
                return
            try:
                self.decode_TCP_layer(pkt['data'], pkt)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                log.error("Error reading TCP header on packet %d" %
                          (pkt['num']))
                log.error("Size: %d iphlen: %d" %
                          (len(data), pkt['iphlen']))
                log.error("TCP header size: %d" % len(pkt['data']))
                dumphex(data)
                log.error(repr(traceback.format_exception(exc_type,
                                                          exc_value,
                                                          exc_traceback)))
                raise e
        elif pkt['proto'] == 17:
            if len(pkt['data']) < 8:
                log.info("Ignoring malformed UDP header on packet %d" %
                          (pkt['num']))
                return
            try:
                self.decode_UDP_layer(pkt['data'], pkt)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                log.error("Error reading UDP header on packet %d" %
                          (pkt['num']))
                dumphex(data)
                log.error(repr(traceback.format_exception(exc_type,
                                                          exc_value,
                                                          exc_traceback)))
                raise e
        else:
            #log.debug('Ignoring non-TCP/UDP packet')
            return
        # We're really going ahead with this packet! Let's get 'er done.
        pkt['time'] = int(ts * 1000000)
        flow_tuple = (pkt['srcip'],
                      pkt['srcport'],
                      pkt['dstip'],
                      pkt['dstport'],
                      pkt['proto'])
        flow_tuple = sort_by_IP(flow_tuple)
        # Find if a flow already exists for this tuple
        if flow_tuple not in self.active_flows:
            # A flow of this tuple does not exists yet, create it.
            self.create_flow(pkt, flow_tuple)
        else:
            # A flow of this tuple already exists, add to it.
            flow = self.active_flows[flow_tuple]
            return_val = flow.add(pkt)
            if return_val == 0:
                return
            elif return_val == 1:
                #This packet ended the TCP connection. Export it.
                #flow.export()
                del self.active_flows[flow_tuple]
            elif return_val == 2:
                # This packet has been added to the wrong flow. This means the
                # previous flow has ended. We export the old flow, remove it,
                # and create a new flow.
                #flow.export()
                del self.active_flows[flow_tuple]
                self.create_flow(pkt, flow_tuple)

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='Converts a network capture '\
        'file into a comma seperated value list of integers representing ' \
        'a list of flow statistics.')
    arg_parser.add_argument('capture_file',
                            help='The capture file to be converted')
    arg_parser.add_argument('--debug',
                            dest='debug',
                            action='store_true',
                            default=False,
                            help='display debugging information')
    arg_parser.add_argument('-r',
                            dest='report',
                            type=int,
                            default=5000000,
                            help='interval (num pkts) which stats be reported')
    args = arg_parser.parse_args()
    if args.report:
        REPORT_INTERVAL = args.report
    if args.debug:
        log.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    log.debug("Flowtbag begin")
    Flowtbag(args.capture_file)
    log.debug("Flowtbag end")