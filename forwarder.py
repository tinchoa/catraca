from scapy.all import *
import netifaces as ni
import traceback as tb
import sys
import httpServer
load_contrib('nsh')


#####CONFIG#####
IFACE='ens3'
VERBOSE=False
################

def forwardNSH(pkt):
    global count
    global flows
    rsp=pkt.copy()

    proto = {17:UDP, 6:TCP}

    try:
        count += 1
        if VERBOSE: 
            print str(count) + ': ' + pkt[IP].src + ' --> ' + pkt[IP].dst + ' | ' + pkt[NSH][IP].src + ' --> ' + pkt[NSH][IP].dst
            print pkt.__repr__()
        try:
            pktGRE=pkt[GRE]
            ipSrc = pktGRE[IP].src
            ipDst = pktGRE[IP].dst
            ipProto = pktGRE[IP].proto
            portSrc = pktGRE[IP][proto[ipProto]].sport
            portDst = pktGRE[IP][proto[ipProto]].dport

            fluxo1 = {'ipSrc':ipSrc, 'ipDst':ipDst, 'ipProto':ipProto, 'portSrc': portSrc, 'portDst':portDst}
            fluxo2 = {'ipSrc':ipSrc, 'ipDst':ipDst}

            if (fluxo1 in flows) or (fluxo2 in flows):
                return
        except Exception as e:
            print e
            tb.print_exc(file=sys.stdout)
        rsp[NSH].NSI=pkt[NSH].NSI-1
        rsp[Ether].dst=pkt[Ether].src
        rsp[Ether].src=pkt[Ether].dst
        rsp[IP].src=pkt[IP].dst
        rsp[IP].dst=pkt[IP].src
        del rsp[IP].chksum
        del rsp[IP].id
        del rsp[UDP].chksum
        sendp(rsp,iface=IFACE,verbose=0)
    except:
        pass

count = 0
flows=[]
server = httpServer.MyServer(flowList=flows)
server.start()
ip = ni.ifaddresses(IFACE)[2][0]['addr']
sniff(iface=IFACE, filter='(udp port 6633) and (dst host '+ip+')', store=0, prn=forwardNSH, count=0)
