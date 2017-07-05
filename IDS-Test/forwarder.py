from scapy.all import *
import netifaces as ni
load_contrib('nsh')

#####CONFIG#####
IFACE='eth0'
VERBOSE=False
################

def forwardNSH(pkt):
    global count
    rsp=pkt.copy()

    try:
        count += 1
        if VERBOSE: 
            print str(count) + ': ' + pkt[IP].src + ' --> ' + pkt[IP].dst + ' | ' + pkt[NSH][IP].src + ' --> ' + pkt[NSH][IP].dst
            print pkt.__repr__()
        rsp[NSH].NSI=pkt[NSH].NSI-1
        rsp[Ether].dst=pkt[Ether].src
        rsp[Ether].src=pkt[Ether].dst
        rsp[IP].src=pkt[IP].dst
        rsp[IP].dst=pkt[IP].src
        del rsp[IP].chksum
        del rsp[IP].id
        del rsp[UDP].chksum
        sendp(rsp,iface='eth0',verbose=0)
    except:
        pass

count = 0
ip = ni.ifaddresses(IFACE)[2][0]['addr']
sniff(iface=IFACE, filter='(udp port 6633) and (dst host '+ip+')', store=0, prn=forwardNSH, count=0)
