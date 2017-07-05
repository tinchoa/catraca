from scapy.all import *
import netifaces as ni
load_contrib('nsh')

#####CONFIG#####
IFACE='eth0'
################

def forwardNSH(pkt):
    global count

    try:
        pkt[NSH].NSI=pkt[NSH].NSI-1
        new_src = pkt[Ether].dst
        pkt[Ether].dst=pkt[Ether].src
        pkt[Ether].src=new_src
        new_src = pkt[IP].dst
        pkt[IP].src=pkt[IP].dst
        pkt[IP].dst=new_src
        del pkt[IP].chksum
        del pkt[IP].id
        del pkt[UDP].chksum
        sendp(pkt,iface=IFACE,verbose=0)
    except:
        pass

ip = ni.ifaddresses(IFACE)[2][0]['addr']
sniff(iface=IFACE, filter='(udp port 6633) and (dst host '+ip+')', store=0, prn=forwardNSH, count=0)
