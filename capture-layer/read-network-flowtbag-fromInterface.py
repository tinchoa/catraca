import threading
import Queue
import os
import netifaces as ni
import flowtbag
import sys
import json

#If Windows
if os.name == 'nt': import win_inet_pton

#Disable warnings
import logging
#logging.getLogger("scapy.runtime").setLevel(logging.ERROR)

from scapy.all import *
load_contrib("nsh")

from kafka import KafkaProducer
from kafka.errors import KafkaError

###Config###
WINDOW = 2.0
CLASS = 'live'
TEST_FILE = 'testFlowtbag.txt'
IFACE = 'ens3' #IFACE = 'eth0' #None = All
TOPIC = 'test2'.encode('utf-8')# + str(contadorTopicos)
ENCAPSULATION = None #GRE #NSH
producer = KafkaProducer(bootstrap_servers=['10.240.180.33:9092'])
#PRODUCER = KafkaProducer(bootstrap_servers=['10.10.10.3:9092'])

############

#CallBack
def removeNSH(pkt):
    global PKTS
    global ENCAPSULATION
    try:
        PKTS.append(pkt[ENCAPSULATION][1])
    except:
        pass

def noNSH(pkt):
    global PKTS
    try:
        PKTS.append(pkt)
    except:
        pass

#Send to Kafka
def salvar_fluxosKafka(flows, classe, topico, producer):
    for flow in flows.active_flows.values():
        msg = str(flow) + ',' + classe
        producer.send(topico, json.dumps(msg).encode('utf-8'))

#Send to Disk
def salvar_fluxosDisco(flows, classe):
    for flow in flows.active_flows.values():
        with open(TEST_FILE,'a') as f:
            f.write(str(flow)+','+classe+'\n')

BUFFER = Queue.Queue()
PKTS = []
def capture():
    global BUFFER
    global PKTS
    ip = ni.ifaddresses(IFACE)[2][0]['addr']
    while(1):
        #Network to flowtbag format
        PKTS = []
        sniff(timeout=WINDOW, iface=IFACE, store=0, prn=noNSH)
        pkts = [(len(pkt), str(pkt), pkt.time) for pkt in PKTS]
        BUFFER.put(pkts)

def write_flows():
    global BUFFER
    global TOPIC
    global producer 
    while (1):
        pkts = BUFFER.get()
        flows=flowtbag.Flowtbag(pkts)
        salvar_fluxosKafka(flows, CLASS, TOPIC, producer)

#Start simple threads
capture_thread = threading.Thread(target=capture)
writer_thread = threading.Thread(target=write_flows)

capture_thread.start()
writer_thread.start()
