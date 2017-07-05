import dpkt
import StringIO
import threading
import Queue
import os

#If Windows
if os.name == 'nt': import win_inet_pton

#Disable warnings
import logging
logging.getLogger("scapy.runtime").setLevel(logging.ERROR)

from scapy.all import *
load_contrib("nsh")

from read_tcpdump import *

###Config###
WINDOW = 2.0
CHARACTERISTICS = ['qtd_pacotes_tcp', 'qtd_src_port', 'qtd_dst_port', 'qtd_fin_flag', 'qtd_syn_flag', 'qtd_psh_flag', 'qtd_ack_flag', 'qtd_urg_flag', 'qtd_pacotes_udp', 'qtd_pacotes_icmp',
                       'qtd_pacotes_ip', 'qtd_tos', 'ttl_medio', 'header_len_medio', 'packet_len_medio', 'qtd_do_not_frag', 'qtd_more_frag','fragment_offset_medio', 'qtd_rst_flag',
                       'qtd_ece_flag', 'qtd_cwr_flag', 'offset_medio', 'qtd_tipos_icmp', 'qtd_codigo_icmp']
CLASS = 'live'
TEST_FILE = 'test.txt'
IFACE = 'eth0' #IFACE = 'eth0' #None = All
############

#CallBack
def removeNSH(pkt):
    global PKTS
    try:
        PKTS.append(pkt[NSH][1])
    except:
        pass

BUFFER = Queue.Queue()
PKTS = []
def capture():
    global BUFFER
    global PKTS
    while(1):
        #Network to pcap in memory
        PKTS = []
        sniff(timeout=WINDOW, iface=IFACE, filter='udp port 6633', store=0, prn=removeNSH)
        pkt = PacketList(PKTS)
        buffer = StringIO.StringIO()
        PcapWriter(buffer).write(pkt)
        buffer.seek(0)
        pcap = dpkt.pcap.Reader(buffer)
        BUFFER.put(pcap)

def write_flows():
    global BUFFER
    while (1):
        #Adapted from Antonio: read_tcpdump.py
        pcap = BUFFER.get()
        contador = 0
        lista_fluxos = []
        fluxos = {}
        for ts, data in pcap:
            eth = dpkt.ethernet.Ethernet(data)
            if contador == 0:
                tempo_inicio = ts
                fluxos = {}
            if ts > tempo_inicio + WINDOW:
                lista_fluxos.append(fluxos)
                fluxos = {}
                tempo_inicio = ts
            fluxos = atualizar_fluxos(eth, fluxos, tempo_inicio, WINDOW)
            contador += 1
            if len(lista_fluxos) > 100:
                salvar_fluxos(TEST_FILE,lista_fluxos, CHARACTERISTICS,CLASS)
                lista_fluxos = []
                # print ts, len(data)
                # print 'Timestamp: ', str(datetime.datetime.utcfromtimestamp(ts))
        if fluxos != {}:
            lista_fluxos.append(fluxos)
        salvar_fluxos(TEST_FILE, lista_fluxos, CHARACTERISTICS, CLASS)

#Start simple threads
capture_thread = threading.Thread(target=capture)
writer_thread = threading.Thread(target=write_flows)

capture_thread.start()
writer_thread.start()
