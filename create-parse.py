import dpkt,pcap,binascii
#from dpkt import *
from ipaddr import *
import database
import urllib,json
import signal
import sys,socket
from scapy.all import *





class Parse:
	def __init__(self):
		print "start"
		self.counter=0
		self.database = database.DB()
		self.database.connection()
		self.node=socket.gethostname()

	def add_colons_to_mac( self, mac_addr ) :
	    """This function accepts a 12 hex digit string and converts it to a colon separated string"""
	    s = list()
	    for i in range(12/2) : 	# mac_addr should always be 12 chars, we work in groups of 2 chars
	        s.append( mac_addr[i*2:i*2+2] )
	    r = ":".join(s)		
	    return r

	def convert_buffer2json(self,bufe):
		buf = []
		for j in str(bufe):
			buf.append(ord(j))

		return json.dumps(buf)

	def convert_json2buffer(self,j): #retorna o buffer fazer Ether(buffer) para  logo gravar .pcap
		buf = json.loads(j)
		stringMagica=""
		for k in buf:
			stringMagica+=chr(k)

		return buffer(stringMagica)

	def generatePcap(self, param):
		pacotes = self.database.get_from_db(param)
		lista_pacotes = []
		for i in pacotes:
			print type(i[0]), i[0]
			pacote = Ether(self.convert_json2buffer(i[0]))
			lista_pacotes.append(pacote)
		wrpcap("/tmp/mycap.pcap", lista_pacotes)

	def run(self):
		self.pcap=pcap.pcap()
		for ts, buf in self.pcap:
			self.counter+=1
			eth = dpkt.ethernet.Ethernet(buf)
			srcMAC=eth.src
		 	dstMAC=eth.dst
		 	ip = eth.data
		 
			
			if eth.type == 39:
				srcIP = "NULL"
				dstIP = "NULL"
				ttl = "NULL"
				L4protocol="STP"
				sport="NULL"
				dport="NULL"
				IPv=ip.v

			elif eth.type == 2048:
			 	eth_data = eth.data
				data_type = str(type(eth_data.data)).split('.',)[-1].replace("'>", '') #getting protocol
			#if eth.type == 2:
				if data_type == "IGMP":
					L4protocol="IGMP"
					sport="NULL"
					dport="NULL"
				if data_type == "UDP":
					L4protocol="UDP"
					sport=ip.data.sport
					dport=ip.data.dport
				if data_type == "TCP":
					L4protocol="TCP"
					sport=ip.data.sport
					dport=ip.data.dport		
				
				srcIP = IPAddress(Bytes(ip.src))
			 	dstIP = IPAddress(Bytes(ip.dst))
			 	ttl = ip.ttl
			 	IPv=ip.v
				load=ip.data.data

			elif eth.type == 2054:
				L4protocol="ARP"
				srcIP = "NULL"
				dstIP = "NULL"
				ttl = "NULL"
				sport="NULL"
				dport="NULL"
				IPv="NULL"
				load=ip.unpack

			elif eth.type == 34983:
				L4protocol="UNKNOWN"
				srcIP = "NULL"
				dstIP = "NULL"
				ttl = "NULL"		
				sport="NULL"
				dport="NULL"
				IPv="NULL"
				load=ip
			else:
				L4protocol="UNKNOWN-TYPE"
				srcIP = "NULL"
				dstIP = "NULL"
				ttl = "NULL"		
				sport="NULL"
				dport="NULL"
				IPv="NULL"
				load=ip.unpack


			
			size = len(buf)
			srcMAC=self.add_colons_to_mac( binascii.hexlify(srcMAC))
			dstMAC=self.add_colons_to_mac( binascii.hexlify(dstMAC))

			packet = {			"id":self.counter,\
								"timestamp": ts,\
								"type": eth.type,\
								"IPprot" : IPv,\
								"srcIP": srcIP,\
								"dstIP": dstIP,\
								"ttl": ttl,\
								"srcMAC": srcMAC,\
								"dstMAC": dstMAC,\
								"L4protocol": L4protocol,\
								"srcPort": sport,\
								"dstPort": dport,\
								"payload": load,\
								"size": size,\
								"node": self.node\
								}


			packet = (str(self.counter),str(ts), str(eth.type), str(IPv),str(srcIP),str(dstIP),str(ttl),\
				str(srcMAC),str(dstMAC),str(L4protocol),str(sport),str(dport),self.convert_buffer2json(buf),\
				str(size),str(self.node)
				)
			'''	packet = (str(self.counter),str(ts), str(eth.type), str(IPv),str(srcIP),str(dstIP),str(ttl),\
				str(srcMAC),str(dstMAC),str(L4protocol),str(sport),str(dport),str(hexdump(load)),\
				str(size),str(self.node)
				)'''
			#print str(packet)+'\n'
			self.database.send_to_db(packet)

			
parse=Parse()
try:
	#parse.run()
	parse.generatePcap({"l4protocol":"UDP"})
except KeyboardInterrupt:
	print "Exiting"
	parse.database.close_comunication()