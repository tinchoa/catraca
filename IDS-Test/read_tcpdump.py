import dpkt
import datetime
import socket
import inspect
import sys
import json
# http://www.commercialventvac.com/dpkt.html#mozTocId761132
# https://dpkt.readthedocs.org/en/latest/_modules/examples/print_packets.html#mac_addr

def mac_addr(mac_string):
	return ':'.join('%02x' % ord(b) for b in mac_string)


def ip_to_str(address):
	return socket.inet_ntop(socket.AF_INET, address)

def print_packet(eth):
	"""Args:
		   packet: eth = dpkt.ethernet.Ethernet(data)
	"""
	# print 'Timestamp: ', str(datetime.datetime.utcfromtimestamp(timestamp))
	# Unpack the Ethernet frame (mac src/dst, ethertype)
	print 'Ethernet Frame: ', mac_addr(eth.src), mac_addr(eth.dst), eth.type
	# Make sure the Ethernet frame contains an IP packet
	# EtherType (IP, ARP, PPPoE, IP6... see http://en.wikipedia.org/wiki/EtherType)
	if eth.type != dpkt.ethernet.ETH_TYPE_IP:
		print 'Non IP Packet type not supported %s' % eth.data.__class__.__name__
		return
	# Now unpack the data within the Ethernet frame (the IP packet)
	# Pulling out src, dst, length, fragment info, TTL, and Protocol
	ip = eth.data
	# Pull out fragment information (flags and offset all packed into off field, so use bitmasks)
	do_not_fragment = bool(ip.off & dpkt.ip.IP_DF)
	more_fragments = bool(ip.off & dpkt.ip.IP_MF)
	fragment_offset = ip.off & dpkt.ip.IP_OFFMASK
	# Print out the info
	print 'IP: %s -> %s   (len=%d ttl=%d DF=%d MF=%d offset=%d)' % \
		  (ip_to_str(ip.src), ip_to_str(ip.dst), ip.len, ip.ttl, do_not_fragment, more_fragments, fragment_offset)

def informacoes_ip(pacote_ip):
	informacoes = {}
	informacoes['ip_origem'] = ip_to_str(pacote_ip.src)
	informacoes['ip_destino'] = ip_to_str(pacote_ip.dst)
	informacoes['id'] = pacote_ip.id
	informacoes['tos'] = pacote_ip.tos
	informacoes['ttl'] = pacote_ip.ttl
	informacoes['packet_len'] = pacote_ip.len
	informacoes['header_len'] = pacote_ip.hl
	informacoes['do_not_fragment'] = bool(pacote_ip.off & dpkt.ip.IP_DF)
	informacoes['more_fragments'] = bool(pacote_ip.off & dpkt.ip.IP_MF)
	informacoes['fragment_offset'] = pacote_ip.off & dpkt.ip.IP_OFFMASK
	return informacoes

def descobrir_tipo(pacote_ip):
	if type(pacote_ip.data) == dpkt.tcp.TCP:
		return 'tcp'
	if type(pacote_ip.data) == dpkt.udp.UDP:
		return 'udp'
	if type(pacote_ip.data) == dpkt.icmp.ICMP:
		return 'icmp'
	return 'invalido'

def informacoes_tcp(pacote_tcp):
	informacoes = {}
	informacoes['src_port'] = pacote_tcp.sport
	informacoes['dst_port'] = pacote_tcp.dport
	informacoes['fin_flag'] = ( pacote_tcp.flags & dpkt.tcp.TH_FIN ) != 0
	informacoes['syn_flag'] = ( pacote_tcp.flags & dpkt.tcp.TH_SYN ) != 0
	informacoes['rst_flag'] = ( pacote_tcp.flags & dpkt.tcp.TH_RST ) != 0
	informacoes['psh_flag'] = ( pacote_tcp.flags & dpkt.tcp.TH_PUSH) != 0
	informacoes['ack_flag'] = ( pacote_tcp.flags & dpkt.tcp.TH_ACK ) != 0
	informacoes['urg_flag'] = ( pacote_tcp.flags & dpkt.tcp.TH_URG ) != 0
	informacoes['ece_flag'] = ( pacote_tcp.flags & dpkt.tcp.TH_ECE ) != 0
	informacoes['cwr_flag'] = ( pacote_tcp.flags & dpkt.tcp.TH_CWR ) != 0
	informacoes['offset'] = pacote_tcp.off
	return informacoes

def informacoes_udp(pacote_udp):
	informacoes = {}
	informacoes['src_port'] = pacote_udp.sport
	informacoes['dst_port'] = pacote_udp.dport
	return informacoes

def informacoes_icmp(pacote_icmp):
	informacoes = {}
	informacoes['type'] = pacote_icmp.type
	informacoes['code'] = pacote_icmp.code
	return informacoes

def inicializar_fluxo(tempo_inicio, tempo_fim):
	fluxo = {}
	fluxo['inicio'] = tempo_inicio
	fluxo['fim'] = tempo_fim
	fluxo['qtd_pacotes_ip'] = 0
	fluxo['qtd_pacotes_tcp'] = 0
	fluxo['qtd_pacotes_udp'] = 0
	fluxo['qtd_pacotes_icmp'] = 0
	fluxo['qtd_tos'] = 0
	fluxo['tos'] = []
	fluxo['ttl_medio'] = 0
	fluxo['ttl_total'] = 0
	fluxo['header_len_medio'] = 0
	fluxo['header_len_total'] = 0
	fluxo['packet_len_medio'] = 0
	fluxo['packet_len_total'] = 0
	fluxo['qtd_do_not_frag'] = 0
	fluxo['qtd_more_frag'] = 0
	fluxo['fragment_offset_medio'] = 0
	fluxo['fragment_offset_total'] = 0
	fluxo['qtd_src_port'] = 0
	fluxo['src_port'] = []
	fluxo['qtd_dst_port'] = 0
	fluxo['dst_port'] = []
	fluxo['qtd_fin_flag'] = 0
	fluxo['qtd_syn_flag'] = 0
	fluxo['qtd_rst_flag'] = 0
	fluxo['qtd_psh_flag'] = 0
	fluxo['qtd_ack_flag'] = 0
	fluxo['qtd_urg_flag'] = 0
	fluxo['qtd_ece_flag'] = 0
	fluxo['qtd_cwr_flag'] = 0
	fluxo['offset_medio'] = 0
	fluxo['offset_total'] = 0
	fluxo['qtd_tipos_icmp'] = 0
	fluxo['tipos_icmp'] = []
	fluxo['qtd_codigo_icmp'] = 0
	fluxo['codigos_icmp'] = []
	return fluxo

def atualizar_fluxos(pacote, fluxos, tempo_inicio, janela):
	if pacote.type != dpkt.ethernet.ETH_TYPE_IP:
		return fluxos
	pacote_ip = pacote.data
	inf_ip = informacoes_ip(pacote_ip)
	if descobrir_tipo(pacote_ip) == 'invalido':
		return fluxos
	identificador = inf_ip['ip_origem'] + ' -> ' + inf_ip['ip_destino']
	if identificador not in fluxos:
		fluxos[identificador] = inicializar_fluxo(tempo_inicio, tempo_inicio + janela)
	fluxos[identificador]['qtd_pacotes_ip'] += 1
	if inf_ip['tos'] not in fluxos[identificador]['tos']:
		fluxos[identificador]['tos'].append(inf_ip['tos'])
		fluxos[identificador]['qtd_tos'] += 1
	fluxos[identificador]['ttl_total'] += inf_ip['ttl']
	fluxos[identificador]['ttl_medio'] = (1.0 * fluxos[identificador]['ttl_total'])/fluxos[identificador]['qtd_pacotes_ip']
	fluxos[identificador]['header_len_total'] += inf_ip['header_len']
	fluxos[identificador]['header_len_medio'] = (1.0 * fluxos[identificador]['header_len_total'])/fluxos[identificador]['qtd_pacotes_ip']
	fluxos[identificador]['packet_len_total'] += inf_ip['packet_len']
	fluxos[identificador]['packet_len_medio'] = (1.0 * fluxos[identificador]['packet_len_total'])/fluxos[identificador]['qtd_pacotes_ip']
	fluxos[identificador]['fragment_offset_total'] += inf_ip['fragment_offset']
	fluxos[identificador]['fragment_offset_medio'] = (1.0 * fluxos[identificador]['fragment_offset_total'])/fluxos[identificador]['qtd_pacotes_ip']
	if inf_ip['do_not_fragment'] == True:
		fluxos[identificador]['qtd_do_not_frag'] += 1
	if inf_ip['more_fragments'] == True:
		fluxos[identificador]['qtd_more_frag'] += 1
	if descobrir_tipo(pacote_ip) == 'tcp':
		fluxos[identificador]['qtd_pacotes_tcp'] += 1
		pacote_tcp = pacote_ip.data
		inf_tcp = informacoes_tcp(pacote_tcp)
		if inf_tcp['src_port'] not in fluxos[identificador]['src_port']:
			fluxos[identificador]['src_port'].append(inf_tcp['src_port'])
			fluxos[identificador]['qtd_src_port'] += 1
		if inf_tcp['dst_port'] not in fluxos[identificador]['dst_port']:
			fluxos[identificador]['dst_port'].append(inf_tcp['dst_port'])
			fluxos[identificador]['qtd_dst_port'] += 1
		if inf_tcp['fin_flag'] == True:
			fluxos[identificador]['qtd_fin_flag'] += 1
		if inf_tcp['syn_flag'] == True:
			fluxos[identificador]['qtd_syn_flag'] += 1
		if inf_tcp['rst_flag'] == True:
			fluxos[identificador]['qtd_rst_flag'] += 1
		if inf_tcp['psh_flag'] == True:
			fluxos[identificador]['qtd_psh_flag'] += 1
		if inf_tcp['ack_flag'] == True:
			fluxos[identificador]['qtd_ack_flag'] += 1
		if inf_tcp['urg_flag'] == True:
			fluxos[identificador]['qtd_urg_flag'] += 1
		if inf_tcp['ece_flag'] == True:
			fluxos[identificador]['qtd_fin_flag'] += 1
		if inf_tcp['cwr_flag'] == True:
			fluxos[identificador]['qtd_cwr_flag'] += 1
		fluxos[identificador]['offset_total'] += inf_tcp['offset']
		fluxos[identificador]['offset_medio'] = (1.0 * fluxos[identificador]['offset_total'])/fluxos[identificador]['qtd_pacotes_tcp']
	elif descobrir_tipo(pacote_ip) == 'icmp':
		fluxos[identificador]['qtd_pacotes_icmp'] += 1
		pacote_icmp = pacote_ip.data
		inf_icmp = informacoes_icmp(pacote_icmp)
		if inf_icmp['type'] not in fluxos[identificador]['tipos_icmp']:
			fluxos[identificador]['tipos_icmp'].append(inf_icmp['type'])
			fluxos[identificador]['qtd_tipos_icmp'] += 1
		if inf_icmp['code'] not in fluxos[identificador]['codigos_icmp']:
			fluxos[identificador]['codigos_icmp'].append(inf_icmp['code'])
			fluxos[identificador]['qtd_codigo_icmp'] += 1
	else:
		fluxos[identificador]['qtd_pacotes_udp'] += 1
		pacote_udp = pacote_ip.data
		inf_udp = informacoes_udp(pacote_udp)
		if inf_udp['src_port'] not in fluxos[identificador]['src_port']:
			fluxos[identificador]['src_port'].append(inf_udp['src_port'])
			fluxos[identificador]['qtd_src_port'] += 1
		if inf_udp['dst_port'] not in fluxos[identificador]['dst_port']:
			fluxos[identificador]['dst_port'].append(inf_udp['dst_port'])
			fluxos[identificador]['qtd_dst_port'] += 1
	return fluxos

def salvar_fluxos(nome_arquivo, lista_fluxos, caracteristicas, classe):
	arquivo_escrita = open(nome_arquivo, 'a')
	for fluxos in lista_fluxos:
		for identificador, fluxo in fluxos.items():
			for caracteristica in caracteristicas:
				arquivo_escrita.write(str(fluxo[caracteristica]) + ',')
			arquivo_escrita.write(str(classe) + '\n')
	arquivo_escrita.close()



def salvar_fluxosKafka(lista_fluxos, caracteristicas, classe,topico, producer):
	mensagem = None
	for fluxos in lista_fluxos:
		mensagempPre=[]
		for identificador, fluxo in fluxos.items():
			for caracteristica in caracteristicas:
				mensagempPre.append(str(fluxo[caracteristica]) + ',')
			mensagempPre.append(str(classe))
			mensagem=mensagempPre
		if mensagem: producer.send(topico, json.dumps(mensagem).encode('utf-8'))


# uso:
# python read_tcpdump.py <pcap a ser lido> <arquivo a ser salco .csv> <classe do ataque> <janela>
if __name__ == "__main__":
	arquivo = open(str(sys.argv[1]))
	pcap = dpkt.pcap.Reader(arquivo)

	nome_arquivo = str(sys.argv[2])
	classe = int(sys.argv[3])
	caracteristicas = ['qtd_pacotes_tcp', 'qtd_src_port', 'qtd_dst_port', 'qtd_fin_flag', 'qtd_syn_flag', 'qtd_psh_flag', 'qtd_ack_flag', 'qtd_urg_flag', 'qtd_pacotes_udp', 'qtd_pacotes_icmp',
					   'qtd_pacotes_ip', 'qtd_tos', 'ttl_medio', 'header_len_medio', 'packet_len_medio', 'qtd_do_not_frag', 'qtd_more_frag','fragment_offset_medio', 'qtd_rst_flag',
					   'qtd_ece_flag', 'qtd_cwr_flag', 'offset_medio', 'qtd_tipos_icmp', 'qtd_codigo_icmp']

	contador = 0
	janela = float(sys.argv[4])
	lista_fluxos = []
	for ts, data in pcap:
		eth = dpkt.ethernet.Ethernet(data)
		if contador == 0:
			tempo_inicio = ts
			fluxos = {}
		if ts > tempo_inicio + janela:
			lista_fluxos.append(fluxos)
			fluxos = {}
			tempo_inicio = ts
		fluxos = atualizar_fluxos(eth, fluxos, tempo_inicio, janela)
		contador += 1
		if len(lista_fluxos) > 100:
			salvar_fluxos(nome_arquivo, lista_fluxos, caracteristicas, classe)
			lista_fluxos = []
		# print ts, len(data)
		# print 'Timestamp: ', str(datetime.datetime.utcfromtimestamp(ts))
	if fluxos != {}:
		lista_fluxos.append(fluxos)
	arquivo.close()
	salvar_fluxos(nome_arquivo, lista_fluxos, caracteristicas, classe)

	# contador_fluxos = 0
	# for fluxos in lista_fluxos:
	#     for identificador, fluxo in fluxos.items():
	#         print fluxo
	#         print
	#         contador_fluxos += 1
	# print contador_fluxos

	# arquivo = open('teste1.pcap')
	# pcap = dpkt.pcap.Reader(arquivo)
	# contador = 0
	# pacotes = []
	# for ts, data in pcap:
	#     # print ts, len(data)
	#     eth = dpkt.ethernet.Ethernet(data)
	#     if eth.type == dpkt.ethernet.ETH_TYPE_IP:
	#         contador += 1
	#         pacotes.append(eth)
	#
	# arquivo.close()
	# eth = pacotes[1]
	# print_packet(eth)
	# print informacoes_ip(eth.data)
	# if descobrir_tipo(eth.data) == 'tcp':
	#     tcp = eth.data.data
	#     print informacoes_tcp(tcp)
	#
	# print eth.type
	# print type(eth.data.data) == dpkt.tcp.TCP

	# print '\n'
	# attributes = inspect.getmembers(eth, lambda a:not(inspect.isroutine(a)))
	# print attributes
	# print '\n'
	# ip = eth.data
	# attributes = inspect.getmembers(ip, lambda a:not(inspect.isroutine(a)))
	# print attributes
	# print '\n'
	# tcp = ip.data
	# attributes = inspect.getmembers(tcp, lambda a:not(inspect.isroutine(a)))
	# print attributes
	# print '\n'


	#INFORMACOES DO IP:
		# # IP Headers
		#
		# IP_ADDR_LEN = 0x04
		# IP_ADDR_BITS = 0x20
		#
		# IP_HDR_LEN = 0x14
		# IP_OPT_LEN = 0x02
		# IP_OPT_LEN_MAX = 0x28
		# IP_HDR_LEN_MAX = IP_HDR_LEN + IP_OPT_LEN_MAX
		#
		# IP_LEN_MAX = 0xffff
		# IP_LEN_MIN = IP_HDR_LEN
		#
		# # Reserved Addresses
		# IP_ADDR_ANY = "\x00\x00\x00\x00"    # 0.0.0.0
		# IP_ADDR_BROADCAST = "\xff\xff\xff\xff"    # 255.255.255.255
		# IP_ADDR_LOOPBACK = "\x7f\x00\x00\x01"    # 127.0.0.1
		# IP_ADDR_MCAST_ALL = "\xe0\x00\x00\x01"    # 224.0.0.1
		# IP_ADDR_MCAST_LOCAL = "\xe0\x00\x00\xff"    # 224.0.0.255
		#
		# # Type of service (ip_tos), RFC 1349 ("obsoleted by RFC 2474")
		# IP_TOS_DEFAULT = 0x00  # default
		# IP_TOS_LOWDELAY = 0x10  # low delay
		# IP_TOS_THROUGHPUT = 0x08  # high throughput
		# IP_TOS_RELIABILITY = 0x04  # high reliability
		# IP_TOS_LOWCOST = 0x02  # low monetary cost - 1XXX
		# IP_TOS_ECT = 0x02  # ECN-capable transport
		# IP_TOS_CE = 0x01  # congestion experienced
		#
		# # IP precedence (high 3 bits of ip_tos), hopefully unused
		# IP_TOS_PREC_ROUTINE = 0x00
		# IP_TOS_PREC_PRIORITY = 0x20
		# IP_TOS_PREC_IMMEDIATE = 0x40
		# IP_TOS_PREC_FLASH = 0x60
		# IP_TOS_PREC_FLASHOVERRIDE = 0x80
		# IP_TOS_PREC_CRITIC_ECP = 0xa0
		# IP_TOS_PREC_INTERNETCONTROL = 0xc0
		# IP_TOS_PREC_NETCONTROL = 0xe0
		#
		# # Fragmentation flags (ip_off)
		# IP_RF = 0x8000  # reserved
		# IP_DF = 0x4000  # don't fragment
		# IP_MF = 0x2000  # more fragments (not last frag)
		# IP_OFFMASK = 0x1fff  # mask for fragment offset
		#
		# # Time-to-live (ip_ttl), seconds
		# IP_TTL_DEFAULT = 64  # default ttl, RFC 1122, RFC 1340
		# IP_TTL_MAX = 255  # maximum ttl


