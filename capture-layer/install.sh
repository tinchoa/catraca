#echo "nameserver 8.8.8.8" > /etc/resolv.conf
apt-get update
apt-get install -y python-pip python-scapy python-dpkt python-netifaces unzip libpcap0.8 python-libpcap
unzip scapy-master.zip
cd scapy-master
python setup.py install
cd -
tar -xzf netifaces-0.10.6.tar.gz
cd netifaces-0.10.6
python setup.py install
cd -
tar -xzf kafka-python-1.3.3.tar.gz
cd kafka-python-1.3.3
python setup.py install
cd -
tar -xzf dpkt-1.9.1.tar.gz
cd dpkt-1.9.1
python setup.py install
cd -
