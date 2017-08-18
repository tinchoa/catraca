
from scapy.all import *

def memwrpcap(filename, pkt, *args, **kargs):
    """Write a list of packets to a pcap file
    gz: set to 1 to save a gzipped capture
    linktype: force linktype value
    endianness: "<" or ">", force endianness"""

    # use MemoryPcapWriter instead of PcapWriter
    with MemoryPcapWriter(filename, *args, **kargs) as fdesc:
        fdesc.write(pkt)


class MemoryPcapWriter(PcapWriter):
    """A stream PCAP writer with more control than wrpcap()"""
    def __init__(self, filename, linktype=None, gz=False, endianness="", append=False, sync=False):
        """
        linktype: force linktype to a given value. If None, linktype is taken
                  from the first writter packet
        gz: compress the capture on the fly
        endianness: force an endianness (little:"<", big:">"). Default is native
        append: append packets to the capture file instead of truncating it
        sync: do not bufferize writes to the capture file
        """

        self.linktype = linktype
        self.header_present = 0
        self.append=append
        self.gz = gz
        self.endian = endianness
        self.filename=filename
        self.sync=sync
        bufsz=4096
        if sync:
            bufsz=0

        # use filename or file-like object
        if isinstance(self.filename, str):
            self.f = [open,gzip.open][gz](filename,append and "ab" or "wb", gz and 9 or bufsz)
        else: # file-like object
            self.f = filename

    def __exit__(self, exc_type, exc_value, tracback):
        self.flush()
        if isinstance(self.filename, str):
            self.close() # don't close file-like object

