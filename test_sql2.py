import time, os, django, re, mmap, pathlib, pandas as pd, xml.dom.minidom
import xml.etree.ElementTree as ET, copy
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from django.db import connection, connections
from noma.models import NomaGrp, NomaGrpSet, NomaSet, queGrp, queSet, NomaStrMap, NomaSetAct as sa
from noma.tfunc import q_basic, q_grpby, q_compare, q_comp, q_mstr #, q_comps
from noma.utils import nomaRetrace
from datetime import datetime

XDBX = 'xnaxdr'
stbl = 'v_pcap_sip'
cd = 'gtag="pcapng_00" and apmsg="180"'
tf = 'D:/XNA/data/nomasftp/downloads/CMCC/pcapng_retrace1.pcap'

sqlq = q_basic(XDBX,stbl,['*','gtag'],cd)
df = pd.read_sql_query(sqlq, connections[XDBX])

#pf = df['pfile'][0].split(',')[1]
#print(pf)

nomaRetrace(df,tf)


'''
#6E24 FE18

h1 = 'a23e0500'
h2 = '7aec3a10'
h3 = 'a23e05007aec3a10'
#h = 'cae10400'
#h = '6e24fe18'
b1 = bytes(bytearray.fromhex(h1))
b2 = bytes(bytearray.fromhex(h2))
b3 = bytes(bytearray.fromhex(h3))
t1 = int.from_bytes(b1,byteorder='little')
t2 = int.from_bytes(b2,byteorder='little')
t3 = int.from_bytes(b3,byteorder='little')
t4 = (t1 *  4294967296) + t2
ts = t4//1000000
ns = t4%1000000
d = datetime.fromtimestamp(ts)

#print(mg)
print(b1)
print(b2)
print(t1)
print(t2)
print(t3)
print(t4)
print(ts)
print(d)
print(ns)
   
'''
'''
XDBX = 'xnaxdr'
stbl = 'v_pcap_retrace'
cd = 'gtag="test_tag_00" and sigp="sip"'
tf = 'D:/XNA/data/nomasftp/downloads/CMCC/pcap_retrace1.pcap'

sqlq = q_basic(XDBX,stbl,['*','gtag'],cd)
df = pd.read_sql_query(sqlq, connections[XDBX])
nomaRetrace(df,tf)

'''

'''

sdir = pathlib.Path('D:/XNA/data/nomasftp/uploads/pcap/mirage')
ldir = pathlib.Path('D:/XNA/data/nomasftp/downloads/pcap')
sf = sdir / 'hk1051_01.pcap'

with open(sf, 'rUb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
    #s.seek(140)
    mg = s.read(140)
print(mg)


sdir = pathlib.Path('D:/XNA/data/nomasftp/uploads/pcap/mirage')
ldir = pathlib.Path('D:/XNA/data/nomasftp/downloads/pcap')
sf = sdir / 'hk1051_01.pcap'

with open(sf, 'rUb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
    s.seek(128)
    mg = s.read(4)
    if mg == b'\x06\x00\x00\x00':
        print(mg)
        
sdir = pathlib.Path('D:/XNA/data/nomasftp/uploads/pcap/mirage')
ldir = pathlib.Path('D:/XNA/data/nomasftp/downloads/pcap')
sf = sdir / 'sample.pcapng'

#with open(sf, 'rUb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
#    s.seek(132)
#    mg = s.read(4)

mg = b'\x06\x00\x00\x00'
plen = 604
pl = plen.to_bytes(4, byteorder='little')
print(b'\x06\x00\x00\x00' + pl + b'\x00\x00\x00\x00')
pdl = 0
print(bytes(pdl)+pl)

XDBX = 'xnaxdr'
stbl = 'v_pcap_sip'
cd = 'gtag="pcapng_00" and apmsg="180"'
tf = 'D:/XNA/data/nomasftp/downloads/CMCC/pcapng_retrace1.pcap'

sqlq = q_basic(XDBX,stbl,['*','gtag'],cd)
df = pd.read_sql_query(sqlq, connections[XDBX])

#pf = df['pfile'][0].split(',')[1]
#print(pf)

nomaRetrace(df,tf)


'''