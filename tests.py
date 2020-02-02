import time, os, django, pathlib, pandas as pd
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from noma.models import NomaSet, NomaFunc, NomaStrMap, NomaStrMapSet, NomaSetAct as sa 
from noma.utils import nomaMain, nomaCreateTbl, nomaExecT
from django.db import connection, connections
import noma.tfunc

execnoma = 1
createtbl = 0
savenoma = 0

nf01 = NomaFunc.objects.get(epr='dumm')

#[Cisco Interface]
sdir = pathlib.Path('C:/XNA/data/nomasftp/uploads/demo')
ldir = pathlib.Path('C:/XNA/data/nomasftp/downloads/demo/logging')
sf = sdir / 'Cisco CS-01.log'
tf = ldir / 'demo_test.tsv'

gtag = 'demo_cis'
ttype = 'tx'
tname='cisco_intf'
sepr=r'!\r\ninterface\s'
eepr=r'!\r\n(?!interface)'
depr=r'!\ninterface\s'
xtag=None
cs = NomaSet(name=tname, type=ttype, sepr=sepr, eepr=eepr, depr=depr,xtag=xtag)
if savenoma == 1: cs.save()
acts = []
acts.append(sa(set=cs,seq=1,spos=0,eepr=r'\n',fname='intf',fchar=r'VARCHAR(50)'))
acts.append(sa(set=cs,seq=2,spos=0,sepr=r'\n\s(?=switchport\n)',eepr=r'\n',fname='ptyp',fchar=r'VARCHAR(20)'))
acts.append(sa(set=cs,seq=3,spos=0,sepr=r'\n\sswi.+mode\s',eepr=r'\n',fname='mode',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=4,spos=0,sepr=r'\n\sswi.+(?=nonego)',eepr=r'\n',fname='dtp',fchar=r'VARCHAR(20)'))
acts.append(sa(set=cs,seq=5,spos=0,sepr=r'\n\s.+a(ll|cc).+vlan\s',eepr=r'\n',fname='vlan',fchar=r'VARCHAR(200)'))
acts.append(sa(set=cs,seq=6,spos=0,sepr=r'\n\s.+encap.+?\s',eepr=r'\n',fname='encap',fchar=r'VARCHAR(30)'))
acts.append(sa(set=cs,seq=7,spos=0,sepr=r'\n\s(.*?)ip address',eepr=r'\n',fname='ipaddr',fchar=r'VARCHAR(50)'))
acts.append(sa(set=cs,seq=8,spos=0,sepr=r'\n\s(?=shutdown)',eepr=r'\n',fname='sta',fchar=r'VARCHAR(20)'))
acts.append(sa(set=cs,seq=9,spos=0,sepr=r'\n\speed',eepr=r'\n',fname='speed',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=10,spos=0,sepr=r'\n\duplex',eepr=r'\n',fname='dup',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=11,spos=0,sepr=r'\n\smls qos',eepr=r'\n',fname='mlsqos',fchar=r'VARCHAR(30)'))
acts.append(sa(set=cs,seq=13,spos=0,sepr=r'\n\sdesc.+?\s',eepr=r'\n',fname='descr',fchar=r'VARCHAR(100)'))


#tfunc=nf04,nepr=r'val,"ON","true","false"',
#tfunc=nf02,nepr=r'val,smap["OTN_TON"],15',

if execnoma == 1: 
    smap = nomaExecT(NomaStrMap.objects.all().values())
    noma.tfunc.G_SDICT.clear()
    noma.tfunc.G_SDICT.update({'ldir':ldir})
    xlobj = pd.ExcelFile(sf) if ttype == 'xl' else None 
    st = time.time()
    print(nomaMain(sf,tf,cs,acts,smap,gtag,xlobj,'xnaxdr'))
    print(time.time() - st)

if savenoma == 1:
    for act in acts: act.save()

if createtbl == 1: nomaCreateTbl(tname, acts, 'xnaxdr')
    
#acts.append(sa(set=cs,seq=0,spos=0,epos=2,tfunc=nf01,nepr=r'"dk"',fname='dkey',fchar=r'VARCHAR(5)'))
#acts.append(sa(set=cs,seq=1,spos=0,sepr=r'<dns-proxy:forwarder-ip-addresses>',eepr=r'</dns-proxy:forwarder-ip-addresses>',nepr=r'\n'))
#acts.append(sa(set=cs,seq=4,xtag=r'l,ntas_sig_cha_streams,sid'))
#rate-4>(.*?\n)+?\s+?<.+?