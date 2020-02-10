import time, os, django, pathlib, pandas as pd
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from noma.models import NomaSet, NomaFunc, NomaStrMap, NomaStrMapSet, NomaSetAct as sa 
from noma.utils import nomaMain, nomaCreateTbl, nomaExecT
from django.db import connection, connections
import noma.tfunc

execnoma = 0
createtbl = 0
savenoma = 1

nf01 = NomaFunc.objects.get(epr='dumm')

#[Kube Nodes]
sdir = pathlib.Path('C:/XNA/data/nomasftp/uploads/kube')
ldir = pathlib.Path('C:/XNA/data/nomasftp/downloads/kube/logging')
sf = sdir / 'nef_get_nodes.json'
tf = ldir / 'kube_get.tsv'

gtag = 'kube_nodes_v00'
ttype = 'js'
tname='kube_nodes'
sepr=r'items'
eepr=None
depr=None
xtag=None
cs = NomaSet(name=tname, type=ttype, sepr=sepr, eepr=eepr, depr=depr,xtag=xtag)
if savenoma == 1: cs.save()
acts = []
acts.append(sa(set=cs,seq=1,spos=0,epos=0,sepr='metadata.name',fname='nname',fchar=r'VARCHAR(80)'))
acts.append(sa(set=cs,seq=2,spos=0,epos=0,sepr='metadata.creationTimestamp',fname='ctime',fchar=r'VARCHAR(30)'))
acts.append(sa(set=cs,seq=3,spos=0,epos=0,sepr='status.nodeInfo.containerRuntimeVersion',fname='dver',fchar=r'VARCHAR(50)'))
acts.append(sa(set=cs,seq=4,spos=0,epos=0,sepr='status.nodeInfo.kernelVersion',fname='kver',fchar=r'VARCHAR(80)'))
acts.append(sa(set=cs,seq=5,spos=0,epos=0,sepr='status.nodeInfo.osImage',fname='osimg',fchar=r'VARCHAR(80)'))
acts.append(sa(set=cs,seq=6,spos=0,epos=0,sepr='status.nodeInfo.kubeletVersion',fname='kuver',fchar=r'VARCHAR(50)'))
acts.append(sa(set=cs,seq=7,spos=0,epos=0,sepr='status.addresses',varr=0))
acts.append(sa(set=cs,seq=8,spos=0,epos=0,sepr='status.conditions',varr=1))
acts.append(sa(set=cs,seq=9,spos=0,epos=0,sepr='type.InternalIP',eepr=r'address',xtag=r'_r_varr',varr=0,fname='i_ip',fchar=r'VARCHAR(60)'))
acts.append(sa(set=cs,seq=10,spos=0,epos=0,sepr='type.ExternalIP',eepr=r'address',xtag=r'_r_varr',varr=0,fname='e_ip',fchar=r'VARCHAR(60)'))
acts.append(sa(set=cs,seq=11,spos=0,epos=0,sepr='type.Ready',eepr=r'status',xtag=r'_r_varr',varr=1,fname='sta',fchar=r'VARCHAR(10)'))





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