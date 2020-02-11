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

nf01 = NomaFunc.objects.get(epr='i_var')

#[Kube Nodes]
sdir = pathlib.Path('C:/XNA/data/nomasftp/uploads/kube')
ldir = pathlib.Path('C:/XNA/data/nomasftp/downloads/kube/logging')
sf = sdir / 'kube-get-svc.json'
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
acts.append(sa(set=cs,seq=1,spos=0,epos=0,sepr='metadata.namespace',fname='nspace',fchar=r'VARCHAR(30)'))
acts.append(sa(set=cs,seq=2,spos=0,epos=0,sepr='metadata.name',fname='nname',fchar=r'VARCHAR(80)'))
acts.append(sa(set=cs,seq=3,spos=0,epos=0,sepr='spec.type',fname='svc_type',fchar=r'VARCHAR(20)'))
acts.append(sa(set=cs,seq=4,spos=0,epos=0,sepr='spec.clusterIP',fname='c_ip',fchar=r'VARCHAR(50)'))
acts.append(sa(set=cs,seq=5,spos=0,epos=0,sepr='spec.ports',nepr=r't'))
acts.append(sa(set=cs,seq=6,spos=0,epos=0,sepr='name',fname='svc_pname',fchar=r'VARCHAR(50)'))
acts.append(sa(set=cs,seq=7,spos=0,epos=0,sepr='port',fname='svc_port',fchar=r'VARCHAR(10)'))




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