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

#[nTAS Licence CLS]
sdir = pathlib.Path('C:/XNA/data/nomasftp/uploads/VFG/nTAS')
ldir = pathlib.Path('C:/XNA/data/nomasftp/downloads')
sf = sdir / 'TS42_0115p.xml'
tf = ldir / 'ntas_test_01.tsv'

gtag = 'NTAS_v01'
ttype = 'tx'
tname='ntas_lic_cls'
sepr=r'<cls:cls>'
eepr=r'</*>'
depr=r'<cls:cls>'
xtag=r'c,ntas:license-management,feature-info:features/,feature-info:feature'
cs = NomaSet(name=tname, type=ttype, sepr=sepr, eepr=eepr, depr=depr,xtag=xtag)
if savenoma == 1: cs.save()
acts = []
acts.append(sa(set=cs,seq=1,spos=0,sepr=r':feature-code>',eepr=r'</',fname='fea',xtag=r'v,feature-info:feature-code',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=2,spos=0,sepr=r':administrative-state>',eepr=r'</',fname='sta',xtag=r'v,feature-info:administrative-state',fchar=r'VARCHAR(10)'))


gtag = 'TS42_v00'
ttype = 'tx'
tname = 'otas_cdr_file'
sepr = r'ZGAL::ST=A;'
eepr = r'\nCOMM'
depr = r'##'
xtag = None

cs = NomaSet(name=tname, type=ttype, sepr=sepr, eepr=eepr, depr=depr,xtag=xtag)
if savenoma == 1: cs.save()
acts = []
acts.append(sa(set=cs,seq=1,spos=0,sepr=r'FILENAME:\s+',eepr=r'\n',fname='filen',fchar=r'VARCHAR(50)'))
acts.append(sa(set=cs,seq=2,spos=0,sepr=r'SERVICE:\s+',eepr=r'\n',fname='serv',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=3,spos=0,sepr=r'STATE:\s+',eepr=r'\n',fname='sta',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=4,spos=0,sepr=r'USED FOR:\s+',eepr=r'\n',fname='ufor',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=5,spos=0,sepr=r'BLOCK VERSION:\s+',eepr=r'\n',fname='bver',fchar=r'VARCHAR(30)'))
acts.append(sa(set=cs,seq=6,spos=0,sepr=r'VERSION NUMBER:\s+',eepr=r'\n',fname='vern',fchar=r'VARCHAR(10)'))



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