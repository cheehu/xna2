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

nf01 = NomaFunc.objects.get(epr='json_dict')
nf02 = NomaFunc.objects.get(epr='json_list')
nf03 = NomaFunc.objects.get(epr='i_var')
nf04 = NomaFunc.objects.get(epr='dumm')
nf05 = NomaFunc.objects.get(epr='rep_if')
nf06 = NomaFunc.objects.get(epr='json_vald')
nf07 = NomaFunc.objects.get(epr='merge_dl')
nf08 = NomaFunc.objects.get(epr='get_nested')
nf09 = NomaFunc.objects.get(epr='merge_recs')
nf10 = NomaFunc.objects.get(epr='merge_dl2')

#[nTAS AoM
sdir = pathlib.Path('C:/XNA/data/nomasftp/uploads/VFG/nTAS/D200313')
ldir = pathlib.Path('C:/XNA/data/nomasftp/downloads')
sf = sdir / 'TS42_0313e.xml'
tf = ldir / 'ntas_test_01.tsv'

gtag = 'NTAS_v01'
ttype = 'tx'
tname='ntas_oam_aom'
sepr=r'<aom:measurement-jobs>\s+<aom:measurement-job>'
eepr=r'</aom:measurement-jobs>'
depr=r'<aom:measurement-job>'
xtag=r'c,ntas:administration-of-measurements,aom:measurement-jobs/,aom:measurement-job'
cs = NomaSet(name=tname, type=ttype, sepr=sepr, eepr=eepr, depr=depr,xtag=xtag)
if savenoma == 1: cs.save()
acts = []
acts.append(sa(set=cs,seq=1,spos=0,sepr=r':measurement-id>',eepr=r'</',fname='mid',xtag=r'v,oam:measurement-id',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=2,spos=0,sepr=r':start-date>',eepr=r'</',fname='sdate',xtag=r'v,oam:start-date',fchar=r'VARCHAR(15)'))
acts.append(sa(set=cs,seq=3,spos=0,sepr=r':stop-date>',eepr=r'</',fname='edate',xtag=r'v,oam:stop-date',fchar=r'VARCHAR(15)'))
acts.append(sa(set=cs,seq=4,spos=0,sepr=r':granularity-period>',eepr=r'</',fname='intv',xtag=r'v,oam:granularity-period',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=5,spos=0,sepr=r':number-of-intervals>',eepr=r'</',fname='nint',xtag=r'v,oam:number-of-intervals',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=6,spos=0,sepr=r':starting-time>',eepr=r'</',fname='stime',xtag=r'v,oam:starting-time',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=7,xtag=r's,ntas_oam_aom_d,mid'))
       
#acts.append(sa(set=cs,seq=2,tfunc=nf03,nepr=r'records[val]',fname='netron',fchar=r'VARCHAR(50)'))
#acts.append(sa(set=cs,seq=6,spos=0,eepr=r':',name='netpar',fchar=r'VARCHAR(50)'))
#acts.append(sa(set=cs,seq=5,tfunc=nf03,nepr=r'records[val][0]["connectionPoints"]',fname='icp',fchar=r'VARCHAR(50)'))
#acts.append(sa(set=cs,seq=5,sepr=r'externalConnectionPoints',fname='ecp',fchar=r'VARCHAR(50)'))
#acts.append(sa(set=cs,seq=3,sepr='metadata.labels.app',varr=0))
#acts.append(sa(set=cs,seq=4,sepr='metadata.labels.name',tfunc=nf05,nepr=r'val,"^$",varr[0][0]',varr=1))
#acts.append(sa(set=cs,seq=4,sepr='metadata.labels.infra',tfunc=nf05,nepr=r'varr[1][0],"^$",val',fname='setn',fchar=r'VARCHAR(80)'))
#acts.append(sa(set=cs,seq=5,spos=0,epos=0,sepr=r'requirements',tfunc=nf01,nepr=r'val,"sw_image"',fname='sw_im',fchar=r'VARCHAR(30)'))






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