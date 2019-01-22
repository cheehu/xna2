import time, os, django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from noma.models import NomaSet, NomaFunc, NomaStrMap, NomaSetAct as sa 
from noma.utils import nomaMain, nomaCreateTbl, nomaExecT
from django.db import connection, connections

execnoma = 1
savenoma = 0
createtbl = 0

#[MSS IP CPU]
gtag = 'FF01_v1'  
tname = 'mss_ip_cpu'
ttype = 'tx'    
sepr = r'\n\sUNIT.+INFO\r\n'
eepr = r'\r\n\r\nTOTAL'
depr = r'\n'

cs = NomaSet(name=tname, type=ttype, sepr=sepr, eepr=eepr, depr=depr)
if savenoma == 1: cs.save()

#nf1 = NomaFunc.objects.get(epr='repstr')

acts = []
acts.append(sa(set=cs,seq=1,spos=0,epos=11,fname='unit',fchar=r'VARCHAR(15)'))
acts.append(sa(set=cs,seq=2,spos=12,epos=16,fname='phys',fchar=r'VARCHAR(5)'))
acts.append(sa(set=cs,seq=3,spos=17,epos=22,fname='sta',fchar=r'VARCHAR(10)'))
acts.append(sa(set=cs,seq=4,spos=23,epos=44,fname='loca',fchar=r'VARCHAR(30)'))
acts.append(sa(set=cs,seq=5,spos=45,epos=74,fname='info',fchar=r'VARCHAR(30)'))

sf = 'D:/xna/SourceData/DTG/MSS/MSSFFM01/ip_config.log'
tf = 'D:/xna/SourceData/DTG/MSS/test_ip_cpu.tsv'

if execnoma == 1: 
    smap, dfsf = nomaExecT(sf,NomaStrMap.objects.all().values(),cs.type)
    st = time.time()
    print(nomaMain(sf,tf,cs,acts,smap,dfsf,gtag))
    print(time.time() - st)

if savenoma == 1: 
    for act in acts: act.save()

if createtbl == 1: nomaCreateTbl(tname, acts)
    
