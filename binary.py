import os.path, django
import re, mmap

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from noma.models import NomaSet, NomaSetAct as sa 
import noma.tfunc
            

#[First PCAP Trial;]
name = 'pcap_test'
type = 'bi'    
sepr = 24
eepr = None
depr = None    
    
cs = NomaSet(name=name, type=type, sepr=sepr, eepr=eepr, depr=depr)
#cs.save()

acts = []
acts.append(sa(set=cs,seq=1,spos=0,epos=4,eepr='utc(bv,ed)',fname='ts_dt'))
acts.append(sa(set=cs,seq=2,spos=4,epos=8,eepr='intB(bv,ed)',fname='ts_ns'))
acts.append(sa(set=cs,seq=3,spos=8,epos=12,eepr='intB(bv,ed)',fname='plen'))

sf = 'D:/XNA/SourceData/default/cmcc_reg.pcap'
tf = 'D:/xna/SourceData/default/firt_pcap.log'
    

nomaBMain(sf,tf,cs,acts)
#for act in acts: act.save()


  
    



    
        
