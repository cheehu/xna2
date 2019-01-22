import os, django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from .models import nomaSet, nomaSetAct
from .utils import nomaMain


class ac:
    
    def __init__(self, st, sq, ne, sp, ep, se, ee, fe, fn, sk, tf):
        self.st = st
        self.sq = sq
        self.ne = ne
        self.sp = sp
        self.ep = ep
        self.se = se
        self.ee = ee
        self.fe = fe
        self.fn = fn
        self.sk = sk
        self.tf = tf

name = 'mss_ipconf'
sepr = '\n.{33} -+\r\n'
eepr = '\r\nCOMMAND EXECUTED'
depr = '\n{2}'

set = NomaSet(name=name, sepr=sepr, eepr=eepr, depr=depr)

a = []
a.append(ac(set.id, 1, None, 0, 10, None, None, None, 'unit', None, None))
a.append(ac(set.id, 2, '\n\s{2}(?=\S)', 20, None, None, None, None, None, None, None))

i = 0
a1 = NomaSetAct(set=a[i].st, 
                seq=a[i].sq, 
                nerp=a[i].ne, 
                spos=a[i].sp, 
                epos=a[i].ep, 
                sepr=a[i].se, 
                eepr=a[i].ee, 
                fepr=a[i].fe, 
                fname=a[i].fn, 
                skipseq=a[i].sk, 
                tfunc=a[i].tf)

i = 1
a2 = NomaSetAct(set=a[i].st, 
                seq=a[i].sq, 
                nerp=a[i].ne, 
                spos=a[i].sp, 
                epos=a[i].ep, 
                sepr=a[i].se, 
                eepr=a[i].ee, 
                fepr=a[i].fe, 
                fname=a[i].fn, 
                skipseq=a[i].sk, 
                tfunc=a[i].tf)

print("%s  -  %s" % (set.id, set.name))

