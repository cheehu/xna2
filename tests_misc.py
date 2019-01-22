import time, os, django, re, pandas as pd
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from django.db import connection, connections
from noma.tfunc import q_mmls, q_compare, q_comp, q_mstr
   
flds = 'ckey_0,para_1,load_1,rou_2,prio_2' 
mc = '"ZNRC:" 0 "," 1 ",:" 2 ";"'
md = '"ZxxD"'
mm = '"ZNRB:" 0 ":" 1* ";"'

stbl = 'v_comp_mss_sig_routes'
tag1 = "FF01_v1"
tag2 = "FF01_v2"
#mc = "'ZQKC'"
#md = "'ZQKA'"
#mm = "''"

#print(q_mstr(mc, flds))
print(q_mmls(stbl,mc,md,mm,tag1,tag2))
#print(q_compare(stbl,tag1,tag2))
#print(q_comp(stbl,tag1,tag2))





    

