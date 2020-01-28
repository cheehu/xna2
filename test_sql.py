import time, os, django, re, pandas as pd, xml.dom.minidom
import xml.etree.ElementTree as ET, copy
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from django.db import connection, connections
from noma.models import NomaGrp, NomaGrpSet, NomaSet, queGrp, queSet, NomaStrMap, NomaSetAct as sa
from noma.tfunc import q_basic, q_grpby, q_compare, q_comp, q_mstr #, q_comps

####output xml test#####

def out_xml(stbl, df, cd, xdbx, sub=None):
    try: set = NomaSet.objects.get(name=stbl)
    except NomaSet.DoesNotExist: return None, None
    if set.xtag == None: return None, None
    ttag = set.xtag.split(',')
    ttl = len(ttag)
    if ttag[0] == 's' and sub == None: return None, None
    acts = set.acts.filter(xtag__isnull=False)
    if len(acts) == 0: return None, None
    tbl = ET.Element(ttag[2])
    t, e = tbl, []
    for tag in ttag[3:-1]: t = ET.SubElement(t, tag)
    for rno,rec in df.iterrows():
        if ttl > 3: r = ET.SubElement(t, ttag[-1])  
        else: r = copy.deepcopy(tbl) if ttag[0] == 's' else t
        for act in acts:
            rtag = act.xtag.split(',')
            etyp = rtag[0]
            if etyp[0] == 'v':
                if rec[act.fname] == '' and etyp[-1] == 'v' : continue
                rtg = rec[rtag[1][1:]] if rtag[1][0] == '_' else rtag[1]
                f = r.find(rtg)
                if f == None: f = ET.SubElement(r,rtg)
                if len(rtag) > 2:
                    for k in rtag[2:]:
                        k1 = rec[k[1:]] if k[0] == '_' else k
                        c = f.find(k1)
                        f = ET.SubElement(f, k1) if c == None else c
                f.text = rec[act.fname]
            else:
                c1 = cd + ''.join(' and %s="%s"' % (k,rec[k]) for k in rtag[2:])
                sqlq = q_basic(xdbx,rtag[1],['*','gtag'],c1)
                df1 = pd.read_sql_query(sqlq, connections[xdbx])
                if not df1.empty: 
                    tb, sr = out_xml(rtag[1],df1,c1,xdbx,'')
                    if isinstance(tb, list): 
                        if etyp[0] == 'l': 
                            for te in tb: r.append(te)
                        elif etyp[0] == 'c': 
                            c = ET.SubElement(r,tb[0].tag)
                            for te in tb: c.append(te[0])
                        else: 
                            for te in tb: r.append(te[0])
                    else: 
                        f = r
                        if sr != '':
                            stag = sr.split('/')
                            f = r.find(stag[0])
                            if f == None: f = ET.SubElement(r,stag[0])
                            if len(stag) > 1:
                                for k in stag[1:]:
                                    c = f.find(k)
                                    f = ET.SubElement(f, k) if c == None else c
                        f.append(tb)
        if ttl == 3 and ttag[0] != 't': e.append(copy.deepcopy(r))
    if len(e) > 0: tbl = e    
    return tbl, ttag[1]

XDBX = 'xnaxdr'
cd = 'gtag="TS42_v00"'
doc = ET.Element('config')
doc.set('xmlns:ntas','http://nokia.com/nokia-tas') 
#doc.set('xmlns:v4ur', 'urn:ietf:params:xml:ns:yang:ietf-ipv4-unicast-routing')
#doc.set('xmlns:rt', 'urn:ietf:params:xml:ns:yang:ietf-routing')
#doc.set('xmlns:v6ur', 'urn:ietf:params:xml:ns:yang:ietf-ipv6-unicast-routing')
#doc.set('xmlns:smd-port-table', 'http://nokia.com/nokia-tas/smd-port-table')
#doc.set('xmlns:general-sip-parameters', 'http://nokia.com/nokia-tas/general-sip-parameters')
#doc.set('xmlns:eosana', 'http://nokia.com/nokia-tas/eosana')
doc.set('xmlns:op0fil', 'http://nokia.com/nokia-tas/op0fil')

stbl = 'ntas_cc_occp'
sqlq = q_basic(XDBX,stbl,['*','gtag'],cd)
df = pd.read_sql_query(sqlq, connections[XDBX])
tbl, tbr = out_xml(stbl,df,cd,XDBX)
if tbl != None: 
    if tbr == '': tr = doc
    else:
        ttag = tbr.split('/')
        tr = doc.find(ttag[0])
        if tr == None: tr = ET.SubElement(doc,ttag[0])
        if len(ttag) > 1:
            for k in ttag[1:]:
                if k == '': break
                c = tr.find(k)
                tr = ET.SubElement(tr, k) if c == None else c
    if isinstance(tbl, list): 
        if ttag[-1] == '':
            for te in tbl: tr.append(te)
        else:
            g = ET.SubElement(tr,tbl[0].tag)
            for te in tbl: g.append(te[0])
    else: tr.append(tbl)
    
'''  
stbl = 'ntas_sip_hist'
sqlq = q_basic(stbl,['*','gtag'],cd)
df = pd.read_sql_query(sqlq, connections['xnaxdr'])
tbl, tbr = out_xml(stbl,df,cd)

if tbl != None: 
    if tbr == '': tr = doc
    else:
        ttag = tbr.split('/')
        tr = doc.find(ttag[0])
        if tr == None: tr = ET.SubElement(doc,ttag[0])
        if len(ttag) > 1:
            for k in ttag[1:]:
                if k == '': break
                c = tr.find(k)
                tr = ET.SubElement(tr, k) if c == None else c
    if isinstance(tbl, list): 
        if ttag[-1] == '':
            for te in tbl: tr.append(te)
        else:
            g = ET.SubElement(tr,tbl[0].tag)
            for te in tbl: g.append(te[0])
    else: tr.append(tbl)
'''
sdoc = doc
proot = ''
if proot != '' and doc.find(proot): 
    sdoc = doc.find(proot)
    for name in doc.attrib: sdoc.set(name, doc.attrib[name])
#print(ET.tostring(doc))
xmlstr = xml.dom.minidom.parseString(ET.tostring(sdoc)).toprettyxml('  ')
tf = 'C:/XNA/data/nomasftp/downloads/ntas/ntas_test.xlsx.xml'
with open(tf, 'w') as xf: 
    xf.write(xmlstr)

########test xml_out############
 

   
#dfd = {}
#dfd = get_xdfd(stbl,cd,dfd)
#df = dfd['ntas_intf_ipaddr']
#cd = 'df.intf=="admintd-0-chrg-interface"'
#df = df[eval(cd)]
#print(df)

#sqlq = q_basic(stbl,['*','gtag'],cd)
#df = pd.read_sql_query(sqlq, connections['xnaxdr'])

#tf = 'D:/XNA/logs/tas/xml_out.xml'
#xt = ET.ElementTree(doc)
#with open(tf, 'w+b') as xf: 
#    xt.write(xf)   
  


 


    
'''

########test q_comps_delta###########

def q_comps(stbl,ck,cd,rtag,ctags):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    kcols = ',"-",'.join(k for k in ck)
    ckey = '_'.join(k for k in ck)
    kcol1 = ',"-",'.join('t1.%s' % k for k in ck)
    onk = ' and '.join('t1.%s=t2.%s' % (k, k) for k in ck)
    if cd != '': cd =  'and %s' % ' and '.join(c for c in cd.split(','))
    ck.append('gtag')
    ncols = ','.join(cn[0] for cn in cursor if cn[0] != 'gtag')
    nc1 = ncols.split(',')[0]
    ccols = ','.join('if(t1.%s=t2.%s,":=",concat("<>",ifnull(t2.%s,"null")))' % (cn[0],cn[0],cn[0]) for cn in cursor if cn[0] != 'gtag')
    ccols1 = ','.join('concat("null<>",t1.%s)' % cn[0] for cn in cursor if cn[0] != 'gtag')
    sql1 = 'SELECT concat("*",%s) gtag, concat(%s) _%s, %s FROM %s WHERE gtag=%s %s' % (rtag, kcols, ckey, ncols, stbl, rtag, cd)
    sql2 = ''
    for ctag in ctags.split(','):
        sq1 = 'SELECT %s, concat(%s), %s\n FROM (SELECT * FROM %s WHERE gtag=%s %s) t1\n' % (ctag, kcol1, ccols, stbl, rtag, cd)
        sq2 = 'LEFT JOIN (SELECT * FROM %s WHERE gtag=%s %s) t2 On %s\nUNION\n' % (stbl, ctag, cd, onk)    
        sq3 = 'SELECT t1.gtag, concat(%s), %s\n FROM (SELECT * FROM %s WHERE gtag=%s %s) t1\n' % (kcol1, ccols1, stbl, ctag, cd)
        sq4 = 'LEFT JOIN (SELECT * FROM %s WHERE gtag=%s %s) t2 On %s where isnull(t2.%s)' % (stbl, rtag, cd, onk,ck[0])
        sql2 += '\nunion\n%s%s%s%s' % (sq1, sq2, sq3, sq4)
    sqlq = 'SELECT *, if(%s="<>null","<>null",if(left(%s,6)="null<>","null<>",if(locate("<>",concat(%s))>0,"<>",if(left(gtag,1)="*","REF","")))) _comp_\n\
FROM(%s%s) v0 \nORDER BY _%s, gtag' % (nc1, nc1, ncols, sql1, sql2, ckey)
    return sqlq

#ck = ["rname","acname"]
#stbl = 'mss_sig_map_res'    
ck = ["fqdn"]
stbl = 'mss_sip_fqdn'
cd = ''
rtag = '"FF01_v00"'
ctags = '"RM13_v00","BA01_v00"'  
print(q_comps(stbl,ck,cd,rtag,ctags))
#sqlq = q_comps(stbl,ck,cd,rtag,ctags)
#df = pd.read_sql_query(sqlq, connections['xnaxdr'])
#print(df.columns.values)
#if '_comp_' in df.columns.values:
#if df['_comp_'].str.contains('<>'):
#    print('T')
    
########test q_comps_delta###########







####output xml test#####

sf = 'D:/XNA/logs/tas/ntas_01.xlsx'
gtag = 'TS9_v01'   

sd = pd.read_excel(sf, sheet_name=None)
sd.pop('Index',None)
grp = NomaGrp.objects.get(name='noma_excel')
esets = [NomaSet(name=k, type='xl', sepr=[k,0,0,None], eepr=[None,None], depr=None,xtag=None) for k in sd]
gsets = [NomaGrpSet(grp=grp, seq=i, set=esets[i], sfile=k, ttbl=k) for i, k in enumerate(sd)]
for gset in gsets: 
    acts = [(sa(set=gset.set,seq=cn,sepr=cv,fname=cv)) for cn, cv in enumerate(sd[gset.sfile].columns.values)]
    set = esets[gset.seq]
    print('%s,%s,%s' % (gset.seq, set.name, gset.sfile))
    for act in acts:
        print('%s,%s,%s' % (act.seq, act.sepr, act.fname))
        

grp = NomaGrp.objects.get(name='noma_excel')
#grpset in grp.sets.all():
grpsets = [(NomaGrpSet(grp=grp,seq=sh.index(), set=sn, sfile=sn,tfile=sn)) for sn, sh in sd.items()]
for grpset in grpsets: 
    #cs = NomaSet.objects.get(name=grpset.set)
    cs = NomaSet(name=grpset.set, type='xl', sepr=[grpset.set,0,0,None], eepr=[None,None], depr=None,xtag=None)
    #acts = set.acts.all()
    acts = [(sa(set=cs,seq=cn,sepr=cv,fname=cv)) for cn, cv in enumerate(sh.columns.values)]

sf = 'D:/XNA/logs/tas/ntas_01.xlsx'
gtag = 'TS9_v01'   
tname = 'ntas_interfaces'
ttype = 'xl'    
sepr = '["%s", 0, None, None]' % tname
eepr = '[None, None]'
depr = None
xtag = None

sd = pd.read_excel(sf, sheet_name=None)
grp = NomaGrp.objects.get(pk=int(id))
#grpset in grp.sets.all():
grpsets = [(NomaGrpSet(grp=grp,seq=sh.index(), set=sn, sfile=sn,tfile=sn)) for sn, sh in sd.items()]
for grpset in grpsets: 
    #cs = NomaSet.objects.get(name=grpset.set)
    sepr = '["%s", 0, None, None]' % grpset.set
    cs = NomaSet(name=grpset.set, type='xl', sepr=sepr, eepr='[None, None]', depr=None, xtag=None)
    #acts = set.acts.all()
    acts = [(sa(set=cs,seq=cn,sepr=cv,fname=cv)) for cn, cv in enumerate(sh.columns.values)]



print(cs.eepr)

def p_sets(sf):
    s_dict = pd.read_excel(sf, sheetname=None)
    css = []
    for n, s in s_dict.items():
        css.append(NomaSet(name=n, type='xl', sepr=[n, 0, None, None], eepr=[None,None], depr=None,xtag=None))
        acts = []
        for cn, cv in enumerate(sheet.columns.values)
            acts.append(sa(set=css[,seq=cn,sepr=cv,fname=cv))

set = NomaSet.objects.get(name='ntas_intf_ipaddr')
acts = set.acts.filter(xtag__isnull=False)
print(acts[0].sepr)


def getQRec(sf, gtag):
    cond = '' if gtag == None else ' WHERE gtag="%s"' % gtag 
    sqlq = "SELECT * FROM %s%s" % (sf, cond)         
    try:
        df = pd.read_sql_query(sqlq, connections['xnaxdr'])
    except Exception as e:
        info = '"%s"' % e
        #info = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        df = 'Error!!!: %s' % info 
    return df
    
sf = 'mss_cha_sa'
gtag = 'RM13_v00'
print(getQRec(sf, gtag))

########test q_comps###########

def q_comps(stbl,ck,cd,rtag,ctags):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    kcols = ',"-",'.join(k for k in ck)
    ckey = '_'.join(k for k in ck)
    kcol1 = ',"-",'.join('t1.%s' % k for k in ck)
    onk = ' and '.join('t1.%s=t2.%s' % (k, k) for k in ck)
    if cd != '': cd =  'and %s' % ' and '.join(c for c in cd.split(','))
    ck.append('gtag')
    ncols = ','.join(cn[0] for cn in cursor if cn[0] not in ck)
    ccols = ','.join('if(t1.%s=t2.%s,":=",concat("<>",t1.%s)) %s' % (cn[0],cn[0],cn[0],cn[0]) for cn in cursor if cn[0] not in ck)
    if ccols != '': ccols = ',' + ccols
    sql1 = 'SELECT concat("*",gtag) gtag, concat(%s) %s, %s FROM %s WHERE gtag=%s %s\nUNION\n' % (kcols, ckey, ncols, stbl, rtag, cd)
    sql2 = 'SELECT t1.gtag,concat(%s) %s %s\n' % (kcol1,ckey,ccols)
    sql3 = 'FROM (SELECT * FROM %s WHERE gtag IN (%s) %s) t1\n' % (stbl, ctags, cd)
    sql4 = 'LEFT JOIN (SELECT * FROM %s WHERE gtag=%s %s) t2\n' % (stbl, rtag, cd)    
    sqlq = '%s%s%s%sOn %s ORDER BY %s, gtag' % (sql1, sql2, sql3, sql4, onk, ckey)
    
    return sqlq
    
ck = ["rname","acname"]
cd = ''
stbl = 'mss_sig_map_res'
rtag = '"FF01_v00"'
ctags = '"BA01_v00","RM13_v00","RM15_v00"'  
print(q_comps(stbl,ck,cd,rtag,ctags))

########test q_comps###########

#print(q_mstr(mc, flds))
#print(q_mmls(stbl,mc,md,mm,tag1,tag2))
#print(q_compare(stbl,tag1,tag2))
#print(q_comp(stbl,tag1,tag2))

#mm = "ZNRB:",ckey,":",concat(IF(para="","",concat("PARA=",para)),IF(lsh="","",concat("LOAD=",lsh)),IF(aopc="","",concat("AOPC=",aopc)))
#flds = 'ckey_0,para_1,load_1,rou_2,prio_2'


with connections['xnaxdr'].cursor() as cursor:
    cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    
    kcols = ',"-",'.join(k for k in ck)
    ck.append('gtag')
    ncols = ','.join(cn[0] for cn in cursor if cn[0] not in ck)
    ccols = ','.join('if(t1.%s=t2.%s,"=",t1.%s) %s' % (cn[0],cn[0],cn[0],cn[0]) for cn in cursor if cn[0] not in ck)
    
    
    sql1 = 'SELECT concat("*",gtag) gtag, concat(%s) ckey, %s FROM %s WHERE gtag=%s \nUNION\n' % (kcols, ncols, stbl, rtag)
    sql2 = 'SELECT t1.gtag,t1.ckey, %s\n' % ccols
    sql3 = 'FROM (SELECT concat(%s) ckey, gtag, %s FROM %s WHERE gtag IN (%s)) t1\n' % (kcols, ncols, stbl, ctags)
    sql4 = 'LEFT JOIN (SELECT concat(%s) ckey, gtag, %s FROM %s WHERE gtag=%s) t2\n' % (kcols, ncols, stbl, rtag)    
    sqlq = '%s%s%s%sOn t1.ckey=t2.ckey ORDER BY ckey, gtag' % (sql1, sql2, sql3, sql4)
    
    print(sqlq)

'''



'''
select  concat("*",gtag) gtag, concat(ntyp,"-",pval,"-",numb) ckey, ptyp, np from mss_vlr_nnum where gtag="FF01_v00"
union
select t1.gtag, t1.ckey, if(t1.ptyp=t2.ptyp,"=",t1.ptyp) ptyp, if(t1.np=t2.np,"=",t1.np) np 
from (select concat(ntyp,"-",pval,"-",numb) ckey, gtag, ptyp, np from mss_vlr_nnum where gtag in ("BA01_v00","RM13_v00","RM15_v00")) t1
left join (select concat(ntyp,"-",pval,"-",numb) ckey, gtag, ptyp, np from mss_vlr_nnum where gtag="FF01_v00") t2 
on t1.ckey = t2.ckey 
order by ckey, gtag
'''

'''
q_comp_all(stbl):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    kcols = '-'.join(cn[0] for cn in cursor if cn[3] == 'PRI')
    
stbl = 'mss_sig_serv'  
with connections['xnaxdr'].cursor() as cursor:
    cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    for cn in cursor:
        print('%s %s %s %s' % (cn[0],cn[1],cn[2],cn[3]))

flds = 'ckey_0,para_1,load_1,rou_2,prio_2' 
mc = '"ZNRB:",0,",:","1*",";"'

def dbtbs(ptt):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute('SHOW TABLES')
    tbls = [tb[0] for tb in cursor if re.search(ptt,tb[0])]
    return tuple(tbls)

tbs = dbtbs('ana')
for t in tbs:
    print(t)   


def get_qtbs(ptt=''):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute('SHOW TABLES')
    if ptt == '': tbls = [tb[0] for tb in cursor]
    else: tbls = [tb[0] for tb in cursor if re.search(ptt,tb[0])]
    return tbls


if 'mss_ana_occ' in get_qtbs():    
    print('Y')
    

    
gby = 'ip_s'
fn = 'count(ip_s)'
cd = ''
stbl = 'pcap_base'
print(q_grpby(stbl,gby,fn,cd))


#print(q_mstr(mc, flds))
#print(q_mmls(stbl,mc,md,mm,tag1,tag2))
#print(q_compare(stbl,tag1,tag2))
#print(q_comp(stbl,tag1,tag2))
    
    
    
'''
