import time, os, django, re, pandas as pd
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "xna2.settings")
django.setup()
from django.db import connection, connections
from noma.tfunc import q_basic, q_grpby, q_compare, q_comp, q_mstr #, q_comps

def q_subana(stb1,atyp,maxd=8,flds=['*'], gtag=''):
    stb2 = 'mss_attr_val'
    stb3 = 'mss_attr_st'
    atp = 'aif' if atyp in ['MTC', 'MOC'] else atyp 
    rtbl = 'mss_ares_%s' % atp
    c1 = '"SUB" and atyp = "%s" and %s' % (atyp,gtag)
    c2 = 't1.atyp = "%s" and t1.%s' % (atyp,gtag)
    c3 = '"FIN" and atyp = "%s" and %s' % (atyp,gtag)
    v2 = 'select subname, attr, "*DEF" val, dres ana from %(stb1)s where dtyp = %(c1)s\n\
union select subname, attr, "*UNK" val, ures ana from %(stb1)s where utyp = %(c1)s\n\
union (select t1.subname, t2.attr, t1.aval val, t1.res ana from %(stb2)s t1\n\
join %(stb1)s t2 on t1.gtag = t2.gtag and t1.atyp = t2.atyp and t1.subname = t2.subname\n\
where t1.rtyp = "SUB" and %(c2)s)' % {"c1":c1, "c2":c2, "stb1":stb1,"stb2":stb2} 
    v1 = 'select concat(if(t1.epcn="","",concat(t1.epcn,"-")),t1.subname) ana1, v1.attr attr1, v1.val val1, v1.ana ana2 from %(stb3)s t1\n\
join (%(v2)s) v1 on t1.subname = v1.subname where %(c2)s' % {"c2":c2, "stb3":stb3, "v2":v2}
    v3 = 'select subname, attr, "*DEF" val, dres res from %(stb1)s where dtyp = %(c3)s\n\
union select subname, attr, "*UNK" val, ures res from %(stb1)s where utyp = %(c3)s\n\
union (select t1.subname, t2.attr, t1.aval val, t1.res res from %(stb2)s t1\n\
join %(stb1)s t2 on t1.gtag = t2.gtag and t1.atyp = t2.atyp and t1.subname = t2.subname\n\
where t1.rtyp = "FIN" and %(c2)s)' % {"c3":c3, "c2":c2, "stb1":stb1,"stb2":stb2}
    v4 = 'select * from %s where %s' % (rtbl, gtag) 
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % rtbl)
        cols = ','.join('v2.%s' % cn[0] for cn in cursor if cn[0] not in ['gtag','resn','rtyp'])
    
    sql1 = v1
    d = 2
    while d < maxd:
        sql1 = 'SELECT v1.*, v2.attr attr%(n)s, v2.val val%(n)s, v2.ana ana%(m)s FROM (%(q)s) v1\n\
join (%(v2)s) v2 on v1.ana%(n)s = v2.subname' % {"n":d, "m":d+1, "q":sql1, "v2":v2}
        df = pd.read_sql_query(sql1, connections['xnaxdr'])
        if df.empty: break
        d += 1
    
    ec = ''.join(',""' for i in range(0,(d-1)*3))
    ob = ','.join('ana%s,val%s' % (i,i) for i in range(1,d+1))
    
    s1 = 'select concat(if(t1.epcn="","",concat(t1.epcn,"-")),t1.subname) ana1,\
v1.attr attr1, v1.val val1 %(ec)s, v1.res from %(stb3)s t1\n\
join (%(v3)s) v1 on t1.subname = v1.subname where %(c2)s' % {"ec":ec, "c2":c2, "stb3":stb3, "v3":v3}
    
    s2 = 'select v1.*, v2.attr attr2, v2.val val2 %(ec)s, v2.res from (%(v1)s) v1\n\
join (%(v3)s) v2 on v1.ana2 = v2.subname' % {"ec":ec[:-9],"v1":v1, "v3":v3}    
        
    sqlq = s2 + '\nUNION ' + s1
    sql1 = v1
    n = 2
    while n < d:
        sql1 = 'SELECT v1.*, v2.attr attr%(n)s, v2.val val%(n)s, v2.ana ana%(m)s FROM (%(q)s) v1\n\
join (%(v2)s) v2 on v1.ana%(n)s = v2.subname' % {"n":n, "m":n+1, "q":sql1, "v2":v2}
        s3 = 'select v1.*, v2.attr attr%(m)s, v2.val val%(m)s %(ec)s, v2.res from (%(q)s) v1\n\
join (%(v3)s) v2 on v1.ana%(m)s = v2.subname' % {"ec":ec[:-n*9],"m":n+1, "q":sql1, "v3":v3}
        sqlq = s3 + '\nUNION ' + sqlq
        n += 1
    
    sqlq = 'select v1.*, %s from (%s) v1 join (%s) v2 on v1.res = v2.resn' % (cols,sqlq,v4) 
    sqlq += '\nORDER BY %s' % ob  
    
    return sqlq

print(q_subana('mss_attr_ana', 'EOS', 10, ['*','gtag'],'gtag="RM13_v00"'))
    
'''

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
