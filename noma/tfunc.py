from datetime import datetime
import re, pandas as pd
from django.db import connection, connections

def to_txt(bv):
    return bv.decode()

def to_ipv4(bv):
    return ".".join(str(b) for b in bv)
        
def to_intb(bv, s, l):
    e = int.from_bytes(bv,byteorder='big')
    d = 2 ** s
    c = d * ((2 ** l) - 1)
    return str(int((e & c) / d))

def to_intbn(bv, s, l):
    e = int.from_bytes(bv,byteorder='big')
    d = 2 ** s
    c = d * ((2 ** l) - 1)
    return int((e & c) / d)
        
def to_hex(bv, d, n):
    if d == None:
        return bv.hex()
    else:
        a = bv.hex()
    return d.join(a[j:j+n] for j in range(0, len(a), n))

def to_intBE(bv,ed):
    return str(int.from_bytes(bv,byteorder=ed))

def to_intB(bv):
    return str(int.from_bytes(bv,byteorder='big'))

def to_intBn(bv):
    return int.from_bytes(bv,byteorder='big')
        
def to_utc(bv,ed):
    e = int.from_bytes(bv,byteorder=ed)
    return str(datetime.utcfromtimestamp(e))

def to_sigp(pa,sm):
    sig = 'unk'
    for p in pa:
        if p in sm: 
            sig = sm[p]
            break
    return sig

def to_hdl(bv,s, l, hd):
    e = int.from_bytes(bv,byteorder='big')
    d = 2 ** s
    c = d * ((2 ** l) - 1)
    hl = 4 * int((e & c) / d)
    return hl - hd

def i_var(iv):
    return str(iv)
    
def i_vara(iv):
    return ','.join(str(v) for v in iv)

def to_paylen(hdl):
    ips = int(hdl[0])
    ipl = int(hdl[1])
    aps = int(hdl[3])
    return str(ipl - (aps - ips))
    
def dupp(dupl,qno,iph,bs):
    dup = 'N'
    upid = '%s-%s-%s' % (iph[0],iph[1],iph[3])
    if iph[0] != '0' and upid in dupl: dup = 'Y'
    dupl[qno[0]] = upid
    qno[0] = 0 if qno[0] == bs else qno[0] + 1 
    return dup


def defrag(dupl,recs,rno,skb,qno,iph,hdl,bs):
    clen = int(hdl[1]) - int(hdl[2])*4
    fr0 = '%s,%s,%s' % (rno, 50+skb, clen)
    if iph[1] == '0' and iph[2] != '1': return fr0
    frl = ['%s:%s' % (iph[1],fr0)]
    rp, tlen = [None]*5, 0 
    for i in range(1,15): 
        rn = rno + i
        pid = to_intB(recs[rn][34+skb:36+skb])
        ofs = to_intbn(recs[rn][36+skb:38+skb],0,13)
        mf = to_intb(recs[rn][36+skb:37+skb],5,2)
        csum = to_intB(recs[rn][40+skb:42+skb])
        ipdl = to_intBn(recs[rn][32+skb:34+skb]) - to_intbn(recs[rn][30+skb:31+skb],0,4)*4
        upid = '%s-%s-%s' % (pid, ofs, csum)
        if upid not in dupl:
            if pid == iph[0]: 
                clen += ipdl
                if ofs != 0 and mf == '0': tlen = ofs*8 + ipdl
                frl.append('%s:%s,%s,%s' % (ofs, rn, 50+skb, ipdl))
                dupl[qno[0]] = upid
                qno[0] = 0 if qno[0] == bs else qno[0] + 1 
                if clen == tlen: break
    frl.sort()
    return '-'.join(f.split(':')[1] for f in frl)

def apdata(rec,sp):
    return rec[int(sp):]
    

def siphdr(sipmsg,sepr,eepr):
    hdr = ''
    m = re.search(sepr, sipmsg)
    if m != None:
        fs = m.end()
        m = re.search(eepr, sipmsg[fs:])
        fe = fs + 10 if m == None else fs + m.start()
        val = sipmsg[fs:fe].strip()
        hdr = val.decode()
    return hdr
    
def dumm(ep):
    return ep
    
def conc(varr,d):
    return d.join(va for va in varr)

def repstr(val,ostr,nstr):
    return re.sub(ostr,nstr,val)    
    
def nume(val):
    if not val.isdigit(): val = ''
    return val

def repstra(val,dc,od,nd,tl):
    vs = re.sub(od,',',val).split(',')
    cvs = []
    for v in vs:
        cv = dc[v] if v in dc else v[0:tl]
        cvs.append(cv)
    return nd.join(cv for cv in cvs)

def repstr1(val,dc,tl):
    cv = dc[val] if val in dc else val[0:tl]
    return cv

def trim(val,s,e):
    return val[s:e]
    

    
def q_basic(stbl, flds=['*'], cond=''):
    if flds[0] == '*': 
        if len(flds) > 1:
            with connections['xnaxdr'].cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM %s" % stbl)
            cols = ','.join(cn[0] for cn in cursor if cn[0] not in flds)
        else: cols = '*'
    else:
        cols = ','.join(cn for cn in flds)
    if cond != '': cond = ' WHERE %s' % cond 
    sqlq = "SELECT %s FROM %s%s" % (cols, stbl, cond)
    return sqlq

def q_delete(stbl, cond='gtag is null'):
    if cond != '': cond = ' WHERE %s' % cond 
    sqlq = "DELETE FROM %s%s" % (stbl, cond)
    return sqlq

def q_copy(stbl, tag1, tag2):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
        acols = ','.join(cn[0] for cn in cursor)
        bcols = "'%s',%s" % (tag2, ','.join(cn[0] for cn in cursor if cn[0] != 'gtag'))
        sqlq = "INSERT INTO %s (%s) SELECT %s FROM %s WHERE gtag='%s'" % (stbl, acols, bcols, stbl, tag1)
        cursor.execute(sqlq)
    sqlq = "SELECT * FROM %s WHERE gtag='%s'" % (stbl, tag2)
    
    return sqlq
    

def q_compare(stbl, tag1, tag2):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    ncols = ','.join(cn[0] for cn in cursor if cn[0][:2] == 'c_')
    icols = ','.join(cn[0] for cn in cursor if cn[0][:2] =='i_')
    ca, cs = [[] for y in range(10)], []
    c_ps = "concat(ckey,%s) c_ps" % ncols
    for nc in ncols.split(','): 
        ca[0].append("IF(v1.%s=v2.%s,concat(v1.%s,':','=='),concat(v1.%s,'<>',v2.%s)) %s" % (nc, nc, nc, nc, nc, nc))
        ca[1].append("concat(%s,':##') %s" % (nc, nc))
        ca[2].append("concat(%s,':==') %s" % (nc, nc))
        ca[3].append("concat('##:',%s) %s" % (nc, nc))
    if icols != '':
        for ic in icols.split(','): 
            for i in range(3): ca[i].append("v1.%s" % ic)
            ca[3].append("v2.%s" % ic)
        ncols = '%s,%s' % (ncols, icols)
    for i in range(4): cs.append(','.join(cf for cf in ca[i]))
    ivt1 = "(SELECT ckey, %s, %s FROM %s WHERE gtag='%s') v1" % (ncols, c_ps, stbl, tag1)
    ivt2 = "(SELECT ckey, %s, %s FROM %s WHERE gtag='%s') v2" % (ncols, c_ps, stbl, tag2)
    ivt3 = "(SELECT ckey, %s FROM %s WHERE gtag='%s') v1" % (ncols, stbl, tag1)
    ivt4 = "(SELECT ckey, %s FROM %s WHERE gtag='%s') v2" % (ncols, stbl, tag2)
    cvt1 = "ckey IN (SELECT ckey FROM %s WHERE gtag='%s') AND c_ps NOT IN (SELECT %s FROM %s WHERE gtag='%s'))" % (stbl, tag1, c_ps, stbl, tag1)
    cvt2 = "ckey IN (SELECT ckey FROM %s WHERE gtag='%s') AND c_ps NOT IN (SELECT %s FROM %s WHERE gtag='%s'))" % (stbl, tag2, c_ps, stbl, tag2)
    cvt3 = "ckey NOT IN (SELECT ckey FROM %s WHERE gtag='%s')" % (stbl, tag1)
    cvt4 = "ckey NOT IN (SELECT ckey FROM %s WHERE gtag='%s')" % (stbl, tag2)
    cvt5 = "c_ps IN (SELECT %s FROM %s WHERE gtag='%s')" % (c_ps, stbl, tag2)    
    sqlm = "SELECT concat(k1,':==') c_key, %s, 'MODIFY' act\n" \
           "FROM (SELECT ckey k1, %s FROM %s\n WHERE %s v1\n" \
           "LEFT OUTER JOIN (SELECT ckey k2, %s FROM %s\n WHERE %s v2\n" \
           "ON k1= k2" % (cs[0], ncols, ivt1, cvt2, ncols, ivt2, cvt1)
    sqld = "SELECT concat(ckey,':##') c_key, %s, 'DELETE' act\n FROM %s\n WHERE %s" % (cs[1], ivt3, cvt4)
    sqln = "SELECT concat(ckey,':==') c_key, %s, 'NO_ACT' act\n FROM %s\n WHERE %s" % (cs[2], ivt1, cvt5)
    sqlc = "SELECT concat('##:',ckey) c_key, %s, 'CREATE' act\n FROM %s\n WHERE %s" % (cs[3], ivt4, cvt3)
    sqlq = '%s\nUNION %s\nUNION %s\nUNION %s' % (sqld, sqlc, sqlm, sqln) 
       
    return sqlq

def q_comp(stbl, tag1, tag2):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    ncols = ','.join(cn[0] for cn in cursor if cn[0][:2] == 'c_')
    qcols = "ckey ckey_0,%s" % ','.join('%s %s' % (cn[0],cn[0][2:]) for cn in cursor if cn[0][:2] == 'c_')
    icols = ','.join(cn[0] for cn in cursor if cn[0][:2] =='i_')
    ca, cs = [[] for y in range(10)], []
    c_ps = "concat(ckey,%s) c_ps" % ncols
    for nc in ncols.split(','): 
        ca[0].append("IF(v1.%s=v2.%s,concat(v1.%s,':','=='),concat(v1.%s,'<>',v2.%s)) %s" % (nc, nc, nc, nc, nc, nc))
        ca[1].append("concat(%s,':##') %s" % (nc, nc))
        ca[2].append("concat(%s,':==') %s" % (nc, nc))
        ca[3].append("concat('##:',%s) %s" % (nc, nc))
    if icols != '':
        for ic in icols.split(','): 
            for i in range(3): ca[i].append("v1.%s" % ic)
            ca[3].append("v2.%s" % ic)
        ncols = '%s,%s' % (ncols, icols)
    for cn in cursor: 
        if cn[0][:2] == 'c_': ca[4].append("IF(v1.%s<>v2.%s,v2.%s,'') %s" % (cn[0], cn[0], cn[0], cn[0][2:]))
        rcols = "k1 ckey_0,%s" % ','.join(cn for cn in ca[4])
    
    cs.append('%s,%s' % (','.join(cf for cf in ca[0]),rcols))
    for i in range(1,4): cs.append('%s,%s' % (','.join(cf for cf in ca[i]),qcols))
    ivt1 = "(SELECT ckey, %s, %s FROM %s WHERE gtag='%s') v1" % (ncols, c_ps, stbl, tag1)
    ivt2 = "(SELECT ckey, %s, %s FROM %s WHERE gtag='%s') v2" % (ncols, c_ps, stbl, tag2)
    ivt3 = "(SELECT ckey, %s FROM %s WHERE gtag='%s') v1" % (ncols, stbl, tag1)
    ivt4 = "(SELECT ckey, %s FROM %s WHERE gtag='%s') v2" % (ncols, stbl, tag2)
    cvt1 = "ckey IN (SELECT ckey FROM %s WHERE gtag='%s') AND c_ps NOT IN (SELECT %s FROM %s WHERE gtag='%s'))" % (stbl, tag1, c_ps, stbl, tag1)
    cvt2 = "ckey IN (SELECT ckey FROM %s WHERE gtag='%s') AND c_ps NOT IN (SELECT %s FROM %s WHERE gtag='%s'))" % (stbl, tag2, c_ps, stbl, tag2)
    cvt3 = "ckey NOT IN (SELECT ckey FROM %s WHERE gtag='%s')" % (stbl, tag1)
    cvt4 = "ckey NOT IN (SELECT ckey FROM %s WHERE gtag='%s')" % (stbl, tag2)
    cvt5 = "c_ps IN (SELECT %s FROM %s WHERE gtag='%s')" % (c_ps, stbl, tag2)    
    sqlm = "SELECT concat(k1,':==') c_key, %s, 'MODIFY' act\n" \
           "FROM (SELECT ckey k1, %s FROM %s\n WHERE %s v1\n" \
           "LEFT OUTER JOIN (SELECT ckey k2, %s FROM %s\n WHERE %s v2\n" \
           "ON k1= k2" % (cs[0], ncols, ivt1, cvt2, ncols, ivt2, cvt1)
    sqld = "SELECT concat(ckey,':##') c_key, %s, 'DELETE' act\n FROM %s\n WHERE %s" % (cs[1], ivt3, cvt4)
    sqln = "SELECT concat(ckey,':==') c_key, %s, 'NO_ACT' act\n FROM %s\n WHERE %s" % (cs[2], ivt1, cvt5)
    sqlc = "SELECT concat('##:',ckey) c_key, %s, 'CREATE' act\n FROM %s\n WHERE %s" % (cs[3], ivt4, cvt3)
    sqlq = '%s\nUNION %s\nUNION %s\nUNION %s' % (sqld, sqlc, sqlm, sqln) 
       
    return sqlq

    
def q_mstr(ma, fd):
    fa = fd.split(',')
    ml = []
    va = ma.split(' ')
    for p in va:
        if p[0].isdigit():
            if p[-1] == '*':
                ml.append("mid(concat(%s),2)" % ','.join('IF(%s="","",concat(",%s=",%s))' % (f,f[:-2],f) for f in fa if f[-1] == p[0]))
            else: ml.append("mid(concat(%s),2)" % ','.join('concat(",",%s)' % f for f in fa if f[-1] == p[0]))
        else: ml.append(p)
    return ','.join(m for m in ml)   
    
def q_mmls(stbl, mc, md, mm, tag1, tag2):
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    sqlc = q_comp(stbl,tag1,tag2)
    cps = "ckey_0,%s,act" % ','.join(cn[0][2:] for cn in cursor if cn[0][:2] == 'c_')
    sql1 = "SELECT %s,\nconcat(%s) mml\nFROM (%s) v3\nWHERE act = 'MODIFY'" % (cps,q_mstr(mm,cps), sqlc)
    sql2 = "SELECT %s,\nif(act='CREATE', concat(%s), concat(%s)) mml\nFROM (%s) v3\n" \
           "WHERE act = 'CREATE' or act = 'DELETE'" % (cps, q_mstr(mc,cps), q_mstr(md,cps), sqlc) 
    sqlq = "%s UNION\n%s" % (sql2, sql1)
    
    return sqlq

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
    if ncols != '': ncols = ',' + ncols
    if ccols != '': ccols = ',' + ccols
    sql1 = 'SELECT concat("*",gtag) gtag, concat(%s) %s %s FROM %s WHERE gtag=%s %s\nUNION\n' % (kcols, ckey, ncols, stbl, rtag, cd)
    sql2 = 'SELECT t1.gtag,concat(%s) %s %s\n' % (kcol1,ckey,ccols)
    sql3 = 'FROM (SELECT * FROM %s WHERE gtag IN (%s) %s) t1\n' % (stbl, ctags, cd)
    sql4 = 'LEFT JOIN (SELECT * FROM %s WHERE gtag=%s %s) t2\n' % (stbl, rtag, cd)    
    sqlq = '%s%s%s%sOn %s ORDER BY %s, gtag' % (sql1, sql2, sql3, sql4, onk, ckey)
    
    return sqlq
    
    