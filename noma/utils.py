import os, pathlib, zipfile, pandas as pd
from django.db import connection, connections
import re, mmap, json, codecs, traceback
from django.conf import settings
import noma.tfunc
from .models import NomaGrp, NomaSet, NomaGrpSet, NomaSetAct
from .middleware import get_current_ngrp

def get_qtbs(xdbx, ptt=''):
    with connections[xdbx].cursor() as cursor:
        cursor.execute('SHOW TABLES')
    if ptt == '': tbls = [tb[0] for tb in cursor]
    else: tbls = [tb[0] for tb in cursor if re.search(ptt,tb[0])]
    return tbls

def dirzip(spath):
    bdir = pathlib.Path(settings.GRP_DIR) / get_current_ngrp()[2] / 'uploads'
    sdir = bdir / spath
    if sdir.suffix == '.zip':
        tdir = pathlib.Path('%s/%s_unzip' % (sdir.parent, sdir.stem))
        with zipfile.ZipFile(sdir,"r") as zip_ref:
            zip_ref.extractall(tdir)
        return tdir
    return sdir
 

def nomaInfo(nomagrp, id, nomaset):
    xdbx = get_current_ngrp()[1]
    grp = nomagrp.objects.get(pk=int(id))
    bdir = pathlib.Path(settings.GRP_DIR) / get_current_ngrp()[2] / 'uploads'
    sdir = bdir / grp.sdir
    if sdir.suffix == '.zip': sdir = pathlib.Path('%s/%s_unzip' % (sdir.parent, sdir.stem))
    if grp.name == 'noma_excel':
        sfile = sdir / grp.sfile
        if list(sdir.glob(grp.sfile)):
            sd = pd.read_excel(sfile, sheet_name=None)
            sd.pop('Index',None)
            esets = [NomaSet(name=k, type='xl', sepr=[k,0,0,None], eepr=[None,None], depr=None,xtag=None) for k in sd]
            grpsets = [NomaGrpSet(grp=grp, seq=i, set=esets[i], sfile=k, ttbl=k) for i, k in enumerate(sd)]
        else: return 'No file matching %s \n' % sfile
    else:
        grpsets = [] 
        for gs in grp.sets.all():
            if gs.set.name == 'noma_group':
                try: 
                    gp = NomaGrp.objects.get(name=gs.sfile)
                    grpsets += gp.sets.all()
                except NomaGrp.DoesNotExist: continue
            else: grpsets.append(gs)
    log_content = "NOMA Group:%s gtag:%s\n From Source DIR: %s\n\n" % (grp.name, grp.gtag, sdir)
    for grpset in grpsets:
        set = nomaset.objects.get(name=grpset.set) if grp.name != 'noma_excel' else esets[grpset.seq]
        log_content = log_content + "Execute NOMA Set: " + set.name + "\n"
        log_content = log_content + "   Set Type : " + set.type + "\n"
        log_content = log_content + "   Target Table: " + grpset.ttbl + "\n"
        sfile = sdir / grpset.sfile if grp.sfile == None else sdir / grp.sfile
        log_content = '%s   Source Files: %s\n' % (log_content, sfile)
        if set.type == 'sq': 
            sfiles = [sfile] if sfile.name in get_qtbs(xdbx) else []
        else: sfiles = list(sdir.glob(grpset.sfile)) if grp.sfile == None else list(sdir.glob(grp.sfile))
        if sfiles:
            for sfile in sfiles:
                log_content = log_content + "       " + sfile.name  + "\n"
        else:
            #log_content = log_content + "       No file matching " + grpset.sfile + "\n"
            log_content = '%s       No file matching %s \n' % (log_content, grpset.sfile if grp.sfile == None else grp.sfile)
        log_content = log_content + "   NOMA Actions Sequences:\n"
        if grp.name != 'noma_excel': acts = set.acts.all()
        else: acts = [(NomaSetAct(set=grpset.set,seq=cn,sepr=cv,fname=cv)) for cn, cv in enumerate(sd[grpset.sfile].columns.values)]
        for act in acts:
            log_content = log_content + "       " + str(act.seq)  + " - " +  str(act.fname) + "\n"
        log_content = log_content + "\n"
    return log_content

def nomaCount(nomagrp, id, nomaset):
    xdbx = get_current_ngrp()[1]
    grp = nomagrp.objects.get(pk=int(id))
    sdir = dirzip(grp.sdir)
    if grp.name == 'noma_excel':
        sfile = sdir / grp.sfile
        if list(sdir.glob(grp.sfile)):
            sd = pd.read_excel(sfile, sheet_name=None)
            sd.pop('Index',None)
            esets = [NomaSet(name=k, type='xl', sepr=[k,0,0,None], eepr=[None,None], depr=None,xtag=None) for k in sd]
            grpsets = [NomaGrpSet(grp=grp, seq=i, set=esets[i], sfile=k, ttbl=k) for i, k in enumerate(sd)]
        else: return 0
    else:
        grpsets = [] 
        for gs in grp.sets.all():
            if gs.set.name == 'noma_group':
                try: 
                    gp = NomaGrp.objects.get(name=gs.sfile)
                    grpsets += gp.sets.all()
                except NomaGrp.DoesNotExist: continue
            else: grpsets.append(gs)
    task_count = 0
    for grpset in grpsets:
        set = nomaset.objects.get(name=grpset.set) if grp.name != 'noma_excel' else esets[grpset.seq]
        sfile = sdir / grpset.sfile if grp.sfile == None else sdir / grp.sfile
        if set.type == 'sq': 
            sfiles = [sfile] if sfile.name in get_qtbs(xdbx) else []
        else: sfiles = list(sdir.glob(grpset.sfile)) if grp.sfile == None else list(sdir.glob(grp.sfile))
        if sfiles: task_count += len(sfiles)
    return task_count
    
def getRecords(s, srt_txt, end_txt):
    cmdP = 'Error!!! : Start text: %s not found' % srt_txt
    cmdS, cmdE = re.compile(srt_txt.encode()), re.compile(end_txt.encode())
    m = cmdS.search(s)
    if m != None:
        cmdP = 'Error!!! : End text: %s not found' % end_txt
        n = cmdE.search(s, m.end())
        if n != None:
            cmdP = s[m.end():n.start()]
           
    return cmdP
        
def getFields(po, sep, acts, vs, outf, smap):
    records = re.split(sep, po)
    rno = 0
    for rec in records:
        radd, skipf = True, 0
        varr = [[] for y in range(10)]
        vals = list(vs)
        rec.strip()
        for idx, act in enumerate(acts):
            if act.seq > 99: continue
            if skipf > 0: 
                skipf -= 1
                continue
            nepr = act.nepr if act.tfunc == None else None
            fs, fe = act.spos, act.epos
            m_s = re.search('\n', rec).start() if act.sepr == None and act.eepr == r'\n' else 0
            if act.sepr != None: 
                m = re.search(act.sepr, rec)
                fs = None if m == None else fs + m.end()
                if fs != None and fe != None: fe += m.end()
            if fs != None and act.eepr != None and m_s == 0:
                m = re.search(act.eepr, rec[fs:]) 
                if m != None: fe = fs + m.start()
            val = '' if fs == None else rec[fs:fe].strip() if m_s == 0 else rec[:m_s][fs:fe].strip()
            if act.tfunc != None:
                tfun = 'noma.tfunc.%s(%s)' % (act.tfunc, act.nepr)
                val = eval(tfun) 
            if nepr == None:
                if act.varr != None: varr[act.varr].append(val)
                if act.fname: vals.append(val)
                if act.fepr != None:
                    if not re.search(act.fepr, val): 
                        if act.skipf == 0: 
                            radd = False
                            break
                        skipf = act.skipf
                else: skipf = act.skipf
            else:
                if act.varr != None: val = varr[act.varr][0]
                getFields(val,nepr,acts[idx+1:],vals,outf,smap)
                break
        rno += 1
        if radd and nepr == None:
            outf.write('%s\n' % '\t'.join(va for va in vals))

            
def getJRecords(s, srt_txt):
    cmdP = 'Error!!! : Start text: %s not found' % srt_txt
    m = s[srt_txt]
    if m != None:
        cmdP = m
    return cmdP

def getJFields(records, acts, vs, outf):
    for rec in records:
        radd = True
        vals = list(vs)
        for idx, act in enumerate(acts):
            val = rec
            keys = act.sepr.split('.')
            if act.eepr == None:
                for key in keys:
                    val = val[key]
            else:
                val = next((dic for dic in rec if dic[keys[0]] == keys[1]), '')[act.eepr]
            if act.nepr != None:
                if val != None:
                    getJFields(val,acts[idx+1:],vals,outf)
                break
            else:
                if act.fname: vals.append(val)
                if act.fepr != None:
                    radd = re.search(act.fepr, val)
                    if not radd: break
        if radd and act.nepr == None:
            outf.write('%s\n' % '\t'.join(va for va in vals))

def getBRecords(s, sa):
    sp, ef, n1, n2 = int(sa[1]), int(sa[2]), 0, 0 
    if sp == 128: n1, n2 = 12, 4
    s.seek(sp)
    rec = []
    cp = sp
    while cp < ef:
        s.seek(cp+n1+8)
        plen = 16 + int.from_bytes(s.read(4),byteorder=sa[3])
        s.seek(cp+n1)
        rec.append(s.read(plen))
        cp += plen + n1 + n2
        pad = 4 - (plen % 4)
        if n2 == 4 and pad < 4: cp += pad 
    return rec

def getBFields(records, acts, vs, outf, sfa, smap):
    ed, sf = sfa[3], sfa[0]
    for rno, rec in enumerate(records):
        radd = True
        varr = [[] for y in range(10)]
        vals = list(vs)
        skipf, skipb = 0, 0
        for idx, act in enumerate(acts):
            if skipf > 0: 
                skipf -= 1
                continue
            tfun = 'bv.hex()' if act.tfunc == None else 'noma.tfunc.%s(%s)' % (act.tfunc, act.nepr)
            sp = None if act.spos == None else act.spos+skipb
            ep = None if act.epos == None else act.epos+skipb
            bv = rec[sp:ep]
            val = eval(tfun)
            if act.varr != None: varr[act.varr].append(val)
            skipb = skipb + val if act.skipb < 0 else skipb + act.skipb 
            if act.skipf > 0:
                if act.sepr == None: skipf = act.skipf
                else: 
                    if not re.search(act.sepr, val): skipf = act.skipf
            if act.fname: vals.append(val)
            if act.fepr != None:
                radd = re.search(act.fepr, val)
                if not radd: break
        if radd: outf.write('%s\n' % '\t'.join(va for va in vals))
 
def getQRecords(sf, gtag,xdbx):
    cond = '' if gtag == None else ' WHERE gtag="%s"' % gtag 
    sqlq = "SELECT * FROM %s%s" % (sf, cond)         
    try:
        df = pd.read_sql_query(sqlq, connections[xdbx])
    except Exception as e:
        info = '"%s"' % e
        #info = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        df = 'Error!!!: %s' % info 
    return df     

def getERecords(xl,sepr,eepr):
    try:
        skr = range(0,sepr[2]) if sepr[1] == None else range(sepr[1]+1,sepr[1]+sepr[2]+1)
        df = xl.parse(sepr[0],header=sepr[1],skiprows=skr,usecols=eepr[0],dtype=str)
        sr = 0
        if sepr[3] != None:
            ca = sepr[3].split(':')
            cf = ca[0] if ca[0].isdigit() else '"%s"' % ca[0]
            cd = 'df[%s].str.contains("%s")' % (cf, 'nan' if ca[1] == '' else ca[1])
            er = df.index[eval(cd)]
            if not er.empty: 
                sr = er[0]
                df = df[sr:]
        if eepr[1] != None:
            ca = eepr[1].split(':')
            cf = ca[0] if ca[0].isdigit() else '"%s"' % ca[0]
            cd = 'df[%s].str.contains("%s")' % (cf, 'nan' if ca[1] == '' else ca[1])
            er = df.index[eval(cd)]
            if not er.empty: df = df[:er[0]-sr]
    except Exception as e:
        info = '"%s"' % e
        #info = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        df = 'Error!!!: %s' % info        
    return df

def getEFields(df, acts, vs, outf, smap):
    for rno,rec in df.iterrows():
        radd, skipf = True, 0
        varr = [[] for y in range(10)]
        vals = list(vs)
        for idx, act in enumerate(acts):
            if skipf > 0: 
                skipf -= 1
                continue
            nepr = act.nepr if act.tfunc == None else None
            se = int(act.sepr) if act.sepr.isdigit() else act.sepr
            fs, fe = act.spos, act.epos
            val = '' if rec[se] == 'nan' else rec[se][fs:fe]
            if act.tfunc != None:
                tfun = 'noma.tfunc.%s(%s)' % (act.tfunc, act.nepr)
                val = eval(tfun) 
            if nepr == None:
                if act.varr != None: varr[act.varr].append(val)
                if act.fname: vals.append(val)
                if act.fepr != None:
                    if not re.search(act.fepr, val): 
                        if act.skipf == 0: 
                            radd = False
                            break
                        skipf = act.skipf
                else: skipf = act.skipf
            else:
                if act.varr != None: val = varr[act.varr][0]
                nepr = nepr.split(':')
                if nepr[0] == 's': val = [r.split(nepr[1]) for r in val.split(nepr[2])]
                sdf = pd.DataFrame(val)
                getEFields(sdf,acts[idx+1:],vals,outf,smap)
                break
        
        if radd and nepr == None: outf.write('%s\n' % '\t'.join(va for va in vals))

    
def nomaMain(sf, tf, set, acts, smap, gtag, xlobj, xdbx):
    if set.type == 'tx':
        with open(sf, 'rUb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
            res = getRecords(s, set.sepr, set.eepr)
        if res[:8] != 'Error!!!':
            with open(tf, 'w+b') as fw:            
                fw.write(res)
            with open(tf, 'r+', encoding='cp1252') as fw:
                res = fw.read()
            with open(tf, 'r+', newline='') as fw:
                fw.truncate()
                iniv = [] if gtag == None else [gtag]
                getFields(res, set.depr, acts, iniv, fw, smap)
            res = "Successfully Executed " + set.name 
    elif set.type == 'js':
        with codecs.open(sf, 'r', 'utf-8') as s:
            jf = json.load(s)
        res = getJRecords(jf, set.sepr)
        if res[:8] != 'Error!!!':
            with open(tf, 'w') as fw:
                getJFields(res, acts, [], fw)
            res = "Successfully Executed " + set.name
    elif set.type == 'bi':
        sfa = ['',0,0,'','']
        sfa[0] = '%s' % ('/'.join(fn for fn in str(sf).split('\\')))
        sfa[1] = set.sepr
        sfa[2] = sf.stat().st_size
        with open(sf, 'rUb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
            if set.sepr == '128': s.seek(8)
            mg = s.read(1)
            sfa[3] = 'little' if mg[0] in (212, 77) else 'big'
            res = getBRecords(s, sfa)
        sfa[4] = '%s,%s,%s,%s' % (sfa[0],sfa[1],sfa[2],sfa[3]) 
        noma.tfunc.G_DUPL = [None]*20
        noma.tfunc.G_QNO = [0]
        with open(tf, 'w') as fw:
            iniv = [] if gtag == None else [gtag]
            getBFields(res, acts, iniv, fw, sfa, smap)
        res = "Successfully Executed " + set.name
    elif set.type == 'xl':
        sepr, eepr = eval('list(%s)' % set.sepr), eval('list(%s)' % set.eepr)
        df = getERecords(xlobj,sepr,eepr)
        if isinstance(df, str):
            res = df
        else:
            with open(tf, 'w', newline='') as fw:
                iniv = [] if gtag == None else [gtag]
                getEFields(df, acts, iniv, fw, smap)
            res = "Successfully Executed " + set.name
    elif set.type == 'sq':
        df = getQRecords(sf.name,gtag,xdbx)
        if isinstance(df, str):
            res = df
        else:
            noma.tfunc.G_DUPL = [None]*20
            noma.tfunc.G_QNO = [0]
            with open(tf, 'w', newline='') as fw:
                iniv = [] if gtag == None else [gtag]
                getEFields(df, acts, iniv, fw, smap)
            res = "Successfully Executed " + set.name
        
    return res


def queInfo(quegrp, id, queset):
    grp = quegrp.objects.get(pk=int(id))
    odir = pathlib.Path(settings.GRP_DIR) / get_current_ngrp()[2] / 'downloads'
    ldir = odir / grp.ldir
    tfile = ldir / grp.tfile
    log_content = "Query Group:%s to Output File: %s\n\n" % (grp.name, tfile)
    grpsets = grp.sets.all()
    for grpset in grpsets:
        set = queset.objects.get(name=grpset.set)
        log_content = log_content + "Execute Query Set: " + set.name + "\n"
        log_content = log_content + "   Query SQL Sequences:\n"
        for sq in set.Sqls.all():
            ps = "'%s',%s,%s,%s" % (sq.stbl,sq.qpar,grpset.spar,grp.gpar)
            pars = '(%s)' % ','.join(p for p in ps.split(',') if p != '' and p[:2] != '__')
            log_content = log_content + "       %s  -  %s%s\n" % (sq.seq, sq.qfunc, pars)
        log_content = log_content + "\n"
    return log_content

def queCount(quegrp, id, queset):
    grp = quegrp.objects.get(pk=int(id))
    grpsets = grp.sets.all()
    task_count = 0
    for grpset in grpsets:
        set = queset.objects.get(name=grpset.set)
        for sq in set.Sqls.all():
            task_count += 1
    return task_count
    
    
def nomaCreateTbl(tb, acts, xdbx):
    hdrs, flds = [], []
    for act in acts:
        if act.fname != None and act.fname not in hdrs: 
            hdrs.append(act.fname)
            flds.append('%s %s' % (act.fname, act.fchar))
    fds = ','.join(fd for fd in flds)
    create_sql = "CREATE TABLE %s (%s)" % (tb, fds)
    with connections[xdbx].cursor() as cursor:
        cursor.execute(create_sql)
        
def nomaRetrace(df, tf):
    fw = open(tf, 'wb')
    fw.write(b'\xd4\xc3\xb2\xa1\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x01\x00\x00\x00')
    dff = df.groupby('pfile')
    for name,group in dff:
        sfa = name.strip().split(',')
        with open(sfa[0], 'rUb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
            res = getBRecords(s, sfa)
            for row,data in group.iterrows():
                segs = data['pidxx'].split('&')
                for seg in segs:
                    gtps = seg.split('-')
                    for gtp in gtps:
                        pkts = gtp.split(',')
                        for p in pkts: fw.write(res[int(p)])
                    


def nomaExecT(strm):
    dfsm = pd.DataFrame.from_records(strm)
    sm = dfsm.groupby('ctag_id')
    smap = {}
    for name,group in sm:
        smap[name] = {}
        for row,data in group.iterrows(): smap[name].update({data['ostr'] : data['cstr']})
    return smap                

'''
def nomaLoad(tf, tb, hd):
    load_sql = "LOAD DATA LOCAL INFILE %s INTO TABLE %s \
                FIELDS TERMINATED BY '\t' %s" % (tf, tb, hd)
    with connections[XDBX].cursor() as cursor:
        cursor.execute(load_sql)
'''
