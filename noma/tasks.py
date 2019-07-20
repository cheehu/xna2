from __future__ import absolute_import, unicode_literals
from celery import shared_task, current_task
import os, pathlib, traceback
from .models import NomaGrp, NomaSet, NomaGrpSet, queGrp, queSet, NomaStrMap, NomaSetAct
from .utils import nomaMain, nomaRetrace, get_qtbs
from django.db import connection, connections
import re, pandas as pd
import xml.etree.ElementTree as ET
import noma.tfunc
from .forms import XDBX, BDIR, ODIR

@shared_task
def nomaExec(id, total_count):
    if total_count == 0: return {'current': total_count, 'total': total_count, 'percent': 100, 'status': 'ok'}
    grp = NomaGrp.objects.get(pk=int(id))
    sdir = BDIR / grp.sdir
    if sdir.suffix == '.zip': sdir = pathlib.Path('%s/%s_unzip' % (sdir.parent, sdir.stem))
    if grp.name == 'noma_excel':
        sfile = sdir / grp.sfile
        sd = pd.read_excel(sfile, sheet_name=None)
        sd.pop('Index',None)
        esets = [NomaSet(name=k, type='xl', sepr=[k,0,0,None], eepr=[None,None], depr=None,xtag=None) for k in sd]
        gsets = [NomaGrpSet(grp=grp, seq=i, set=esets[i], sfile=k, ttbl=k) for i, k in enumerate(sd)]
    else:
        gsets = [] 
        for gs in grp.sets.all():
            if gs.set.name == 'noma_group':
                try: 
                    gp = NomaGrp.objects.get(name=gs.sfile)
                    gsets += gp.sets.all()
                except NomaGrp.DoesNotExist: continue
            else: gsets.append(gs)
    ldir = ODIR / grp.ldir
    noma.tfunc.G_SDICT.clear()
    noma.tfunc.G_SDICT.update({'ldir':ldir})
    f=open(ldir / (grp.name + '.log'), "w+")
    f.write("Executing NOMA Group: %s from Source DIR: %s\n" % (grp, sdir))
    dfsm = pd.DataFrame.from_records(NomaStrMap.objects.all().values())
    sm = dfsm.groupby('ctag_id')
    smap = {}
    for name,group in sm:
        smap[name] = {}
        for row,data in group.iterrows(): smap[name].update({data['ostr'] : data['cstr']})
    i, xlobj, pfile = 0, None, ''
    for grpset in gsets:
        set = NomaSet.objects.get(name=grpset.set) if grp.name != 'noma_excel' else esets[grpset.seq]
        f.write("\nExecuting NOMA Set: %s\n" % set.name)
        f.write("   Set Type: %s\n" % set.type)
        f.write("   Target Table: %s\n" % grpset.ttbl)
        f.write("   NOMA Actions Sequences:\n")
        hdrs = [] if grp.gtag == None else ['gtag']
        if grp.name != 'noma_excel': acts = set.acts.all()
        else: acts = [(NomaSetAct(set=grpset.set,seq=cn,sepr=cv,fname=cv)) for cn, cv in enumerate(sd[grpset.sfile].columns.values)]
        for act in acts:
            if act.fname != None and act.fname[0] != '_' and act.fname not in hdrs: hdrs.append(act.fname)
            f.write("       %s  -  %s  -  %s   -  %s\n" % (act.seq, act.fname, act.sepr, act.eepr))
        f.write("\n")
        sfile = sdir / grpset.sfile if grp.sfile == None else sdir / grp.sfile
        tfile = ldir / (set.name + '.tsv')
        f.write("   Source Files: %s\n" % sfile)
        if set.type == 'sq': 
            if sfile.name in get_qtbs(): sfiles = [sfile]
        else: sfiles = list(sdir.glob(grpset.sfile)) if grp.sfile == None else list(sdir.glob(grp.sfile))
        if sfiles:
            for sfile in sfiles:
                if set.type == 'xl' and sfile != pfile: xlobj = pd.ExcelFile(sfile)
                f.write("       %s\n" % sfile.name)
                i = i + 1
                msg = nomaMain(sfile, tfile, set, acts, smap, grp.gtag, xlobj)
                pfile = sfile
                f.write("%s\n" % msg)
                f.flush()
                os.fsync(f.fileno())
                if msg[:8] != 'Error!!!':
                    if grpset.ttbl == '-----': continue
                    hdr = ','.join(hd for hd in hdrs)
                    tf = "'%s'" % ('/'.join(fn for fn in str(tfile).split('\\')))
                    sqlq = "LOAD DATA LOCAL INFILE %s INTO TABLE %s FIELDS TERMINATED BY '\t' ESCAPED BY '\b' (%s)" % (tf, grpset.ttbl, hdr)
                    with connections[XDBX].cursor() as cursor:
                        try:
                            cursor.execute(sqlq)
                            f.write("Succesfully Loaded:\n  %s\n\n" % sqlq)
                            if not grpset.ocsv: os.remove(tfile)
                            current_task.update_state(state='PROGRESS',
                                          meta={'current': i, 'total': total_count, 'status': 'ok', 
                                                'percent': int((float(i) / total_count) * 100)})
                        except Exception as e:
                            os.remove(tfile)
                            info = '"%s"' % e
                            #info = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                            sta = 'Error occurs at NomaSet %s -- ' % set.name
                            current_task.update_state(state='FAILURE', meta={'status': sta, 'info': info})
                            return {'status': sta, 'info': info}
                
        else:
            f.write("       No file matching %s\n\n" % grpset.sfile if grp.sfile == None else grp.sfile)     
    f.close()
    return {'current': total_count, 'total': total_count, 'percent': 100, 'status': 'ok'}

@shared_task
def nomaQExe(id, total_count):
    grp = queGrp.objects.get(pk=int(id))
    ldir = ODIR / grp.ldir
    tfile = ldir / grp.tfile
    writer = pd.ExcelWriter(tfile, engine='xlsxwriter')
    workbook = writer.book
    format_hdr = workbook.add_format({'bold': True, 'fg_color': '#D7E4BC'})
    format_title = workbook.add_format({'bold': True, 'fg_color': '#00FFFF', 'font_size':14})
    format_comp = workbook.add_format({'bold': True, 'fg_color': '#FFFF00'})
    Indexsheet = workbook.add_worksheet('Index')
    Indexsheet.write(0,0, grp.name, format_title)
    Indexsheet.write(0,1, '', format_title)
    Indexsheet.set_column(0, 0, 3)
    Indexsheet.set_column(1, 1, 80)
    f=open(ldir / (grp.name + '.log'), "w+")
    f.write("Executing Que Group: %s Output to DIR: %s\n\n" % (grp, ldir))
    i, j = 0, 0
    doc = ET.Element('config')
    for grpset in grp.sets.all():
        j += 1
        set = queSet.objects.get(name=grpset.set)
        f.write("Executing Query Set: %s\n" % set.name)
        Indexsheet.write(j+1,0, set.name, format_hdr)
        Indexsheet.write(j+1,1, '', format_hdr)
        f.write("   NOMA Queries Sequences:\n")
        for sq in set.Sqls.all():
            i += 1
            j += 1
            ps = "'%s',%s,%s,%s" % (sq.stbl,sq.qpar,grpset.spar,grp.gpar)
            pars = '(%s)' % ','.join(p for p in ps.split(',') if p != '')
            ps = pars if len(pars) < 100 else pars[:120]
            f.write("       %s  -  %s  -   %s%s\n" % (sq.seq, sq.name, sq.qfunc, pars))
            hl = 'internal:%s!A1' % sq.name
            Indexsheet.write_url(j+1,1, hl, string='%s: %s' % (sq.name,ps))
            qfun = 'noma.tfunc.%s%s' % (sq.qfunc, pars)
            try:
                sqlq = eval(qfun) 
                if str(sq.qfunc) == 'q_delete':
                    with connections[XDBX].cursor() as cursor:
                        cursor.execute(sqlq)
                else: 
                    df = pd.read_sql_query(sqlq, connections[XDBX])
                    if '_comp_' in df.columns.values:
                        if any('<>' in s for s in df['_comp_'].values):
                            Indexsheet.write(j+1,2, 'Data Different!!!', format_comp)
                    excelout(df,writer,workbook,sq.name,ldir)
                    if str(sq.qfunc) == 'q_basic':
                        cd = re.search('gtag="(.+?)"',sqlq)
                        if cd != None:
                            xtbl, xtbr = out_xml(sq.stbl,df,cd[0])
                            if xtbl != None: 
                                if xtbr == '': tr = doc
                                else:
                                    ttag = xtbr.split('/')
                                    tr = doc.find(ttag[0])
                                    if tr == None: tr = ET.SubElement(doc,ttag[0])
                                    if len(ttag) > 1:
                                        for k in ttag[1:]:
                                            c = tr.find(k)
                                            tr = ET.SubElement(tr, k) if c == None else c
                                tr.append(xtbl)
                    
                f.write("       Succesfully Executed:\n\n")
                current_task.update_state(state='PROGRESS',
                                          meta={'current': i, 'total': total_count, 'status': 'ok',
                                            'percent': int((float(i) / total_count) * 100)})
            except Exception as e:
                info = '"%s"' % e
                #info = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                sta = 'Error occurs at %s -- ' % sq.name
                current_task.update_state(state='FAILURE', meta={'status': sta, 'info': info})
                return {'status': sta, 'info': info}
        
        f.write("\n")
    writer.save()
    f.close()
    if len(doc) > 0: 
        xt = ET.ElementTree(doc)
        with open('%s.xml' % tfile, 'w+b') as xf: 
            xt.write(xf) 
        
    return {'current': total_count, 'total': total_count, 'percent': 100, 'status': 'ok',}

def out_xml(stbl, df, cd, sub=None):
    try: set = NomaSet.objects.get(name=stbl)
    except NomaSet.DoesNotExist: return None, None 
    if set.xtag == None: return None, None
    ttag = set.xtag.split(',')
    ttl = len(ttag)
    if ttag[0] == 's' and sub == None: return None, None
    acts = set.acts.filter(xtag__isnull=False)
    if len(acts) == 0: return None, None
    tbl = ET.Element(ttag[2])
    t = tbl
    for tag in ttag[3:-1]: t = ET.SubElement(t, tag)
    for rno,rec in df.iterrows():
        r = ET.SubElement(t, ttag[-1]) if ttl > 3 else t
        for act in acts:
            rtag = act.xtag.split(',')
            if rtag[0] == 'v':
                if rec[act.fname] == '': continue
                f = r.find(rtag[1])
                if f == None: f = ET.SubElement(r,rtag[1])
                if len(rtag) > 2:
                    for k in rtag[2:]:
                        c = f.find(k)
                        f = ET.SubElement(f, k) if c == None else c
                f.text = rec[act.fname]
            else:
                c1 = cd + ''.join(' and %s="%s"' % (k,rec[k]) for k in rtag[2:])
                sqlq = noma.tfunc.q_basic(rtag[1],['*','gtag'],c1)
                df1 = pd.read_sql_query(sqlq, connections[XDBX])
                if not df1.empty: r.append(out_xml(rtag[1],df1,c1,'')[0])
    return tbl, ttag[1]
    
def excelout(df,writer,workbook,sqn,ldir):
    format_col = workbook.add_format({'align':'left','font_size':9})
    format_hdr = workbook.add_format({'bold': True, 'fg_color': '#D7E4BC'})
    format_add = workbook.add_format({'bold': True, 'bg_color': '#FFFF00'})
    format_del = workbook.add_format({'bold': True, 'bg_color': '#FF0000'})
    format_mod = workbook.add_format({'bold': True, 'bg_color': '#FF00FF'})
    format_ref = workbook.add_format({'bold': True, 'bg_color': '#CCFFFF'})
    df.to_excel(writer, sheet_name=sqn, index=False, startrow=1, header=False)
    worksheet = writer.sheets[sqn]
    worksheet.conditional_format(1,0,len(df.index),len(df.columns)-1,{'type':'text','criteria':'containing','value':':##','format':format_add})
    worksheet.conditional_format(1,0,len(df.index),len(df.columns)-1,{'type':'text','criteria':'containing','value':'##:','format':format_del})
    worksheet.conditional_format(1,0,len(df.index),len(df.columns)-1,{'type':'text','criteria':'containing','value':'<>','format':format_mod})
    worksheet.conditional_format(1,0,len(df.index),len(df.columns)-1,{'type':'formula','criteria':'=LEFT($A2,1)="*"','format':format_ref})
    worksheet.write_url(0,0, 'internal:Index!A1')
    for cnum, value in enumerate(df.columns.values): worksheet.write(0, cnum, value, format_hdr)
    cn = len(df.columns)-1
    worksheet.autofilter(0,0,0,cn)
    worksheet.set_column(0, cn, None, format_col)
    worksheet.freeze_panes(1, 0)
    for c, col in enumerate(df.columns):
        clen = max(df[col].astype(str).map(len).max(), len(str(df[col].name))+1) + 1
        worksheet.set_column(c, c, clen)
    if 'pidxx' in df.columns.values: nomaRetrace(df, '%s%s' % (ldir, sqn))
        
    return True

    