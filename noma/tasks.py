from __future__ import absolute_import, unicode_literals
from celery import shared_task, current_task
import os, pathlib, traceback, copy
from .models import NomaGrp, NomaSet, NomaGrpSet, queGrp, queSet, NomaStrMap, NomaSetAct
from .utils import nomaMain, nomaRetrace, get_qtbs
from django.db import connection, connections
from django.conf import settings
import re, pandas as pd
import xml.etree.ElementTree as ET, xml.dom.minidom
import noma.tfunc
from .middleware import get_current_ngrp, set_db_for_router

@shared_task
def nomaExec(id, total_count,ngrp):
    if total_count == 0: return {'current': total_count, 'total': total_count, 'percent': 100, 'status': 'ok'}
    set_db_for_router(ngrp[0])
    grp = NomaGrp.objects.get(pk=int(id))
    bdir = pathlib.Path(settings.GRP_DIR) / ngrp[2] / 'uploads'
    sdir = bdir / grp.sdir
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
    odir = pathlib.Path(settings.GRP_DIR) / ngrp[2] / 'downloads'
    ldir = odir / grp.ldir
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
            if sfile.name in get_qtbs(ngrp[1]): sfiles = [sfile]
        else: sfiles = list(sdir.glob(grpset.sfile)) if grp.sfile == None else list(sdir.glob(grp.sfile))
        if sfiles:
            for sfile in sfiles:
                if set.type == 'xl' and sfile != pfile: xlobj = pd.ExcelFile(sfile)
                f.write("       %s\n" % sfile.name)
                i = i + 1
                try: msg = nomaMain(sfile, tfile, set, acts, smap, grp.gtag, xlobj, ngrp[1])
                except Exception as e:
                    info = '"%s"' % e
                    sta = 'Error occurs at NomaSet %s -- ' % set.name
                    current_task.update_state(state='FAILURE', meta={'status': sta, 'info': info})
                    return {'status': sta, 'info': info}
                pfile = sfile
                f.write("%s\n" % msg)
                f.flush()
                os.fsync(f.fileno())
                if msg[:8] != 'Error!!!':
                    if grpset.ttbl == '-----': continue
                    hdr = ','.join(hd for hd in hdrs)
                    tf = "'%s'" % ('/'.join(fn for fn in str(tfile).split('\\')))
                    sqlq = "LOAD DATA LOCAL INFILE %s INTO TABLE %s FIELDS TERMINATED BY '\t' ESCAPED BY '\b' (%s)" % (tf, grpset.ttbl, hdr)
                    with connections[ngrp[1]].cursor() as cursor:
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
def nomaQExe(id, total_count, ngrp):
    xdbx = ngrp[1]
    set_db_for_router(ngrp[0])
    grp = queGrp.objects.get(pk=int(id))
    odir = pathlib.Path(settings.GRP_DIR) / ngrp[2] / 'downloads'
    ldir = odir / grp.ldir
    tfile = ldir / grp.tfile
    writer = pd.ExcelWriter(tfile, engine='xlsxwriter')
    workbook = writer.book
    format_hdr = workbook.add_format({'bold': True, 'fg_color': '#D7E4BC'})
    format_title = workbook.add_format({'bold': True, 'fg_color': '#00FFFF', 'font_size':14})
    format_comp = workbook.add_format({'bold': True, 'fg_color': '#FFFF00'})
    Indexsheet = workbook.add_worksheet('Index')
    gpars = '(%s)' % ','.join(p for p in grp.gpar.split(',') if p != '' and p[:2] != '__')
    Indexsheet.write(0,0, '%s %s' % (grp.name, '' if gpars == '()' else gpars), format_title)
    Indexsheet.write(0,1, '', format_title)
    Indexsheet.set_column(0, 0, 3)
    Indexsheet.set_column(1, 1, 80)
    f=open(ldir / (grp.name + '.log'), "w+")
    f.write("Executing Que Group: %s Output to DIR: %s\n\n" % (grp, ldir))
    i, j = 0, 0
    xdg = {p.split('=')[0][2:]: p.split('=')[1] for p in grp.gpar.split(',') if p[:2] == '__'}
    dashf = '%s' % tfile if 'xdash' in xdg else None
    if 'xroot' in xdg:
        doc = ET.Element(xdg['xroot'])
        for k, v in xdg.items(): 
            if k[:5] == 'xmlns': doc.set(k,v)
    else: doc = None
    for grpset in grp.sets.all():
        j += 1
        set = queSet.objects.get(name=grpset.set)
        f.write("Executing Query Set: %s\n" % set.name)
        spars = '(%s)' % ','.join(p for p in grpset.spar.split(',') if p != '' and p[:2] != '__')
        Indexsheet.write(j+1,0, '%s %s' % (set.name, '' if spars == '()' else spars), format_hdr)
        Indexsheet.write(j+1,1, '', format_hdr)
        f.write("   NOMA Queries Sequences:\n")
        if doc != None:
            xds = {p.split('=')[0][2:]: p.split('=')[1] for p in grpset.spar.split(',') if p[:7] == '__xmlns'}
            for k, v in xds.items(): doc.set(k,v)
        for sq in set.Sqls.all():
            i += 1
            j += 1
            ps = "'%s','%s',%s,%s,%s" % (xdbx,sq.stbl,sq.qpar,grpset.spar,grp.gpar)
            pars = '(%s)' % ','.join(p for p in ps.split(',') if p != '' and p[:2] != '__')
            f.write("       %s  -  %s  -   %s%s\n" % (sq.seq, sq.name, sq.qfunc, pars))
            hl = 'internal:%s!A1' % sq.name
            qpars = '(%s)' % ','.join(p for p in sq.qpar.split(',') if p != '' and p[:2] != '__')
            Indexsheet.write_url(j+1,1, hl, string='%s %s' % (sq.name,'' if qpars == '()' else qpars))
            dpars = re.search('__dash=(.+)(,|$)',sq.qpar)
            if dpars: Indexsheet.write(j+1,4,dpars[1])
            qfun = 'noma.tfunc.%s%s' % (sq.qfunc, pars)
            try:
                sqlq = eval(qfun) 
                if str(sq.qfunc) == 'q_delete':
                    with connections[xdbx].cursor() as cursor:
                        cursor.execute(sqlq)
                else: 
                    df = pd.read_sql_query(sqlq, connections[xdbx])
                    Indexsheet.write(j+1,2, len(df))
                    if '_comp_' in df.columns.values:
                        if any('<>' in s for s in df['_comp_'].values):
                            Indexsheet.write(j+1,3, 'Data Different!!!', format_comp)
                    excelout(df,writer,workbook,sq.name,ldir,tfile.name.split('.')[0])
                    if str(sq.qfunc) == 'q_basic' and doc != None:
                        cd = re.search('gtag="(.+?)"',sqlq)
                        if cd != None:
                            xtbl, xtbr = out_xml(sq.stbl,df,cd[0],xdbx)
                            if xtbl != None: 
                                xdq = {p.split('=')[0][2:]: p.split('=')[1] for p in sq.qpar.split(',') if p[:7] == '__xmlns'}
                                for k, v in xdq.items(): doc.set(k,v)
                                if xtbr == '': tr = doc
                                else:
                                    ttag = xtbr.split('/')
                                    tr = doc.find(ttag[0])
                                    if tr == None: tr = ET.SubElement(doc,ttag[0])
                                    if len(ttag) > 1:
                                        for k in ttag[1:]:
                                            if k == '': break
                                            c = tr.find(k)
                                            tr = ET.SubElement(tr, k) if c == None else c
                                if isinstance(xtbl, list): 
                                    if ttag[-1] == '':
                                        for te in xtbl: tr.append(te)
                                    else:
                                        g = ET.SubElement(tr,xtbl[0].tag)
                                        for te in xtbl: g.append(te[0])
                                else: tr.append(xtbl)
                
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
    if doc != None and len(doc) > 0: 
        try:
            sdoc = doc
            if 'proot' in xdg and doc.find(xdg['proot']): 
                sdoc = doc.find(xdg['proot'])
                for name in doc.attrib: sdoc.set(name, doc.attrib[name])
            xmlstr = xml.dom.minidom.parseString(ET.tostring(sdoc)).toprettyxml('  ')
            with open('%s.xml' % tfile, 'w') as xf: 
                xf.write(xmlstr)
        except Exception as e:
            info = '"%s"' % e
            sta = 'Error occurs in xml output!'
            current_task.update_state(state='FAILURE', meta={'status': sta, 'info': info})
            return {'status': sta, 'info': info}
        
    return {'current': total_count, 'total': total_count, 'percent': 100, 'status': 'ok', 'dashf': dashf}

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
                sqlq = noma.tfunc.q_basic(xdbx,rtag[1],['*','gtag'],c1)
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
    

def excelout(df,writer,workbook,sqn,ldir,tfile):
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
    if 'pidxx' in df.columns.values: nomaRetrace(df, '%s\%s_%s.pcap' % (ldir, tfile, sqn))
        
    return True

    