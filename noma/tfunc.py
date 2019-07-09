from datetime import datetime
import codecs, re, csv, pandas as pd
from django.db import connection, connections
from .forms import XDBX
G_SDICT = {}
G_DUPL = []
G_QNO = []

def to_test(ba):
    return str(len(ba))

def to_byte_str(ba):
    return str(ba)[2:-1]

def to_bytes(bs):
    return bytes(codecs.decode(bs.strip(), 'unicode_escape'), 'iso_8859-1')

def rstr_to_str(bs):
    return bytes(codecs.decode(bs.strip(), 'unicode_escape'), 'iso_8859-1').decode()
    
def hex_to_int(bv):
    return str(int(bv, 16))
    
def hex_to_byte(hv):
    return bytes.fromhex(hv.strip())
    
def to_txt(bv):
    return bv.decode()

def to_ipv4(bv):
    return ".".join(str(b) for b in bv)
        
def to_intbit(bv, s, l):
    e = int.from_bytes(bv,byteorder='big')
    d = 2 ** s
    c = d * ((2 ** l) - 1)
    return str(int((e & c) / d))

def to_intbitn(bv, s, l):
    e = int.from_bytes(bv,byteorder='big')
    d = 2 ** s
    c = d * ((2 ** l) - 1)
    return int((e & c) / d)
        
def to_hex(bv, d=None, n=None):
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
    for p in pa:
        for k,v in sm.items():
            if p in k.split(','): return v
    return 'unk'

def to_hdl(bv,s, l, hd):
    e = int.from_bytes(bv,byteorder='big')
    d = 2 ** s
    c = d * ((2 ** l) - 1)
    hl = 4 * int((e & c) / d)
    return hl - hd

def i_var(iv):
    return str(iv)
    
def i_vara(iv,dl):
    return dl.join(str(v) for v in iv)

def to_paylen(bv,ver):
    if ver == 6: 
        cv = to_intBn(bv[:2])-8 if bv[2] == b'44' else to_intBn(bv[:2])
    elif ver == 4: cv = to_intBn(bv[2:]) - to_intbitn(bv[0:1],0,4)*4
    return str(cv) 
    
def dupp(bv,bs):
    if bv in G_DUPL: return 'Y'
    if bs > 0:
        G_DUPL[G_QNO[0]] = bv
        G_QNO[0] = 0 if G_QNO[0] == bs else G_QNO[0] + 1 
    return 'N'
    
def defrag(recs,rno,skb,bv,va,bs):
    tl = to_intBn(bv[2:4])
    if tl == 0: return '0'
    if tl < len(bv): bv = bv[:tl]
    cv = str(bv[9])
    ofs = to_intbitn(bv[6:8],0,13)
    mf = to_intbit(bv[6:7],5,1)
    hl = to_intbitn(bv[0:1],0,4)*4
    fr0 = bv[8+hl:] if cv =='50' and ofs == 0 else bv[hl:]
    if cv =='50' and mf == '0': 
        pl = bv[-14] + 14
        fr0 = fr0[:-pl]
        cv = str(bv[-13])
    if ofs == 0 and mf != '1':
        va.append([rno,fr0])
        return cv
    rl = len(recs)
    clen = tl - hl
    pid0 = to_intB(bv[4:6])
    frl = {ofs:[rno,fr0]}
    tlen = ofs*8 + clen if ofs != 0 and mf == '0' else 0 
    for i in range(1,20): 
        rn = rno + i
        if rn >= rl: break
        if recs[rn][28+skb:30+skb:].hex() != '0800': continue
        rv = recs[rn][30+skb:]
        if rv in G_DUPL: continue
        tl = to_intBn(rv[2:4])
        bv = rv[:tl]
        ofs = to_intbitn(bv[6:8],0,13)
        mf = to_intbit(bv[6:7],5,1)
        pid = to_intB(bv[4:6])
        if pid == pid0: 
            hl = to_intbitn(bv[0:1],0,4)*4
            ipdl = tl - hl
            clen += ipdl
            if ofs != 0 and mf == '0': tlen = ofs*8 + ipdl
            nh = str(bv[9])
            fr = bv[8+hl:] if nh =='50' and ofs == 0 else bv[hl:]
            if nh =='50' and mf == '0': 
                pl = bv[-14] + 14
                fr = fr[:-pl]
                cv = str(bv[-13])
            frl.update({ofs:[rn, fr]})
            G_DUPL[G_QNO[0]] = rv
            G_QNO[0] = 0 if G_QNO[0] == bs else G_QNO[0] + 1 
            if clen == tlen: break
    frs = dict(sorted(frl.items()))
    pid, ipd = '', b''
    for k,v in frs.items():
        pid += ',%s' % v[0]
        ipd += v[1]
    va.append([pid[1:],ipd])
    return cv

def defrag_g(recs,rno,bv,va,bs):
    cv = str(bv[9])
    ofs = to_intbitn(bv[6:8],0,13)
    mf = to_intbit(bv[6:7],5,1)
    hl = to_intbitn(bv[0:1],0,4)*4
    fr0 = bv[8+hl:] if cv =='50' and ofs == 0 else bv[hl:]
    if cv =='50' and mf == '0': 
        pl = bv[-14] + 14
        fr0 = fr0[:-pl]
        cv = str(bv[-13])
    if ofs == 0 and mf != '1':
        va.append([recs.loc[rno,'pidxx'],fr0])
        return cv
    rl = len(recs)
    clen = to_intBn(bv[2:4]) - hl
    pid0 = to_intB(bv[4:6])
    frl = {ofs:[recs.loc[rno,'pidxx'],fr0]}
    tlen = ofs*8 + clen if ofs != 0 and mf == '0' else 0 
    for i in range(1,20): 
        rn = rno + i
        if rn >= rl: break
        bv = to_bytes(recs.loc[rn,'apd'])
        ofs = to_intbitn(bv[6:8],0,13)
        mf = to_intbit(bv[6:7],5,1)
        pid = to_intB(bv[4:6])
        if pid == pid0: 
            hl = to_intbitn(bv[0:1],0,4)*4
            ipdl = to_intBn(bv[2:4]) - hl
            clen += ipdl
            if ofs != 0 and mf == '0': tlen = ofs*8 + ipdl
            nh = str(bv[9])
            fr = bv[8+hl:] if nh =='50' and ofs == 0 else bv[hl:]
            if nh =='50' and mf == '0': 
                pl = bv[-14] + 14
                fr = fr[:-pl]
                cv = str(bv[-13])
            frl.update({ofs:[recs.loc[rn,'pidxx'], fr]})
            G_DUPL[G_QNO[0]] = bv
            G_QNO[0] = 0 if G_QNO[0] == bs else G_QNO[0] + 1 
            if clen == tlen: break
    frs = dict(sorted(frl.items()))
    pid, ipd = '', b''
    for k,v in frs.items():
        pid += '-%s' % v[0]
        ipd += v[1]
    va.append([pid[1:],ipd])
    return cv
    
    
def defrag6(recs,rno,skb,bv,va,bs):
    cv = str(bv[6])
    if cv != '44':
        if cv == '50':
            pl = len(bv) - bv[-14] - 14
            fr0 = bv[48:pl]
            cv = str(bv[-13])
        else: fr0 = bv[40:]
        va.append([rno,fr0])
        return cv
    rl = len(recs)
    clen = to_intBn(bv[4:6]) - 8
    pid0 = to_intB(bv[44:48])
    ofs = to_intbitn(bv[42:44],3,13)
    mf = to_intbit(bv[43:44],0,1)
    cv = str(bv[40])
    fr0 = bv[56:] if cv =='50' and ofs == 0 else bv[48:]
    if cv =='50' and mf == '0': 
        pl = bv[-14] + 14
        fr0 = fr0[:-pl]
        cv = str(bv[-13])
    frl = {ofs:[rno,fr0]}
    tlen = ofs*8 + clen if ofs != 0 and mf == '0' else 0 
    for i in range(1,20): 
        rn = rno + i
        if rn >= rl: break
        if recs[rn][28+skb:30+skb:].hex() != '86dd': continue
        bv = recs[rn][30+skb:]
        if bv in G_DUPL: continue
        if str(bv[6]) == '44':
            pid = to_intB(recs[rn][74+skb:78+skb])
            if pid == pid0: 
                ofs = to_intbitn(bv[42:44],3,13)
                mf = to_intbit(bv[43:44],0,1)
                ipdl = to_intBn(bv[4:6]) - 8
                clen += ipdl
                if ofs != 0 and mf == '0': tlen = ofs*8 + ipdl
                nh = str(bv[40])
                fr = bv[56:] if nh =='50' and ofs == 0 else bv[48:]
                if nh =='50' and mf == '0': 
                    pl = bv[-14] + 14
                    fr = fr[:-pl]
                    cv = str(bv[-13])
                frl.update({ofs:[rn, fr]})
                G_DUPL[G_QNO[0]] = bv
                G_QNO[0] = 0 if G_QNO[0] == bs else G_QNO[0] + 1 
                if clen == tlen: break
    frs = dict(sorted(frl.items()))
    pid, ipd = '', b''
    for k,v in frs.items():
        pid += ',%s' % v[0]
        ipd += v[1]
    va.append([pid[1:],ipd])
    return cv

def defrag6g(recs,rno,bv,va,bs):
    cv = str(bv[6])
    if cv != '44':
        if cv == '50':
            pl = len(bv) - bv[-14] - 14
            fr0 = bv[48:pl]
            cv = str(bv[-13])
        else: fr0 = bv[40:]
        va.append([recs.loc[rno,'pidxx'],fr0])
        return cv
    rl = len(recs)
    clen = to_intBn(bv[4:6]) - 8
    pid0 = to_intB(bv[44:48])
    ofs = to_intbitn(bv[42:44],3,13)
    mf = to_intbit(bv[43:44],0,1)
    cv = str(bv[40])
    fr0 = bv[56:] if cv =='50' and ofs == 0 else bv[48:]
    if cv =='50' and mf == '0': 
        pl = bv[-14] + 14
        fr0 = fr0[:-pl]
        cv = str(bv[-13])
    frl = {ofs:[recs.loc[rno,'pidxx'],fr0]}
    tlen = ofs*8 + clen if ofs != 0 and mf == '0' else 0 
    for i in range(1,20): 
        rn = rno + i
        if rn >= rl: break
        bv = to_bytes(recs.loc[rn,'apd'])
        if str(bv[6]) == '44':
            pid = to_intB(bv[44:48])
            if pid == pid0: 
                ofs = to_intbitn(bv[42:44],3,13)
                mf = to_intbit(bv[43:44],0,1)
                ipdl = to_intBn(bv[4:6]) - 8
                clen += ipdl
                if ofs != 0 and mf == '0': tlen = ofs*8 + ipdl
                nh = str(bv[40])
                fr = bv[56:] if nh =='50' and ofs == 0 else bv[48:]
                if nh =='50' and mf == '0': 
                    pl = bv[-14] + 14
                    fr = fr[:-pl]
                    cv = str(bv[-13])
                frl.update({ofs:[recs.loc[rn,'pidxx'], fr]})
                G_DUPL[G_QNO[0]] = bv
                G_QNO[0] = 0 if G_QNO[0] == bs else G_QNO[0] + 1 
                if clen == tlen: break
    frs = dict(sorted(frl.items()))
    pid, ipd = '', b''
    for k,v in frs.items():
        pid += '-%s' % v[0]
        ipd += v[1]
    va.append([pid[1:],ipd])
    return cv
    
    
def apdata(bv,tt,sgnp,va):
    dl = len(bv)
    if tt == '132':
        sp = to_intBn(bv[14:16]) if bv[12] == 3 else 0
        if dl > (12+sp) and bv[12+sp] == 0:
                va.append(bv[sp+28:])
                va.append(to_intbitn(bv[sp+13:sp+14],0,4) % 2)
                va.append(to_intBn(bv[sp+16:sp+20]))
                va.append(dl - sp - 28)
        else: va.append('')
    elif tt == '6': 
        flg = to_intbitn(bv[13:14],0,4)
        if flg == 0 or flg == 8:
            hl = to_intbitn(bv[12:13],4,4) * 4
            va.append(bv[hl:])
            va.append(flg)
            va.append(to_intBn(bv[4:8]))
            va.append(dl - hl)
        else: va.append('')
    else: 
        va.append(bv[16:]) if sgnp == 'gtp' else va.append(bv[8:])
        va.append('')
        va.append('')
        va.append(dl - 16) if sgnp == 'gtp' else va.append(dl - 8)
    return '' if va[0] == '' or va[3] == 0 else str(va[3])    


def ap_msg(apd,sgnp='sip',dc=None):
    if sgnp == 'sip':
        m = re.search('(^(?=[A-Z]+\s(sip|tel):)|(?<=^SIP/2\.0\s))\S{3}', apd.decode('utf-8'))
        cv = 'NaN' if m == None else m[0]
    elif sgnp == 'dia': 
        if apd[0] == 1 and to_intbitn(apd[4:5],0,4) == 0 and to_intbitn(apd[5:7],4,12) == 0:
            cc = to_intB(apd[5:8])
            cv = '%s%s' % (dc[cc] if cc in dc else cc, 'R' if to_intbitn(apd[4:5],4,4) > 7 else 'A')
        else: cv = 'NaN'
    elif sgnp == 'gtp': 
        cv = 'ipv%s' % to_intbit(apd[0:1],4,4)
    return cv

def sipmsg(sipreq):
    m = re.search('(^(?=[A-Z]+\s(sip|tel):)|(?<=^SIP/2\.0\s))\S{3}', sipreq) 
    return 'NaN' if m == None else m[0]      

def deseg(recs,rno,va,bs):
    fst,lst,apl = recs.loc[rno,'apmsg'], recs.loc[rno,'last'], recs.loc[rno,'appl']
    sid0 = '%s%s%s%s' % (recs.loc[rno,'ip_s'], recs.loc[rno,'port_s'], recs.loc[rno,'ip_d'], recs.loc[rno,'port_d'])
    if fst != 'NaN' and lst == '8': 
        va.append([recs.loc[rno,'pidxx'],recs.loc[rno,'apd']])
        return apl
    mss, flen, rl = '', 0, len(recs)
    seg0 = recs.loc[rno,'segid']
    if fst == 'NaN': 
        for i in range(1,20): 
            rn = rno + i
            if rn >= rl: break
            if recs.loc[rn,'ts_dt'][-5:] != recs.loc[rno,'ts_dt'][-5:]: break
            sid = '%s%s%s%s' % (recs.loc[rn,'ip_s'], recs.loc[rn,'port_s'], recs.loc[rn,'ip_d'], recs.loc[rn,'port_d'])
            seg = recs.loc[rn,'segid']
            if sid+seg[:4] == sid0+seg0[:4]: 
                if recs.loc[rn,'apmsg'] != 'NaN': 
                    mss, flen = recs.loc[rn,'appl'], int(seg)
                    break
    else: mss, flen = apl, int(seg0)
    if mss == '': return ''
    if lst == '8' and fst == 'NaN': 
        if apl == mss: lst = '0'
    tlen,clen = 0,0
    frl = {seg0:[recs.loc[rno,'pidxx'],recs.loc[rno,'apd']]}
    appl = int(apl)
    if lst == '0': clen = appl 
    else: tlen = int(seg0)
    for i in range(1,20): 
        rn = rno + i
        if rn >= rl: break
        if recs.loc[rn,'ts_dt'][-5:] != recs.loc[rno,'ts_dt'][-5:]: break
        sid = '%s%s%s%s' % (recs.loc[rn,'ip_s'], recs.loc[rn,'port_s'], recs.loc[rn,'ip_d'], recs.loc[rn,'port_d'])
        seg = recs.loc[rn,'segid']
        if sid+seg[:4] == sid0+seg0[:4]: 
            fst,lst,apl = recs.loc[rn,'apmsg'], recs.loc[rn,'last'], recs.loc[rn,'appl']
            if fst != 'NaN' and int(seg) > flen: break 
            appl += int(apl)
            if fst == 'NaN' and lst == '8':
                if apl == mss: lst = '0'
            if lst == '0': clen += appl
            else: tlen = int(seg)
            frl.update({seg:[recs.loc[rn,'pidxx'], recs.loc[rn,'apd']]})
            G_DUPL[G_QNO[0]] = seg
            G_QNO[0] = 0 if G_QNO[0] == bs else G_QNO[0] + 1 
            if flen + clen == tlen: break
    frs = dict(sorted(frl.items()))
    pid, ipd = '', ''
    for k,v in frs.items():
        pid += '&%s' % v[0]
        ipd += v[1]
    va.append([pid[1:],ipd])
    return str(appl)

def deseg_s(recs,rno,va,bs):
    fst,lst,apl = recs.loc[rno,'apmsg'], recs.loc[rno,'last'], recs.loc[rno,'appl']
    sid0 = '%s%s%s%s' % (recs.loc[rno,'ip_s'], recs.loc[rno,'port_s'], recs.loc[rno,'ip_d'], recs.loc[rno,'port_d'])
    if fst != 'NaN' and lst == '1': 
        va.append([recs.loc[rno,'pidxx'],recs.loc[rno,'apd']])
        return apl
    rl = len(recs)
    seg0 = recs.loc[rno,'segid']
    frl = {seg0:[recs.loc[rno,'pidxx'],recs.loc[rno,'apd']]}
    appl = int(apl)
    flen = 0 if fst == 'NaN' else int(seg0)
    tlen = 0 if lst == '0' else int(seg0)
    for i in range(1,20): 
        rn = rno + i
        if rn >= rl: break
        tid = int(recs.loc[rn,'segid'])
        if abs(tid - int(seg0)) < 10: 
            if recs.loc[rn,'last'] == '1' and tlen == 0: tlen = tid
            elif recs.loc[rn,'apmsg'] != 'NaN' and flen == 0: flen = tid
            if flen > 0 and tlen > 0: break
    if tlen == 0 or flen == 0: return ''     
    j = 0
    for i in range(1,20): 
        rn = rno + i
        if rn >= rl: break
        if recs.loc[rn,'ts_dt'][-5:] != recs.loc[rno,'ts_dt'][-5:]: break
        sid = '%s%s%s%s' % (recs.loc[rn,'ip_s'], recs.loc[rn,'port_s'], recs.loc[rn,'ip_d'], recs.loc[rn,'port_d'])
        seg = recs.loc[rn,'segid']
        if flen <= int(seg) <= tlen and sid == sid0: 
            appl += int(recs.loc[rn,'appl'])
            j += 1
            frl.update({seg:[recs.loc[rn,'pidxx'], recs.loc[rn,'apd']]})
            G_DUPL[G_QNO[0]] = seg
            G_QNO[0] = 0 if G_QNO[0] == bs else G_QNO[0] + 1 
            if flen + j == tlen: break
    frs = dict(sorted(frl.items()))
    pid, ipd = '', ''
    for k,v in frs.items():
        pid += '&%s' % v[0]
        ipd += v[1]
    va.append([pid[1:],ipd])
    return str(appl)
    
       
def siphdr(sipmsg,sepr,eepr):
    hdr = ''
    m = re.search(sepr, sipmsg)
    if m != None:
        fs = m.end()
        m = re.search(eepr, sipmsg[fs:])
        fe = fs + 10 if m == None else fs + m.start()
        hdr = sipmsg[fs:fe]
    return hdr
    
    
def dumm(ep):
    return ep
    
def conc(varr,d):
    return d.join(va for va in varr if va != '')

def repstr(val,ostr,nstr):
    return re.sub(ostr,nstr,val)
    
def x_col(val,dp,sp,ep,dl):
    return dl.join(r[sp:ep].strip() for r in val.split(dp))

def steps(n,s,d=','):    
    cv = d.join('%s&&%s' % (x-s,x-1) for x in range(s,n+1,s))
    l = n % s
    if l > 0: cv += '%s%s&&%s' % ('' if cv == '' else ',', n-l, n-1)
    return cv
    
def x_grp(va,k,fc=['G'],dp=',',d=','):
    data = {}
    data.update({k:va[k].split(dp)})
    if fc[0] == 'G':
        df = pd.DataFrame(data).sort_values(k).groupby(k).count()
        return d.join(idx for idx, row in df.iterrows())
    data.update({fc[1]:va[fc[1]].split(dp) if fc[0] != 'S' else list(map(int, va[fc[1]].split(dp)))})
    if fc[0] == 'C':
        df = pd.DataFrame(data).sort_values(k).groupby(k).count()
    elif fc[0] == 'S':
        df = pd.DataFrame(data).sort_values(k).groupby(k).sum()
    elif fc[0] == 'F':
        df = pd.DataFrame(data).sort_values(k).groupby(k).first()
    elif fc[0] == 'L':
        df = pd.DataFrame(data).sort_values(k).groupby(k).last()
    return d.join('%s:%s' % (idx,row[fc[1]]) for idx, row in df.iterrows())
    
    
def rep_if(val,ostr,ystr,nstr=''):
    if nstr == '': nstr = val
    cv = ystr if re.search(ostr,val) else nstr 
    return cv

def reps_if(val,ostr,ystr,nstr=''):
    if nstr == '': nstr = val
    cv = re.sub(ostr,ystr,val) if re.search(ostr,val) else nstr 
    return cv     
    
def nume(val):
    if not val.isdigit(): val = ''
    return val

def num_th(val,th,tl=5):
    if val.isdigit():
        val = re.sub(",",".",val)
        cv = str(int(float(val) * th))
    else: cv = val[0:tl]
    return cv

def repstra(val,dc,od,nd,tl):
    vs = re.sub(od,',',val).split(',')
    cvs = []
    for v in vs:
        cv = dc[v] if v in dc else v[0:tl]
        cvs.append(cv)
    return nd.join(cv for cv in cvs)

def repstr1(val,dc,tl,c=None):
    if val in dc:
        cv = dc[val]
        if c != None: cv = cv.split(',')[c]
    else: cv = val[0:tl]
    return cv
    
def repstr2(val,od,nd,sfx):
    vs = re.sub(od,',',val).split(',')
    cv = '' if val == '' else nd.join('%s%s' % (v,sfx) for v in vs)
    return cv

def ip_gw(ipa, mask, fa=True):
    m = re.search('(\d+\.\d+\.\d+\.)(\d+)',ipa)
    a = int(m[2])
    s = 256/(2**(int(mask)-24))
    n = int(a - (a % s))
    g = n + 1 if fa else n + s - 1
    return '%s%s' % (m[1],g)
    
    
def nx_line(va, sepr,l,n=1):
    stx = '^(.*?)%s(.*?\n){%s}' % (sepr, n)
    a = re.search(stx, va)
    if a != None:
        sp = a.end() + len(a.group(1))
        ep = sp + l
        a = va[sp:ep].strip()
    return '' if a == None else a
    
def lookup_c(st, sf, kc, vc):
    if sf not in G_SDICT:
        G_SDICT[sf] = {}
        csvf = G_SDICT['ldir'] / ('%s.tsv' % sf)
        if csvf.is_file():
            with open(csvf) as s: 
                cf = csv.reader(s, delimiter='\t')
                for row in cf: G_SDICT[sf].update({row[kc] : row[vc]})
    cv = G_SDICT[sf][st] if st in G_SDICT[sf] else st
    return cv
    
def trim(val,s,e):
    return val[s:e]
    
def xl_col(rec,cols,cd,dc):
    cls, cv = [], ''
    for cos in cols.split('&'): cls += eval('list(range(%s))' % cos)
    for c in cls:
        if re.search(cd,rec[c]):
            cs = str(c)
            cv = dc[cs] if cs in dc else 'n.a.'
            if cv == 'NBR': cv = rec[c]
            break
    return cv

def xl_colv(rec,cols,cd,dl=' '):
    cls = []
    for cos in cols.split('&'): cls += eval('list(range(%s))' % cos)
    cv = dl.join(rec[c] for c in cls if re.search(cd,rec[c]))
    return cv

def xl_hdrv(rec,cols,cd,dc):
    cls = []
    for cos in cols.split('&'): cls += eval('list(range(%s))' % cos)
    cv = ','.join('%s=%s' % (dc[str(c)] if str(c) in dc else "n_a", cd) for c in cls if re.search(cd,rec[c]))
    return cv

def xl_hdr_v(rec,cols,dc):
    cls = []
    for cos in cols.split('&'): cls += eval('list(range(%s))' % cos)
    cv = ','.join('%s=%s' % (dc[str(c)] if str(c) in dc else "n_a", rec[c]) for c in cls)
    return cv    

def d_expand(d):
    if d[-1] == '%': return [d[:-1]]
    m = re.search('&',d)
    if m == None: return [d]
    d = re.sub('&&-|&&(?=\d)','~',d)
    d = re.sub('&-','&',d)
    e = d.split('&')
    if len(e) > 1: e = [e[0]] + ['%s%s' % (e[0][:-1], c) for c in e[1:]]
    g = []
    for k in e:
        d = k.split('~')
        if len(d) > 1:
            if len(d[1]) == 1: 
                p = d[0][:-1]
                f = int(d[0][-1])
                j = int(d[1]) - f
            else:
                for b in range(len(d[0])):
                    if d[0][b] != d[1][b]: break
                p = d[0][:b-1]
                f = int(d[0][b:])
                j = int(d[1][b:]) - f
            g += ['%s%s' % (p,(f+i)) for i in range(j+1)]
        else: g.append(k)
    return g
    

    
def q_basic(stbl, flds=['*'], cond=''):
    if flds[0] == '*': 
        if len(flds) > 1:
            with connections[XDBX].cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM %s" % stbl)
            cols = ','.join(cn[0] for cn in cursor if cn[0] not in flds)
        else: cols = '*'
    else:
        cols = ','.join(cn for cn in flds)
    if cond != '': cond = ' WHERE %s' % cond 
    sqlq = "SELECT %s FROM %s%s" % (cols, stbl, cond)
    return sqlq
    
def q_grpby(stbl, gby, fn, cond=''):
    if cond != '': cond = ' WHERE %s' % cond 
    sqlq = "SELECT %s, %s FROM %s%s Group BY %s" % (gby, fn, stbl, cond, gby)
    return sqlq

def q_delete(stbl, cond='gtag is null'):
    if cond != '': cond = ' WHERE %s' % cond 
    sqlq = "DELETE FROM %s%s" % (stbl, cond)
    return sqlq

def q_copy(stbl, tag1, tag2):
    with connections[XDBX].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
        acols = ','.join(cn[0] for cn in cursor)
        bcols = "'%s',%s" % (tag2, ','.join(cn[0] for cn in cursor if cn[0] != 'gtag'))
        sqlq = "INSERT INTO %s (%s) SELECT %s FROM %s WHERE gtag='%s'" % (stbl, acols, bcols, stbl, tag1)
        cursor.execute(sqlq)
    sqlq = "SELECT * FROM %s WHERE gtag='%s'" % (stbl, tag2)
    
    return sqlq
    

def q_compare(stbl, tag1, tag2):
    with connections[XDBX].cursor() as cursor:
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
    with connections[XDBX].cursor() as cursor:
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
    with connections[XDBX].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % stbl)
    sqlc = q_comp(stbl,tag1,tag2)
    cps = "ckey_0,%s,act" % ','.join(cn[0][2:] for cn in cursor if cn[0][:2] == 'c_')
    sql1 = "SELECT %s,\nconcat(%s) mml\nFROM (%s) v3\nWHERE act = 'MODIFY'" % (cps,q_mstr(mm,cps), sqlc)
    sql2 = "SELECT %s,\nif(act='CREATE', concat(%s), concat(%s)) mml\nFROM (%s) v3\n" \
           "WHERE act = 'CREATE' or act = 'DELETE'" % (cps, q_mstr(mc,cps), q_mstr(md,cps), sqlc) 
    sqlq = "%s UNION\n%s" % (sql2, sql1)
    
    return sqlq

def q_comps(stbl,ck,cd,rtag,ctags):
    with connections[XDBX].cursor() as cursor:
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
    
def q_subana(stb1,atyp,maxd=8,flds=['*'], gtag=''):
    stb2 = 'mss_attr_val'
    stb3 = 'mss_attr_st'
    atp = 'aif' if atyp in ['MTC', 'MOC'] else atyp 
    rtbl = 'mss_ares_%s' % atp.lower()
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
    with connections[XDBX].cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM %s" % rtbl)
        cols = ','.join('v2.%s' % cn[0] for cn in cursor if cn[0] not in ['gtag','resn','rtyp'])
    
    sql1 = v1
    d = 2
    while d < maxd:
        sql1 = 'SELECT v1.*, v2.attr attr%(n)s, v2.val val%(n)s, v2.ana ana%(m)s FROM (%(q)s) v1\n\
join (%(v2)s) v2 on v1.ana%(n)s = v2.subname' % {"n":d, "m":d+1, "q":sql1, "v2":v2}
        df = pd.read_sql_query(sql1, connections[XDBX])
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
    