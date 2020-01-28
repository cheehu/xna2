import os
import re, mmap, pandas as pd
#import xml.etree.ElementTree as ET
import pathlib

#ldir = pathlib.Path('D:/XNA/data/nomasftp/downloads/CMCC')
#sfile = ldir / 'pcap_base_test_vlan_m3.pcap'

#BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

a = 'abcd'
print(a[0:0])

        

#print('%s' % tfile)

#s = ['default', 'xnaxdr', 'nomasftp']
#print(s[2])
#n = []
#s = ['nomadb'+n[0], 'xnaxdr'+n[0], 'nomasftp'+n[0]]
#print(s[2])



#d = {p.split('=')[0][2:]: p.split('=')[1] for p in s.split(',')}

#for k, v in d.items():
#    if k[:6] == 'xmlns:': print('"%s","%s"' % (k, v))



#a = r'E\x00\x05\xc8\x00\x17\x00\x00\xff2\x19\x13'
#c = eval(a)
#c = codecs.decode(a, 'unicode_escape')
#c = codecs.decode(a)
#b = re.sub(r'\','v',a)
#b = bytes(c,'cp65001')
#c = a + b
#b = bytes(a, 'iso_8859-1')
#ipd.append(b[1])
#b = str(a, 'utf-8')
#print(b)
    


#E\x00\x05\xdc\x00\x12\x00\x00\xff\x06\x190\n\xc85\xc3\n\xc8Q\x87}e\x13\xc4\xae\xf0\x00\x01\xd0\xa1\x91


'''

ldir = pathlib.Path('D:/XNA/data/nomasftp/downloads/CMCC')
sfile = ldir / 'CMCC_dash.xlsx'

sd = pd.read_excel(sfile, sheet_name=None, header=None)

#hd = {k: sd[k].values.tolist() for k in sd}
#stbl = 'pvt_sip_cat_apps'

#df = sd['pvt_sip_cat_asg']
#dfl = df.values.tolist()
hd = sd['Index'][[1,4]].loc[3:].values.tolist()
dini = {k[0].strip(): k[1].split('%') for k in hd}
sd.pop('Index',None)
#for k in sd: 
print(dini['pvt_sip_cat_apps'][1].split('&'))
#print(dini)

ldir = pathlib.Path('D:/XNA/data/nomasftp/downloads/CMCC')
sfile = ldir / 'CMCC_dash.xlsx'

sd = pd.read_excel(sfile, sheet_name=None, header=None)
sd.pop('Index',None)
dd = [{'label': k, 'value': k} for k in sd]
hd = {k: sd[k].values.tolist() for k in sd}
stbl = 'pvt_sip_cat_apps'

#df = sd['pvt_sip_cat_asg']
#dfl = df.values.tolist()
print(hd[stbl][0])

'''







'''
s = '__xmlns:fw=http://abd,__xmlns:cli=http://ntas,__xroot=config'
xr = re.search('__xroot=(\S+)',s)
print(xr[1])

def xtbl(elm, xp):
    ta, tb = [], False
    ptag = etag(elm.tag)
    xph = '/'.join(p for p in xp[1:])
    xp.append(ptag)
    for e in elm:
        if len(e) > 1:
            xphc = '/'.join(p for p in xp[1:])
            if e[0].tag == e[1].tag: ta.append('%s,%s' % (xphc,etag(e.tag)))
            else: ta += xtbl(e,xp)
        elif len(e) == 1: ta += xtbl(e,xp)
        else: tb = True
    if tb: ta.append('%s,%s' % (xph,ptag))
    xp.remove(ptag)
    return ta
    

sf = 'D:/XNA/logs/tas/ntas_01.xlsx'
sd = pd.read_excel(sf, sheet_name=None)
for cn, cv in enumerate(sd['ntas_interfaces'].columns.values):
    print(cn)
    

nepr = r's:,:|'
nepr = nepr.split(":")
#c = '11|22|33'
c = r'"aaa","bbb","ccc"|"ddd","eee","fff"|"ggg","hhh","iii"'
c = [r.split(nepr[1]) for r in c.split(nepr[2])]
#tbl = [r.split(',') for r in a.split('|')]
df = pd.DataFrame(c)
print(df)

hv = '450005dc00120000ff0619300ac835c30ac851877d6513c4aef00001d0a191125018b400e4'
bv = bytes.fromhex(hv)
print(int.from_bytes(bv[2:4],byteorder='big'))

def lookup_c(st, sf, kc, vc):
    if sf not in svar: 
        with open(ldir / sf) as s:
            print("open sf")
            cf = csv.reader(s, delimiter='\t')
            svar[sf] = {}
            for row in cf: svar[sf].update({row[kc] : row[vc]})
    return svar[sf][st]

sf = 'mss_lacs.tsv'
svar = {}
st = '1473'

a = lookup_c(st, sf, 2, 1)
print(a)
st = '5008'
a = lookup_c(st, sf, 2, 1)
print(a)


c0 = '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30'
c1 = '2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,5,5,5,5,5,5,5,5,5,5'


def num_th(val,th):
    val = re.sub(",",".",val)
    cv = str(int(float(val) * th))
    return cv

x = '00012'    
print(num_th(x,1))


sf = 'D:/xna/SourceData/DTG/TMSSBN16_Common_DB_v0.1.hck.xlsx'
tf = 'D:/xna/SourceData/DTG/XLS/xlrd_test.csv'
sepr = 'CCBS Parameters in MSC' 
#xl = None
xl = pd.ExcelFile(sf)
print(xl.book) 
 
#st = time.time()    
#csv_from_excel(sf,tf,xl)
#print(time.time() - st)

def csv_from_excel(sf,tf,xl):
    sepr = 'CCBS Parameters in MSC'  
    df = xl.parse(sepr,header=None,skiprows=10,usecols='B:J',dtype=str)
    print(df)
    #print(xl.filename)
    sepr = 'Zone Code Definition'  
    #df = xl.parse(sepr,header=None,skiprows=10,usecols='B:F',dtype=str)
    
    
    return
    

if isinstance(xl, pd.ExcelFile): 
        if sepr not in xl.sheet_names: xl = pd.ExcelFile(sf)
    else:
        xl = pd.ExcelFile(sf)
        

def csv_from_excel(sf,tf,sepr):
    wb = xlrd.open_workbook(sf)
    sh = wb.sheet_by_name(sepr)
    csv_file = open(tf, 'w')
    wr = csv.writer(csv_file, delimiter = ",")
    for rownum in range(10,sh.nrows):
        row = [cell.value for cell in sh.row_slice(rownum,1, 10)]
        wr.writerow(row)
    csv_file.close()
    
sf = 'D:/xna/SourceData/DTG/TMSSBN16_Common_DB_v0.1.hck.xlsx'
eepr = ['C:H',None]

with open(sf, 'rUb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
    sepr = ['CCBS Parameters in MSC',None,10,None]
    st = time.time()
    df = pd.read_excel(s,sepr[0],header=sepr[1],skiprows=10,usecols=eepr[0],dtype=str)
    print(time.time() - st)
    print(df) 
    
    sepr = ['CDR Saving Parameters',None,10,None]
    st = time.time()
    df = pd.read_excel(s,sepr[0],header=sepr[1],skiprows=10,usecols=eepr[0],dtype=str)
    print(time.time() - st)
    print(df) 
    
    
#for row,rec in df.iterrows():
#    print('%s %s' % (row, rec[1]))


sf = 'D:/xna/SourceData/DTG/TMSSBN16_Common_DB_v0.1.hck.xlsx'
tf = 'D:/xna/SourceData/DTG/XLS/xlrd_test.csv'
sepr = 'CCBS Parameters in MSC'    
st = time.time()
csv_from_excel(sf,tf,sepr)
print(time.time() - st)

def nx_line(va, sepr,n,l):
    stx = '^(.*?)%s(.*?\n){%s}' % (sepr, n)
    a = re.search(stx, va)
    if a != None:
        sp = a.end() + len(a.group(1))
        ep = sp + l
        a = va[sp:ep]
    return '' if a == None else a
    
c0 = '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30\n\
2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,5,5,5,5,5,5,5,5,5,5\n\
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30'
stx = '22'
n, l = 1, 3

print(nx_line(c0,stx,n,l))


st = r'SIP/2.0 200 OK\r\nRecord-Route:<sip:pcscftb100-Mw.ims.mnc019.mcc404.3gppnetwork.org:5070;routing_id=pcscf_b_side;lskpmc=PKA;lr>\r\nRecord-Route:<sip:isc@scscftb100-Mw.ims.mnc019.mcc404.3gppnetwork.org:5090;lr;interface=bcc;lskpmc=SKA;routing_id=c3517a005ee3a581d320b1d12ebd2335;transport=SCTP>\r\nv:SIP/2.0/SCTP 10.208.54.58:5060;branch=z9hG4bKfjWd4gagVVUAW799;yop=00.00.5AA92988.0000.7002\r\nf:<sip:+917288900140@ims.mnc007.mcc404.3gppnetwork.org;user=phone>;tag=4CC0D5b6h0W2Zh9.\r\nt:<sip:+917036005978@ims.mnc007.mcc404.3gppnetwork.org;user=phone>;tag=iHVBWnotDk\r\ni:99D7F5255BE1581180F739BA@0270ffffffff\r\nCSeq:2 PRACK\r\nAllow:ACK,BYE,CANCEL,INVITE,MESSAGE,NOTIFY,OPTIONS,PRACK,REFER,UPDATE\r\nk:100rel,path,replaces\r\nUser-Agent:iOS/9.2 (13C75) iPhone\r\nP-Access-Network-Info:3GPP-E-UTRAN-FDD;local-time-zone="2016-07-19T13:04:17+05:30";utran-cell-id-3gpp=404070fa11397804\r\nl:0\r\nP-Charging-Vector:icid-value=1ed7b6e17aea86ecc40299c05c15f425\r\n\r\n'
#st = r'P-Charging-Vector: icid-value=31a3488513c87ebf9a8978f35b123eb5\r\nP-NokiaSiemens.QoSInfo: MaxReqBw-DL=31500;MaxReqBw-UL=31500\r\nP-Charging-Function-A'
sb = noma.tfunc.to_bytes(st)
st = sb.decode()
print(st)
#m = re.search('icid-value=', st)
#fs = m.end()
#m = re.search(';|\n', st[fs:])
#fe = fs + m.start()
#hdr = st[fs:fe].strip()
#print(noma.tfunc.siphdr(st,'icid-value=',';|\n'))

def etag(xtag):
    ta = xtag.split('}')
    return '%s:%s' % (mynr[ta[0][1:]],ta[1])

def xtbl(elm, xp):
    ta, tb, pe, pa = [], False, elm, []
    ptag = etag(elm.tag)
    xph = '/'.join(p for p in xp[1:])
    xp.append(ptag)
    for e in elm:
        if e.tag == pe.tag: continue 
        if len(e) > 0: ta += xtbl(e,xp)
        else: 
            pa.append(etag(e.tag))
            tb = True
        pe = e
    if tb: 
        pr = ','.join(p for p in pa)
        ta.append('%s,%s\t%s' % (xph,ptag,pr))
    xp.remove(ptag)
    return ta

def xml_noma(ta,tf):
    f=open(tf, "w+")
    for t in ta:
        tb = t.split('\t')
        f.write("tname='%s'\n" % tb[0])
        spar = tb[0].split('/')
        epr = spar[-1].split(',')
        f.write("sepr=r'<%s>'\n" % epr[-1])
        f.write("eepr=r'</%s>'\n" % epr[0])
        f.write("depr=r'<%s>'\n" % epr[-1])
        f.write("xtag=r't,%s,%s'\n" % ('/'.join(spar[:-1]),spar[-1]))
        f.write("cs = NomaSet(name=tname, type=ttype, sepr=sepr, eepr=eepr, depr=depr,xtag=xtag)\n\
if savenoma == 1: cs.save()\n\
acts = []\n")
        i = 0
        for p in tb[1].split(','):
            i += 1
            f.write("acts.append(sa(set=cs,seq=%(i)s,spos=0,sepr=r'%(p)s>',eepr=r'</',fname='f%(i)s',xtag=r'v,%(p)s',fchar=r'VARCHAR(10)'))\n" % {"i":i, "p":p})
        f.write('\n')
    f.close()
    print('done')
    
sf = 'D:/XNA/data/xnaxdr/uploads/nTAS/TAS18.xml'
tf = 'D:/XNA/data/xnaxdr/downloads/nTAS/ntas_noma_1.txt'
myns = dict([node for _, node in ET.iterparse(sf, events=['start-ns'])])
mynr = dict([(node[1],node[0]) for _, node in ET.iterparse(sf, events=['start-ns'])])
tree = ET.parse(sf)  
root = tree.getroot()    
    
xtb = xtbl(root,[])
xml_noma(xtb,tf)


'''    





             

