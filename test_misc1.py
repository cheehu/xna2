#mm = "ZNRB:",ckey,":",concat(IF(para="","",concat("PARA=",para)),IF(lsh="","",concat("LOAD=",lsh)),IF(aopc="","",concat("AOPC=",aopc)))

flds = 'ckey_0,para_1,load_1,rou_2,prio_2' 

mc = '"ZNRB:" 0 ",:" 1* ";"'


def q_mstr(ma, fd):
    fa = fd.split(',')
    ml = []
    va = ma.split(' ')
    for p in va:
        if p[0].isdigit():
            if p[-1] == '*': 
                ml.append("mid(concat(%s)" % ','.join('IF(%s="","",concat(",%s=",%s))' % (f,f[:-2],f) for f in fa if f[-1] == p[0]))
            else: ml.append(','.join(f for f in fa if f[-1] == p[0]))
        else: ml.append(p)
    return ','.join(m for m in ml)

print(q_mstr(mc, flds))

#print(flds[0][-1])

             

