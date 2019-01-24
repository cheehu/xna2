import pathlib
#mm = "ZNRB:",ckey,":",concat(IF(para="","",concat("PARA=",para)),IF(lsh="","",concat("LOAD=",lsh)),IF(aopc="","",concat("AOPC=",aopc)))
#flds = 'ckey_0,para_1,load_1,rou_2,prio_2' 
#mc = '"ZNRB:" 0 ",:" 1* ";"'

spath = 'D:/xna/SourceData/DTG/MSS/MSSFFM01/'
sfs = 'abc.tsv'

tfile = spath + 'abc.tsv'

sdir = pathlib.Path(spath)
sfi = sdir / sfs
print(sfi)
#sfiles = list(sdir.glob(sfs))
#tf = str(pathlib.PurePosixPath(sfi))
tf1 = "'%s'" % ('/'.join(fn for fn in tfile.split('\\')))
sqlq = "LOAD DATA LOCAL INFILE %s INTO TABLE table" % (tf1)
print (sqlq)

#print(flds[0][-1])

             

