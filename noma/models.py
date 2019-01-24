from django.db import models
from django.urls import reverse # Used to generate URLs by reversing the URL patterns
from django.conf import settings

class NomaGrp(models.Model):
    name = models.CharField('Group Name', unique=True, max_length=50)
    desc = models.CharField('Description', max_length=200, blank=True, null=True)
    sdir = models.FilePathField('Source File Folder', path=settings.GRP_DIR, recursive=True, allow_files=False, allow_folders=True, max_length=200)
    ldir = models.FilePathField('Output File Folder', path=settings.LOG_DIR, recursive=True, allow_files=False, allow_folders=True, max_length=200)
    gtag = models.CharField('Tag', max_length=100, blank=True, null=True)

    class Meta:
        db_table = 'noma_grp'
        verbose_name_plural = "  [O] Import Groups"
    
    def __str__(self):
        return self.name

class NomaSet(models.Model):
    name = models.CharField(unique=True, max_length=50)
    desc = models.CharField('Description', max_length=200, blank=True, null=True)
    stypes = (
        ('tx', 'Text/XML'),
        ('js', 'JSON'),
        ('bi', 'Binary'),
        ('p2', 'PCAP2pass'),
    )
    type = models.CharField(max_length=2, choices=stypes, default='tx')
    sepr = models.CharField(max_length=100, blank=True, null=True)
    eepr = models.CharField(max_length=100, blank=True, null=True)
    depr = models.CharField(max_length=100, blank=True, null=True)

    class Meta:    
        db_table = 'noma_set'
        verbose_name_plural = " [K] Noma Sets"
        
    def __str__(self):
        return self.name
    
class NomaGrpSet(models.Model):
    grp = models.ForeignKey(NomaGrp,
                            related_name='sets',
                            on_delete=models.CASCADE)
    set = models.ForeignKey(NomaSet,
                            related_name='grps',
                            on_delete=models.CASCADE)
    sfile = models.CharField(max_length=100)
    ttbl = models.CharField(max_length=30)
    ocsv = models.BooleanField(default=True)

    class Meta:
        db_table = 'noma_grpset'
        unique_together = (('grp', 'set'),)
        verbose_name = "Noma Set"
        verbose_name_plural = " Noma Sets"
    
    def __str__(self):
       return str(self.grp) + ' - ' + str(self.set)
     
       
       
class NomaFunc(models.Model):
    epr = models.CharField(unique=True, max_length=50)
    pars = models.CharField(max_length=200, blank=True, null=True)
    desc = models.CharField(max_length=200, blank=True, null=True)
    
    class Meta:
        db_table = 'noma_func'
        verbose_name_plural = '[A] T-Functions'
        
    def __str__(self):
        return self.epr
	   

class NomaSetAct(models.Model):
    set = models.ForeignKey(NomaSet,
                            related_name='acts',
                            on_delete=models.CASCADE)
    seq = models.DecimalField(max_digits=3, decimal_places=1)
    spos = models.SmallIntegerField(blank=True, null=True)
    epos = models.SmallIntegerField(blank=True, null=True)
    sepr = models.CharField(max_length=100, blank=True, null=True)
    eepr = models.CharField(max_length=100, blank=True, null=True)
    tfunc = models.ForeignKey(NomaFunc, on_delete=models.CASCADE, blank=True, null=True)
    nepr = models.CharField(max_length=100, blank=True, null=True)
    fepr = models.CharField(max_length=100, blank=True, null=True)
    fname = models.CharField(max_length=50, blank=True, null=True)
    skipf = models.SmallIntegerField(default=0)
    skipb = models.SmallIntegerField(default=0)
    varr = models.SmallIntegerField(blank=True, null=True)
    fchar = models.CharField(max_length=100, blank=True, null=True)
    
    class Meta:
        db_table = 'noma_setact'
        unique_together = (('set', 'seq'),)
        ordering = ['seq']

    def __str__(self):
        return str(self.set) + ' - ' + str(self.seq)

class queGrp(models.Model):
    name = models.CharField('Group Name', unique=True, max_length=50)
    ldir = models.FilePathField('Output Folder', path=settings.LOG_DIR, recursive=True, allow_files=False, allow_folders=True, max_length=200)
    tfile = models.CharField(max_length=100)
    gpar = models.CharField(max_length=200, blank=True, default='')
    desc = models.CharField(max_length=200, blank=True, default='')
    
    class Meta:
        db_table = 'que_grp'
        verbose_name_plural = "  [O] Query Groups"
    
    def __str__(self):
        return self.name

class queSet(models.Model):
    name = models.CharField(unique=True, max_length=50)
    desc = models.CharField('Description', max_length=200, blank=True, default='')
    
    class Meta:    
        db_table = 'que_set'
        verbose_name_plural = " [K] Query Sets"
        
    def __str__(self):
        return self.name

class queGrpSet(models.Model):
    grp = models.ForeignKey(queGrp,
                            related_name='sets',
                            on_delete=models.CASCADE)
    set = models.ForeignKey(queSet,
                            related_name='grps',
                            on_delete=models.CASCADE)
    
    spar = models.CharField(max_length=200, blank=True, default='')
    
    class Meta:
        db_table = 'que_grpset'
        unique_together = (('grp', 'set'),)
        verbose_name = "Query Set"
        verbose_name_plural = "Query Sets"
    
    def __str__(self):
       return str(self.grp) + ' - ' + str(self.set)

class NomaQFunc(models.Model):
    epr = models.CharField(unique=True, max_length=50)
    pars = models.CharField(max_length=200, blank=True, null=True)
    desc = models.CharField(max_length=200, blank=True, default='')
    
    class Meta:
        db_table = 'noma_qfunc'
        verbose_name_plural = '[A] Q-Functions'
        
    def __str__(self):
        return self.epr
               
class queSetSql(models.Model):
    set = models.ForeignKey(queSet,
                            related_name='Sqls',
                            on_delete=models.CASCADE)
    seq = models.DecimalField(max_digits=3, decimal_places=0)
    name = models.CharField(max_length=50)
    qfunc = models.ForeignKey(NomaQFunc, on_delete=models.CASCADE, blank=True, null=True)
    stbl = models.CharField(max_length=30)
    qpar = models.CharField(max_length=500, blank=True, default='')
    
    class Meta:
        db_table = 'que_setsql'
        unique_together = (('set', 'seq'),)
        ordering = ['seq']

    def __str__(self):
        return str(self.set) + ' - ' + str(self.seq)

class NomaStrMap(models.Model):
    ctag = models.CharField(max_length=20,default='default')
    ostr = models.CharField(max_length=50)
    cstr = models.CharField(max_length=50, blank=True, null=True)
    desc = models.CharField(max_length=200, blank=True, null=True)
    
    class Meta:
        db_table = 'noma_strmap'
        unique_together = (('ctag', 'ostr'),)
        verbose_name_plural = ' [K] Strings Maps'
        
    def __str__(self):
        return self.ostr


        