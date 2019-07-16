from django import forms
from django.db import connections
from .models import NomaSetAct, NomaGrp
from django.conf import settings
from django.forms import widgets
import re, pathlib

BDIR = pathlib.Path(settings.GRP_DIR) / settings.LOG_DIR / 'uploads'
ODIR = pathlib.Path(settings.GRP_DIR) / settings.LOG_DIR / 'downloads'
XDBX = 'xnaxdr'

def get_dbtbs(ptt):
    with connections[XDBX].cursor() as cursor:
        cursor.execute('SHOW TABLES')
    tbls = [(tb[0],tb[0]) for tb in cursor if re.search(ptt,tb[0])]
    tbls.append(('-----','-----'))
    return tuple(tbls)

def get_dirs(spath,fx):
    sdir = pathlib.Path(spath)
    p = sdir.glob('**/*')
    dirs = [(x.relative_to(sdir),x.relative_to(sdir)) for x in p if x.is_dir() or x.suffix.lower() == fx]
    return tuple(dirs)
    

class NomaSetActForm(forms.ModelForm):
    class Meta:
        widgets = { 'seq': forms.NumberInput(attrs={'style': 'width:6ch'}),
                    'nepr': forms.TextInput(attrs={'size': 24}),
                    'sepr': forms.TextInput(attrs={'size': 24}),
                    'eepr': forms.TextInput(attrs={'size': 24}),
                    'spos': forms.NumberInput(attrs={'style': 'width:4ch'}),                    
                    'epos': forms.NumberInput(attrs={'style': 'width:4ch'}),
                    'fepr': forms.TextInput(attrs={'size': 18}),
                    'skipf': forms.NumberInput(attrs={'style': 'width:4ch'}), 
                    'skipb': forms.NumberInput(attrs={'style': 'width:4ch'}),
                    'fname': forms.TextInput(attrs={'size': 10}),
                    'varr': forms.NumberInput(attrs={'style': 'width:3ch'}),
                    'tfunc': forms.Select(attrs={'style': 'width:18ch'}),
                    'xtag': forms.TextInput(attrs={'size': 22})
                    
				 }        
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['tfunc'].widget.can_add_related = False
        self.fields['tfunc'].widget.can_change_related = False
        self.fields['eepr'].strip = False

class NomaSetForm(forms.ModelForm):
    class Meta:
        widgets = { 'name': forms.TextInput(attrs={'size': 25}),
                    'desc': forms.TextInput(attrs={'size': 60}),
                    'type': forms.Select(attrs={'style': 'width:16ch'}),
                    'sepr': forms.TextInput(attrs={'size': 60}),
                    'eepr': forms.TextInput(attrs={'size': 60}),
                    'depr': forms.TextInput(attrs={'size': 60}),
                    'xtag': forms.TextInput(attrs={'size': 60}),
                 } 
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    
class NomaGrpForm(forms.ModelForm):
    class Meta:
        widgets = { 'name': forms.TextInput(attrs={'size': 30}),
                    'desc': forms.TextInput(attrs={'size': 80}),
                    'sdir': forms.TextInput(attrs={'size': 60}),
                    'sfile': forms.TextInput(attrs={'size': 60}),
                 } 
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['sdir'] = forms.ChoiceField(label='Source Folder',
                                                choices=get_dirs(BDIR,'.zip'),
                                                widget=widgets.Select(attrs={'style': 'width:50ch'}))
        self.fields['ldir'] = forms.ChoiceField(label='Log Folder',
                                                choices=get_dirs(ODIR,None),
                                                widget=widgets.Select(attrs={'style': 'width:50ch'}))
        
        
class NomaGrpSetForm(forms.ModelForm):
            
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['ttbl'] = forms.ChoiceField(choices=get_dbtbs('^(?!v_)'))
        
                 
class queGrpForm(forms.ModelForm):
    class Meta:
        widgets = { 'name': forms.TextInput(attrs={'size': 30}),
                    'desc': forms.TextInput(attrs={'size': 80}),
                    'tfile': forms.TextInput(attrs={'size': 50}),
                    'gpar': forms.TextInput(attrs={'size': 80})                    
                 }
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['ldir'] = forms.ChoiceField(label='Output Folder',
                                                choices=get_dirs(ODIR,None),
                                                widget=widgets.Select(attrs={'style': 'width:50ch'}))
        
class queSetSqlForm(forms.ModelForm):
    class Meta:
        widgets = { 'seq': forms.NumberInput(attrs={'style': 'width:7ch'}),
                    'name': forms.TextInput(attrs={'size': 20}),
                    'qpar': forms.TextInput(attrs={'size': 100}),
        		 }
                    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['stbl'] = forms.ChoiceField(choices=get_dbtbs('.+'))
        
        
class NomaStrMapForm(forms.ModelForm):
    class Meta:
        widgets = { 'ostr': forms.TextInput(attrs={'size': 70}),
                    'cstr': forms.TextInput(attrs={'size': 70}),
                    'desc': forms.TextInput(attrs={'size': 70}),
        		 }
                 
                 
class NomaExecForm(forms.Form):
    log_content = forms.CharField(
        label='Noma Group Details',
        disabled=True,
        widget=forms.Textarea(attrs={'rows': 15, 'cols': 180})
    )
    total_count = forms.IntegerField(
        label='Total Noma Tasks',
        disabled=True
    )        

class queExecForm(forms.Form):
    log_content = forms.CharField(
        label='Query Group Details',
        disabled=True,
        widget=forms.Textarea(attrs={'rows': 15, 'cols': 180})
    )
    total_count = forms.IntegerField(
        label='Total Query Tasks',
        disabled=True
    )      