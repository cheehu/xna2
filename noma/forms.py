from django import forms
from django.db import connections
from .models import NomaSetAct, NomaGrp

def get_dbtbs():
    with connections['xnaxdr'].cursor() as cursor:
        cursor.execute('SHOW TABLES')
    tbls = []
    for (tb,) in cursor: tbls.append((tb,tb))
    return tuple(tbls)

class NomaSetActForm(forms.ModelForm):
    class Meta:
        widgets = { 'seq': forms.NumberInput(attrs={'style': 'width:6ch'}),
                    'nepr': forms.TextInput(attrs={'size': 26}),
                    'sepr': forms.TextInput(attrs={'size': 26}),
                    'eepr': forms.TextInput(attrs={'size': 26}),
                    'spos': forms.NumberInput(attrs={'style': 'width:5ch'}),                    
                    'epos': forms.NumberInput(attrs={'style': 'width:5ch'}),
                    'fepr': forms.TextInput(attrs={'size': 20}),
                    'skipf': forms.NumberInput(attrs={'style': 'width:5ch'}), 
                    'skipb': forms.NumberInput(attrs={'style': 'width:5ch'}),
                    'fname': forms.TextInput(attrs={'size': 10}),
                    'varr': forms.NumberInput(attrs={'style': 'width:5ch'}),
                    #'tfunc': forms.Select(attrs={'size': 15})
                    
				 }        
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['tfunc'].widget.can_add_related = False
        self.fields['tfunc'].widget.can_change_related = False
        self.fields['eepr'].strip = False

        
    
class NomaGrpForm(forms.ModelForm):
    class Meta:
        widgets = { 'name': forms.TextInput(attrs={'size': 30}),
                    'desc': forms.TextInput(attrs={'size': 80}),
                    'sdir': forms.Select(attrs={'style': 'width:30ch'}),
                    'ldir': forms.Select(attrs={'style': 'width:30ch'}) 
                
                 } 

class NomaGrpSetForm(forms.ModelForm):
        
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['ttbl'] = forms.ChoiceField(choices=get_dbtbs())
                 
class queGrpForm(forms.ModelForm):
    class Meta:
        widgets = { 'name': forms.TextInput(attrs={'size': 30}),
                    'desc': forms.TextInput(attrs={'size': 60}),
                    'ldir': forms.Select(attrs={'style': 'width:30ch'}),
                    'tfile': forms.TextInput(attrs={'size': 30}) 
                
                 } 
class queSetSqlForm(forms.ModelForm):
    class Meta:
        widgets = { 'seq': forms.NumberInput(attrs={'style': 'width:7ch'}),
                    'name': forms.TextInput(attrs={'size': 20}),
                    'qpar': forms.TextInput(attrs={'size': 120}),
        		 }
                    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['stbl'] = forms.ChoiceField(choices=get_dbtbs())
                 
                 
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