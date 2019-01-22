import json
from django.contrib import admin
from .models import NomaGrp, NomaSet, NomaGrpSet, NomaFunc, NomaQFunc, NomaStrMap, NomaSetAct, queGrp, queSet, queGrpSet, queSetSql
from .forms import NomaSetActForm, NomaGrpForm, NomaGrpSetForm, NomaExecForm, queSetSqlForm, queGrpForm, queExecForm
from .utils import nomaInfo, nomaCount, queInfo, queCount
from django.contrib.admin import AdminSite
from django.shortcuts import redirect, render
from django.urls import path
from .tasks import nomaExec, nomaQExe
from django.http import HttpResponse


class NomaAdminSite(AdminSite):
    site_header = "NOMA Portal"
    site_title = "XNA"
    index_title = "Welcome to NOMA"
    site_url = ""

noma_admin_site = NomaAdminSite(name='noma_portal')

class NomaFuncAdmin(admin.ModelAdmin):
    list_display = ('epr', 'pars', 'desc')
    save_as = True

class NomaStrMapAdmin(admin.ModelAdmin):
    list_display = ('ostr', 'ctag', 'cstr', 'desc')
    save_as = True

class NomaQFuncAdmin(admin.ModelAdmin):
    list_display = ('epr', 'pars', 'desc')
    save_as = True

class NomaGrpSetInline(admin.TabularInline):
    model = NomaGrpSet
    form = NomaGrpSetForm
    extra = 0
    class Media:
        css = { "all" : ("css/hide_admin_original.css",) }
        
class NomaSetActInline(admin.TabularInline):
    model = NomaSetAct
    form = NomaSetActForm
    exclude = ['fchar']
    extra = 0
    class Media:
        css = { "all" : ("css/hide_admin_original.css",) }

class NomaGrpAdmin(admin.ModelAdmin):
    form = NomaGrpForm
    list_display = ('name', 'desc', 'sdir', 'ldir')
    fields = [('name', 'sdir', 'ldir'), ('gtag','desc')]
    inlines = [NomaGrpSetInline]
    save_as = True
    
    class Media:
        css = { "all" : ("css/nomabase.css",) }
    
    def get_urls(self):
        urls = super().get_urls()
        my_urls = [
            path('<pk>/exec_view/', self.admin_site.admin_view(self.exec_view), name="exec_view")
        ]
        return my_urls + urls
    
    def exec_view(self, request, pk):
        total_count = nomaCount(NomaGrp,pk,NomaSet)
        if request.method == 'POST':
            form = NomaExecForm(request.POST)
            task = nomaExec.delay(pk,total_count)
            return HttpResponse(json.dumps({'task_id': task.id}), content_type='application/json')
        else:
            log_content = nomaInfo(NomaGrp,pk,NomaSet)
            form = NomaExecForm(initial={'log_content': log_content, 'total_count': total_count})
        context = {'form': form} 
        return render(request, 'noma/progress.html', context)


class NomaSetAdmin(admin.ModelAdmin):
    list_display = ('name', 'desc', 'type')
    fields = [('name', 'desc', 'type'), 
              ('sepr', 'eepr', 'depr')]
    inlines = [NomaSetActInline]
    save_as = True
    
class queGrpSetInline(admin.TabularInline):
    model = queGrpSet
    extra = 0
    class Media:
        css = { "all" : ("css/hide_admin_original.css",) } 

class queSetSqlInline(admin.TabularInline):
    model = queSetSql
    form = queSetSqlForm
    extra = 0
    class Media:
        css = { "all" : ("css/hide_admin_original.css",) }

class queGrpAdmin(admin.ModelAdmin):
    form = queGrpForm
    list_display = ('name', 'desc', 'ldir', 'tfile')
    fields = [('name', 'ldir', 'tfile'), ('gpar','desc')]
    inlines = [queGrpSetInline]
    save_as = True
    
    class Media:
        css = { "all" : ("css/nomabase.css",) }
    
    def get_urls(self):
        urls = super().get_urls()
        my_urls = [
            path('<pk>/que_view/', self.admin_site.admin_view(self.que_view), name="que_view")
        ]
        return my_urls + urls
    
    def que_view(self, request, pk):
        total_count = queCount(queGrp,pk,queSet)
        if request.method == 'POST':
            form = queExecForm(request.POST)
            task = nomaQExe.delay(pk,total_count)
            return HttpResponse(json.dumps({'task_id': task.id}), content_type='application/json')
        else:
            log_content = queInfo(queGrp,pk,queSet)
            form = queExecForm(initial={'log_content': log_content, 'total_count': total_count})
        context = {'form': form} 
        return render(request, 'noma/progress.html', context)
    
    

class queSetAdmin(admin.ModelAdmin):
    list_display = ('name', 'desc')
    fields = [('name', 'desc')]
    inlines = [queSetSqlInline]
    save_as = True
 
noma_admin_site.register(NomaFunc, NomaFuncAdmin)  
noma_admin_site.register(NomaStrMap, NomaStrMapAdmin)     
noma_admin_site.register(NomaGrp, NomaGrpAdmin)
noma_admin_site.register(NomaSet, NomaSetAdmin)
noma_admin_site.register(queGrp, queGrpAdmin)
noma_admin_site.register(queSet, queSetAdmin)
noma_admin_site.register(NomaQFunc, NomaQFuncAdmin)