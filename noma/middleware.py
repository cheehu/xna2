import threading

_thread_locals = threading.local()

class TenantMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        _thread_locals.request = request
        response = self.get_response(request)
        return response

def get_current_request():
    return getattr(_thread_locals, "request", None)

def get_current_user():
    request = get_current_request()
    if request: return getattr(request, "user", None)

def get_current_ngrp():
    ngrp = []
    user = get_current_user()
    if user: 
        grps = user.groups.values_list('name',flat=True)
        n = [g[-1] for g in grps if '_ng_' in g]
        ngrp = ['nomadb'+n[0], 'xnaxdr'+n[0], 'nomasftp'+n[0]] if n else ['default', 'xnaxdr', 'nomasftp'] 
    return ngrp
    

def get_current_db_name():
    ng = get_current_ngrp()
    nd = ng[0] if ng else getattr(_thread_locals, "DB", None)
    return nd

def set_db_for_router(db):
    setattr(_thread_locals, "DB", db)

def set_ds_for_dash(ds):
    setattr(_thread_locals, "DS", ds)

def get_current_dataset():
    return getattr(_thread_locals, "DS", None)
    

class TenantRouter:
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'noma': return get_current_db_name()
        return 'default'
    
    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'noma': return get_current_db_name()
        return 'default'
        

'''

def get_current_usrgrp(defdb):
    ugrp = defdb
    stt = 'noma' if defdb == 'default' else 'xnax'
    user = get_current_user()
    if user: 
        grps = user.groups.values_list('name',flat=True)
        dbg = [g for g in grps if g[:4] == stt] 
        if dbg: ugrp = dbg[0]
    setattr(_thread_locals, "DB", ugrp)
    return ugrp
    

class DataBaseRouter(object):
    
    def db_for_read(self, model, **hints):
        ug =  _thread_locals.ugrp
        #if rq: usr = rq.user
        #if usr: a = usr.username
        #db = get_current_usrgrp('default')
        #if db != None: return db
        return 'default'

    def db_for_write(self, model, **hints):
        db = get_current_usrgrp('default')
        #if db != None: return db
        return 'default'
        
        

import threading
from uuid import uuid4

from django.db import connections

THREAD_LOCAL = threading.local()


class TenantMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        setattr(THREAD_LOCAL, "request", request)
        response = self.get_response(request)
        return response

def get_current_request():
    return getattr(THREAD_LOCAL, "request", None)

def get_current_user():
    request = get_current_request()
    if request: return getattr(request, "user", None)

def get_current_usrgrp(defdb):
    ugrp = defdb
    stt = 'noma' if defdb == 'default' else 'xnax'
    user = get_current_user()
    if user: 
        grps = user.groups.values_list('name',flat=True)
        dbg = [g for g in grps if g[:4] == stt] 
        if dbg: ugrp = dbg[0]
    #else: ugrp = 'nomadb1'
    return ugrp
    
import threading
from django.utils.deprecation import MiddlewareMixin

_thread_locals = threading.local()

def get_current_request():
    return getattr(_thread_locals, "request", None)

def get_current_user():
    request = get_current_request()
    if request: return getattr(request, "user", None)

def get_current_usrgrp(defdb):
    ugrp = defdb
    stt = 'noma' if defdb == 'default' else 'xna'
    user = get_current_user()
    if user: 
        grps = user.groups.values_list('name',flat=True)
        dbg = [g for g in grps if g[:3] == stt] 
        if dbg: ugrp = dbg[0]
    return ugrp

class ThreadLocals(MiddlewareMixin):
    def process_request(self, request):
        _thread_locals.request = request
        #rq = getattr(_thread_locals, "request", None)
        #usr = getattr(rq, "user", None)
        #_thread_locals.ugrp = usr.groups.values_list('name',flat=True).first()

    def process_response(self, request, response):
        if hasattr(_thread_locals, 'request'): del _thread_locals.request
        return response

    def process_exception(self, request, exception):
        if hasattr(_thread_locals, 'request'): del _thread_locals.request
        
class DataBaseRouter(object):
    
    def db_for_read(self, model, **hints):
        ug =  _thread_locals.ugrp
        #if rq: usr = rq.user
        #if usr: a = usr.username
        #db = get_current_usrgrp('default')
        #if db != None: return db
        return 'default'

    def db_for_write(self, model, **hints):
        db = get_current_usrgrp('default')
        #if db != None: return db
        return 'default'
        
import threading
from django.utils.deprecation import MiddlewareMixin

_thread_locals = threading.local()

def get_current_request():
    return getattr(_thread_locals, "request", None)

def get_current_user():
    request = get_current_request()
    if request: return getattr(request, "user", None)

def get_current_usrgrp(defdb):
    ugrp = defdb
    stt = 'noma' if defdb == 'default' else 'xnax'
    user = get_current_user()
    if user: 
        grps = user.groups.values_list('name',flat=True)
        dbg = [g for g in grps if g[:4] == stt] 
        if dbg: ugrp = dbg[0]
    return ugrp

class TenantMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        _thread_locals.request = request
        response = self.get_response(request)
        return response
        
    def process_request(self, request):
        _thread_locals.request = request
        
    def process_response(self, request, response):
        if hasattr(_thread_locals, 'request'): del _thread_locals.request
        return response

    def process_exception(self, request, exception):
        if hasattr(_thread_locals, 'request'): del _thread_locals.request

class TenantRouter:
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'noma':
            return 'nomadb1'
            #return get_current_usrgrp('default')
        return 'default'

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'noma':
            #return get_current_usrgrp('default')
            return 'nomadb1'
        return 'default'
        
'''