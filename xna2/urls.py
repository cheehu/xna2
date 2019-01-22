from django.contrib import admin
from django.urls import path
from noma.views import exec_prog
from noma.admin import noma_admin_site
admin.site.site_url = ""

urlpatterns = [
    path('admin/', admin.site.urls),
    path('noma/', noma_admin_site.urls),
    path('exec_prog/', exec_prog),
]