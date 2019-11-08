from django.contrib import admin
from django.urls import path
from django.urls import include
from noma.views import exec_prog, dash_pivot
from noma.admin import noma_admin_site

admin.site.site_url = ""

urlpatterns = [
    path('admin/', admin.site.urls),
    path('noma/', noma_admin_site.urls),
    path('exec_prog/', exec_prog),
    path('dash-pivot/', dash_pivot),
    path('django_plotly_dash/', include('django_plotly_dash.urls')),
        
]