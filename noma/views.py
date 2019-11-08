import json
from django.shortcuts import render
from celery.result import AsyncResult
from django.http import HttpResponse
#from .forms import queExecForm
from .middleware import set_ds_for_dash


def exec_prog(request):
    task_id = request.GET.get('task_id', None)
    if task_id is not None:
        task = AsyncResult(task_id)
        data = {
            'state': task.state,
            'result': task.result,
            }
        return HttpResponse(json.dumps(data), content_type='application/json')
    else:
        return HttpResponse('No job id given.')
        
def dash_pivot(request):
    dset = request.GET.get('dataset', None)
    set_ds_for_dash(dset)
    import noma.plotly_apps
    context = {'dataset': dset} 
    return render(request, 'dash_pivot.html', context)