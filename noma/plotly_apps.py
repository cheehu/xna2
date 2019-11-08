import dash_pivottable
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from django_plotly_dash import DjangoDash
from .middleware import get_current_dataset
import pandas as pd

dset = get_current_dataset()
sd = pd.read_excel(dset, sheet_name=None, header=None)
hd = sd['Index'][[1,4]].loc[3:].values.tolist()
dini = {k[0].strip(): k[1].split('%') for k in hd}
sd.pop('Index',None)
pv = []
for k in sd: 
    pv.append(dbc.Alert('Dataset: %s' % k,style={"height": "35px","padding-top": "2px","margin-top": "3px","margin-bottom": "1px", "font-weight": "bold"}))
    pv.append(dash_pivottable.PivotTable(data=sd[k].values.tolist(),
              rendererName=dini[k][0],
              cols=dini[k][1].split('&'),
              rows=dini[k][2].split('&'),
              aggregatorName=dini[k][3],
              vals=dini[k][4].split('&'),
              unusedOrientationCutoff = 'Infinity'))
    #pv.append(html.Hr())
app = DjangoDash('NomaDash')   # replaces dash.Dash
app.layout = html.Div(pv)