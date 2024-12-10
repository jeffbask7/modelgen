from app import *
from app_plot import *
from ecmwf.opendata import Client
from itertools import chain
import pygrib

ens_range = chain(range(12,144,3),range(150,361,6))
model = 'ecmwf'
gribFile = 'data/gribs/ecmwf/maxmin/ecmwf-latest.grib'


client = Client(source='ecmwf')

""" client.retrieve(
    stream='enfo',
    step=step,
    type="cf", #fc for HRES fcst #pf for perterbed fcst
    #param=["msl", "2t", "10u", "10v", "100u", "100v", "tp"],
    #param = ['mx2t6','mn2t6'],
    param = ['2t'],
    target=gribFile,
) """

latestrun = client.latest(
    #stream='enfo',
    step=[144],
    type="fc", #fc for HRES fcst #pf for perterbed fcst
    #param=["msl", "2t", "10u", "10v", "100u", "100v", "tp"],
    #param = ['mx2t6','mn2t6'],
    param = ['2t'],
)

if latestrun.hour in [6,18]:
    ens_range = range(12,144,3)
step = list(ens_range)
                 
client.retrieve(
    #stream='enfo',
    step=step,
    type="fc", #fc for HRES fcst #pf for perterbed fcst
    #param=["msl", "2t", "10u", "10v", "100u", "100v", "tp"],
    #param = ['mx2t6','mn2t6'],
    param = ['2t'],
    target=gribFile,
)



grbs = pygrib.open(gribFile)
# = appendGrib(model, result_list)
tree = buildTree(model, grbs)

df_coords, nearest_indices = targetPointIndex(tree)

#createOutput('Maximum temperature at 2 metres in the last 6 hours', nearest_indices, df_coords, grbs, model)
#createOutput('Minimum temperature at 2 metres in the last 6 hours', nearest_indices, df_coords, grbs, model)
createOutput('2 metre temperature', nearest_indices, df_coords, grbs=grbs, model=model)

plot_image(gribFile, model)