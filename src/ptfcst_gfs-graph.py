from app import *
from app_plot import *


dateparts, hour_str = getInitTime_GFSGraph()
result_list = []
modelrunTimes = []
modelforecastTimes= []
modelforecastSteps = []
dir_root = ''
model = 'gfs-graph'
modelrun = datetime(dateparts.year,dateparts.month,dateparts.day,int(hour_str))
datetime_parts = DateTimeParts.from_datetime(modelrun)
print(datetime_parts)
for indexHour in range(12,385,6):
    
    print('getFilterGrib')
    index_list, result = getFilterGrib_MaxMin(datetime_parts, indexHour, model)
    if os.path.exists(result):
        result_list.append(result)
        print('result', result)

latestGrb = f'{dir_root}data/gribs/{model.lower()}/latest/{model.lower()}-latest.grb2'
if os.path.exists(latestGrb):
    os.remove(latestGrb)


grbs = appendGrib(model, result_list)
tree = buildTree(model, grbs)

df_coords, nearest_indices = targetPointIndex(tree)

#createOutput('Maximum temperature', nearest_indices, df_coords, grbs, model)
createOutput('2 metre temperature', nearest_indices, df_coords, grbs=grbs, model=model)

plot_image(latestGrb, model)

