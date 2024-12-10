from app import *
import pygrib


model='ecmwf'
dir_root=''
latestGrb = f'{dir_root}data/gribs/{model.lower()}/latest/{model.lower()}-latest.grb2'
""" if os.path.exists(latestGrb):
    os.remove(latestGrb) """
if model in ['ecmwf', 'ecmwf-aifs']:
    latestGrb = f'{dir_root}data/gribs/{model.lower()}/maxmin/{model.lower()}-latest.grib'


#grbs = appendGrib(model, result_list)
grbs = pygrib.open(latestGrb)
tree = buildTree(model, grbs)

df_coords, nearest_indices = targetPointIndex(tree)

#createOutput('Maximum temperature', nearest_indices, df_coords, grbs, model)
createOutput('2 metre temperature', nearest_indices, df_coords,  model=model, grbs=grbs)