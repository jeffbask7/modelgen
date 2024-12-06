from app_plot import *
dir_root = ''
model = 'ndfd'
latestGrb = f'{dir_root}data/gribs/{model.lower()}/latest/{model.lower()}-latest.grb2'
if model in ['ecmwf', 'ecmwf-aifs']:
    latestGrb = f'{dir_root}data/gribs/{model.lower()}/maxmin/{model.lower()}-latest.grib'
plot_image(latestGrb, model)