import numpy as np
import pandas as pd
import geopandas as gp
from dataclasses import dataclass
import xarray as xr
import cfgrib
import os
from rasterio.crs import CRS
from pyproj import Transformer
import pygrib
from datetime import datetime, timedelta
import rioxarray
from shapely.geometry import box
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.ticker as ticker
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import metpy.calc as mpcalc
from metpy.units import units
from typing import Optional
import psycopg2
from dotenv import load_dotenv

load_dotenv()



def colortable_gen():
    color_values = {
        -30: 'maroon',
        -20: 'teal',
        -10: 'lavender',
        0: 'white',
        10: 'fuchsia',
        20: 'purple',
        30: 'blue',
        40: 'aqua',
        50: 'green',
        60: 'yellow',
        70: 'orange',
        80: 'red',
        90: 'purple',
        100: 'white',
        110: 'pink',
        130: 'maroon'
    }

    # Create the normalized bounds (0 to 1) for the colormap
    norm = mcolors.Normalize(vmin=min(color_values.keys()), vmax=max(color_values.keys()))

    # Create a LinearSegmentedColormap
    cmap = mcolors.LinearSegmentedColormap.from_list(
        'colormap_tempF',
        [(norm(value), color) for value, color in color_values.items()]
    )

    return cmap

def plot_image(inputfile, model='gfs'):
    cmap = colortable_gen()
    conn, cur = pg_conn()
    fapi_root = "/var/www/fapi/app/"
    grbs = pygrib.open(inputfile)
    if model == 'gfs-graph':
        grbs = grbs.select(name = '2 metre temperature')
    for grb in grbs:

        #prep metadata
        shortname = grb.shortName
        longname = grb.name
        validDate = grb.validDate
        analDate = grb.analDate
        fcstHour = grb.forecastTime
        level = grb.level
        levtype = grb.levtype
        typeOfLevel = grb.typeOfLevel
        unit = grb.units
        cfName = grb.cfVarName
        stepType = grb.stepType
        step = grb.step
        ens_type = ''
        creation_time = datetime.now()
        #step = timedelta(hours=fcstHour)
        filter_kwargs = {'stepType' : f'{stepType}', 'step': step}
        #filter_kwargs = {'shortName' : f'{shortname}', 'step': step }

        #open grib message as xr dataset using cfgrib engine
        ds = xr.open_dataset(
            inputfile,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": filter_kwargs})
        if model in ['gfs', 'gfs-graph']:
            ds=ds.sel(latitude = slice(57,20), longitude= slice(230,300))
        if model in ['ecmwf', 'ecmwf-aifs']:
            ds=ds.sel(latitude = slice(57,20), longitude= slice(-130,-65))
        if model == 'nbm':
            ds = ds.coarsen(y=8, x=8, boundary='trim').mean()
        if model == 'ndfd':
            ds = ds.coarsen(y=2, x=2, boundary='trim').mean()
        variables = list(ds.data_vars)
        variable = ds[variables[0]]
        variable = variable.rio.write_crs("EPSG:4326")
        variable.attrs['units'] = 'kelvin'
        variable = variable.metpy.quantify() 
        variable_F = variable.metpy.convert_units('degF')

        #plot image
        
        fig, ax = plt.subplots(figsize=(10, 8), subplot_kw={'projection': ccrs.PlateCarree()})

        if model in ['ecmwf', 'ecmwf-aifs']:
            contour = ax.contourf(variable.longitude, variable.latitude, variable_F.values, transform=ccrs.PlateCarree(), levels=161, vmin=-30, vmax=130, cmap=cmap)
        else:
            contour = ax.pcolormesh(variable.longitude, variable.latitude, variable_F.values, transform=ccrs.PlateCarree(), vmin=-30, vmax=130, cmap=cmap, shading='gouraud')
        
        #contour = variable_F.plot(ax=ax, transform=ccrs.PlateCarree(), vmin=-30, vmax=130, cmap=cmap)
        ax.add_feature(cfeature.STATES)
        ax.coastlines()
        cbar = fig.colorbar(contour, shrink=1.0, orientation='horizontal', pad=0.03, aspect=60)
        cbar.ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
        #colorbar = contour.colorbar
        #colorbar.ax.set_position([0.78, 0.3, 0.03, 0.4])
        
        time_init = pd.Timestamp(ds.time.values).strftime('%D %H')
        time_valid = pd.Timestamp(ds.valid_time.values).strftime('%a %D %H')
        
        extents = {
            'conus': [-130, -65, 20, 50],       # Global extent
            'central': [-105, -85, 28, 50],     # Regional extent
            'oklahoma': [-103, -93, 33, 38],
            'arkansas': [-95, -89, 32.5, 37],         # Another regional extent
            }
        extents2 = {
            'conus': [-130, -65, 20, 50]
            }
        
        for i, (region, extent) in enumerate(extents.items()):
            print(f'plotting {model} {shortname} {analDate} {validDate} {step} {region}')
            ax.set_extent(extent)
            plt.draw()
            plt.title(f"{model.upper()} {shortname} {time_init}Z fcst hr {step} {time_valid}Z")
            fapi_filename = f"static/data/plots/{model}/{model}_{region}_{shortname}_{str(step).zfill(3)}.png"
            fapi_path = f"{fapi_root}{fapi_filename}"
            plt.savefig(f"data/images/{model}_plot/{model}_{region}_{shortname}_{str(step).zfill(3)}.png", bbox_inches='tight', pad_inches=0.1, dpi=72)
            plt.savefig(fapi_path, bbox_inches='tight', pad_inches=0.1, dpi=72)
         
            insert_query = """
            INSERT INTO model_image.image_meta (path_rel, model, wxvar, "initTime", "validTime", step_hours, "stepType", level, ens_type, "creationTime", region)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (model, wxvar, "initTime", "validTime", level, ens_type, region)
            DO NOTHING;
            """
            #print(insert_query, (fapi_filename, model, shortname, analDate, validDate, step, stepType, level, ens_type, creation_time))
            cur.execute(insert_query, (fapi_filename, model, shortname, analDate, validDate, step, stepType, level, ens_type, creation_time, region))
            if i == len(extents) - 1:
                plt.close(fig)   
    conn.commit()

def pg_conn():

    DB_HOST=os.getenv("DB_HOST")
    DB_NAME=os.getenv("DB_NAME")
    DB_USER=os.getenv("DB_USER")
    DB_PASS=os.getenv("DB_PASS")
    DB_PORT=os.getenv("DB_PORT")

    # Connect to the PostGIS database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor object
    cur = conn.cursor()
    return conn, cur

def tableClean():
    conn, cur = pg_conn()
    cutoff_time = datetime(2024, 11, 20, 0, 0)  # Replace with your desired datetime

    # SQL query to delete rows
    delete_query = "DELETE FROM my_table WHERE time < %s"

    # Execute the delete query
    with conn.cursor() as cur:
        cur.execute(delete_query, (cutoff_time,))
        conn.commit()  # Commit the transaction

    # Close the connection
    conn.close()
