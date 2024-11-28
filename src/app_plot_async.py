import asyncio
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
from matplotlib import ticker
import cartopy.crs as ccrs
import cartopy.feature as cfeature

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
import matplotlib.colors as mcolors
import metpy.calc as mpcalc
from metpy.units import units
from typing import Optional
import psycopg2

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

async def process_grb(grb, inputfile, model, cmap, fapi_root, conn):
    # Prep metadata
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
    filter_kwargs = {'stepType': f'{stepType}', 'step': step}

    # Open grib message as xarray dataset
    ds = xr.open_dataset(
        inputfile,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": filter_kwargs})
    
    # Select region based on model
    if model in ['gfs', 'gfs-graph']:
        ds = ds.sel(latitude=slice(57, 20), longitude=slice(230, 300))
    elif model in ['ecmwf', 'ecmwf-aifs']:
        ds = ds.sel(latitude=slice(57, 20), longitude=slice(-130, -65))
    elif model == 'nbm':
        ds = ds.coarsen(y=8, x=8, boundary='trim').mean()
    elif model == 'ndfd':
        ds = ds.coarsen(latitude=2, longitude=2, boundary='trim').mean()
    
    variables = list(ds.data_vars)
    variable = ds[variables[0]]
    variable = variable.rio.write_crs("EPSG:4326")
    variable.attrs['units'] = 'kelvin'
    variable = variable.metpy.quantify()
    variable_F = variable.metpy.convert_units('degF')

    # Create plot
    fig, ax = plt.subplots(figsize=(10, 8), subplot_kw={'projection': ccrs.PlateCarree()})
    if model in ['ecmwf', 'ecmwf-aifs']:
        contour = ax.contourf(variable.longitude, variable.latitude, variable_F.values,
                              transform=ccrs.PlateCarree(), levels=161, vmin=-30, vmax=130, cmap=cmap)
    else:
        contour = ax.pcolormesh(variable.longitude, variable.latitude, variable_F.values,
                                transform=ccrs.PlateCarree(), vmin=-30, vmax=130, cmap=cmap, shading='gouraud')

    ax.add_feature(cfeature.STATES)
    ax.coastlines()
    cbar = fig.colorbar(contour, shrink=1.0, orientation='horizontal', pad=0.03, aspect=60)
    cbar.ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    
    time_init = pd.Timestamp(ds.time.values).strftime('%D %H')
    time_valid = pd.Timestamp(ds.valid_time.values).strftime('%a %D %H')
    
    extents = {
        'conus': [-130, -65, 20, 50],
        'central': [-105, -85, 28, 50],
        'oklahoma': [-103, -93, 33, 38],
        'arkansas': [-95, -89, 32.5, 37],
    }

    cur = conn.cursor()
    for region, extent in extents.items():
        ax.set_extent(extent)
        plt.draw()
        plt.title(f"{model.upper()} {shortname} {time_init}Z fcst hr {step} {time_valid}Z")
        fapi_filename = f"static/data/plots/{model}/{model}_{region}_{shortname}_{str(step).zfill(3)}.png"
        fapi_path = f"{fapi_root}{fapi_filename}"
        plt.savefig(fapi_path, bbox_inches='tight', pad_inches=0.1)

        # Insert into database
        insert_query = """
        INSERT INTO model_image.image_meta (path_rel, model, wxvar, "initTime", "validTime", step_hours, "stepType", level, ens_type, "creationTime", region)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (model, wxvar, "initTime", "validTime", level, ens_type, region)
        DO NOTHING;
        """
        cur.execute(insert_query, (fapi_filename, model, shortname, analDate, validDate, step, stepType, level, ens_type, creation_time, region))
    conn.commit()
    plt.close(fig)


async def main(inputfile, model='gfs'):
    cmap = colortable_gen()
    conn, _ = pg_conn()
    grbs = pygrib.open(inputfile)

    tasks = []
    with ThreadPoolExecutor() as executor:
        for grb in grbs:
            task = asyncio.to_thread(process_grb, grb, inputfile, model, cmap, "/var/www/fapi/app/", conn)
            tasks.append(task)
        await asyncio.gather(*tasks)


# Run the asynchronous main function
def plot_image(inputfile, model='gfs'):
    asyncio.run(main(inputfile, model))


def pg_conn():
    # Database connection details
    DB_HOST = 'localhost'
    DB_PORT = '5432'
    DB_NAME = 'geoserver_db'
    DB_USER = 'geoserver_user'
    DB_PASS = 'geoserver'

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