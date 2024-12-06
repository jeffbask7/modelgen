import numpy as np
import pandas as pd
import geopandas as gp
import requests
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from cartopy.util import add_cyclic_point
import cartopy.io.shapereader as shpreader
import cartopy.feature as cfeature
from datetime import datetime, timedelta
from matplotlib import pyplot
import matplotlib.pyplot as plt
from matplotlib.tri import Triangulation
from shapely import Polygon
import subprocess
from dataclasses import dataclass
import xarray as xr
import datetime as dt
import cfgrib
import zipfile
import os
import shutil
import rasterio
from rasterio.transform import from_origin
from rasterio.transform import from_bounds
from rasterio.crs import CRS
#import h3
from pyproj import Transformer
import json
import pprint
#import dask
import eccodes
import pygrib
import psycopg2
from osgeo import gdal
from collections import defaultdict
from scipy.spatial import KDTree
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
""" @dataclass
class DateTimeParts:
    year: int
    month: int
    day: int
    hour: int
    minute: int

    @classmethod
    def from_datetime(cls, dt: datetime):
        return cls(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour, minute=dt.minute) """
    
@dataclass
class DateTimeParts:
    year: int
    month: int
    day: int
    hour: int
    month_str: Optional[str] = None
    day_str: Optional[str] = None
    hour_str: Optional[str] = None
    date_str: Optional[str] = None

    @classmethod
    def from_datetime(cls, dt: datetime):
        month_str=str(dt.month).zfill(2)
        day_str = str(dt.day).zfill(2)
        hour_str=str(dt.hour).zfill(2)
        date_str = str(dt.year)+month_str+day_str
        return cls(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour, month_str=month_str, day_str=day_str, hour_str=hour_str, date_str=date_str)
    
def windCalc(u,v):
        #print('windCalc Function')
        wind_abs = np.sqrt(u**2 + v**2)
        wind_dir_trig_to = np.arctan2(u/wind_abs, v/wind_abs)
        wind_dir_trig_to_degrees = wind_dir_trig_to * 180/np.pi ## -111.6 degrees
        wind_dir = wind_dir_trig_to_degrees + 180
        return wind_abs * 2.23694 #TO MPH
def K_to_F(temp):
    temp = ((temp - 273.15) * (9/5)) + 32
    return temp

def F_to_K(temp):
    temp = ((temp - 32) * 5/9) + 273.15
    return temp

def getInitTime_GFS():
    mydate = (dt.datetime.now())
    dateparts = DateTimeParts.from_datetime(mydate)

    if dateparts.hour >= 2 and dateparts.hour <= 8:
        hour_str = '00'
    elif dateparts.hour >= 9 and dateparts.hour <= 15:
        hour_str = '06'
    elif dateparts.hour >= 16 and dateparts.hour <= 22:
        hour_str = '12'
    elif dateparts.hour >= 23:
        hour_str = '18'
    elif dateparts.hour < 2:
        hour_str = '18'
        dateparts = DateTimeParts.from_datetime(mydate - dt.timedelta(days=1))
    else:
        print(f'warning: forecast hour {dateparts.hour} is not between 0 and 23')
        hour_str = None

    return dateparts, hour_str

def getInitTime_GFSGraph():
    mydate = (dt.datetime.now())
    dateparts = DateTimeParts.from_datetime(mydate)

    if dateparts.hour >= 14 and dateparts.hour <= 19:
        hour_str = '00'
    elif dateparts.hour >= 20 and dateparts.hour <= 23:
        hour_str = '06'
    elif dateparts.hour > 23:
        hour_str = '06'
    elif dateparts.hour >= 0 and dateparts.hour <= 1:
        hour_str = '06'
        dateparts = DateTimeParts.from_datetime(mydate - dt.timedelta(days=1))
    elif dateparts.hour >= 2   and dateparts.hour <= 7:
        hour_str = '12'
        dateparts = DateTimeParts.from_datetime(mydate - dt.timedelta(days=1))
    elif dateparts.hour >= 8   and dateparts.hour <= 13:
        hour_str = '18'
        dateparts = DateTimeParts.from_datetime(mydate - dt.timedelta(days=1))
    else:
        print(f'warning: forecast hour {dateparts.hour} is not between 0 and 23')
        hour_str = None

    return dateparts, hour_str

def getInitTime_NBM():
    mydate = (dt.datetime.now())
    #modelrundate = mydate - timedelta(hours=3)
    dateparts = DateTimeParts.from_datetime(mydate)

    if dateparts.hour >= 2 and dateparts.hour <= 8:
        hour_str = '00'
    elif dateparts.hour >= 9 and dateparts.hour <= 15:
        hour_str = '06'
    elif dateparts.hour >= 16 and dateparts.hour <= 21:
        hour_str = '12'
    elif dateparts.hour >= 22:
        hour_str = '18'
    elif dateparts.hour < 2:
        hour_str = '18'
        dateparts = DateTimeParts.from_datetime(mydate - dt.timedelta(days=1))
    else:
        print(f'warning: forecast hour {dateparts.hour} is not between 0 and 23')
        hour_str = None

    return dateparts, hour_str

def appendGrib(model, result_list):
    #model = 'ndfd'
    latestGrb = f'data/gribs/{model.lower()}/latest/{model.lower()}-latest.grb2'
    command_cp = f'cp {result_list[0]} {latestGrb}'
    subprocess.call(command_cp, shell=True)

    for grib_single in result_list[1:]:
        print(grib_single)
        command_append = f'wgrib2 -append {grib_single} -grib {latestGrb}'
        subprocess.call(command_append, shell=True)
    return pygrib.open(latestGrb)


def buildTree(model, grbs):
    latestGrb = f'data/gribs/{model.lower()}/maxmin/{model.lower()}-latest.grb2'
    gribfile = latestGrb
    #grbs = pygrib.open(gribfile)
    for grb in grbs:
        print(grb)
    grb1 = grbs[1]
    print(grb1)
    lats, lons = grb1.latlons()
    if model in ['gfs', 'gfs-graph']:
        lons = ((lons + 180) % 360) - 180 #GFS only
    grid_points = np.column_stack((lats.ravel(), lons.ravel()))
    # Build a KDTree from the lat/lon grid
    return KDTree(grid_points)

def targetPointIndex(tree):
    df_coords = pd.read_csv('stations.csv')
    coordinates = df_coords[['lat', 'lon']].values
    nearest_indices = [tree.query([lat, lon])[1] for lat, lon in coordinates]
    return df_coords, nearest_indices

def createOutput(var, nearest_indices, df_coords, grbs, model):   
    data = {icao: [] for icao in df_coords['ICAO']}
    #grbs_all = pygrib.open(gribfile)
    #tgrb = grbs_all.select(shortName = '2t')
    tgrb = grbs.select(name=var)
    tempgrbs = tgrb

    # Loop through the GRIB messages to extract data for all variables and times
    for grb in tempgrbs:
        print(grb)
        var_name = grb.name
        valid_time = grb.validDate

        # Flatten the data grid to align with the grid points
        data_values = (K_to_F(grb.values.ravel()).round(0))

        # For each row in the DataFrame, get the value from the nearest grid point
        for (icao, idx) in zip(df_coords['ICAO'], nearest_indices):
            nearest_value = data_values[idx]

            # Collect the value, time, and variable for the given ICAO
            data[icao].append({
                'time': valid_time,
                'variable': var_name,
                'value': int(nearest_value),
                'lat': df_coords.loc[df_coords['ICAO'] == icao, 'lat'].values[0],
                'lon': df_coords.loc[df_coords['ICAO'] == icao, 'lon'].values[0],
                'city': df_coords.loc[df_coords['ICAO'] == icao, 'CITY'].values[0],  # Add city
                'state': df_coords.loc[df_coords['ICAO'] == icao, 'STATE'].values[0]  # Add state
            })

    # Create an empty list to store rows for the DataFrame
    trows = []

    # Flatten the data dictionary into the list of rows
    for icao, records in data.items():
        for record in records:
            # Append each record as a dictionary to the rows list
            trows.append({
                'ICAO': icao,
                'time': record['time'],
                'variable': record['variable'],
                'value': record['value'],
                'lat': record['lat'],
                'lon': record['lon'],
                'CITY': record['city'],  # Add city to DataFrame
                'STATE': record['state']  # Add state to DataFrame
            })
    print(trows)

    # Convert the list of rows into a DataFrame
    result_df = pd.DataFrame(trows)

    # The final DataFrame contains the time series for each ICAO code
    print(result_df)
    csv_out = f'/data/point/{model}/output_data_{var_out}.csv'
    result_df.to_csv(csv_out, index=False)

    features = []
    for icao, records in data.items():
        # Extract lat/lon from one of the records (all will have the same lat/lon for the same ICAO)
        lat = records[0]['lat']
        lon = records[0]['lon']
        city = records[0]['city']  # Extract city
        state = records[0]['state']  # Extract state

        # Prepare time series for this ICAO
        time_series = [{
            'time': str(record['time']),
            'variable': str(record['variable']),
            'value': str(record['value'])
        } for record in records]

        # Create the GeoJSON feature for this ICAO with the time series
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [str(lon), str(lat)]
            },
            "properties": {
                "ICAO": icao,
                "CITY": city,  # Add city to GeoJSON
                "STATE": state,  # Add state to GeoJSON
                "time_series": time_series
            }
        }
        features.append(feature)

    # Create the final GeoJSON structure
    geojson_data = {
        "type": "FeatureCollection",
        "features": features
    }

    # Determine file name based on the variable type
    if var == 'Maximum temperature':
        var_out = 'maxtemp'
    elif var == 'Minimum temperature':
        var_out = 'mintemp'
    elif var == '2 metre temperature':
        var_out = 'temp'   
    else:
        var_out = var
    
    # Export to GeoJSON file
    """ with open(f'output_data_{var_out}.geojson', 'w') as f:
        json.dump(geojson_data, f, indent=4) """
    
    # Export to GeoJSON file
    with open(f'/var/www/fapi/app/static/data/point/{model}/output_data_{var_out}.geojson', 'w') as f:
        json.dump(geojson_data, f, indent=4)


    print("GeoJSON with time series exported successfully!")



def getFilterGrib_MaxMin(runDate, indexHour, model='nbm'):
    index_list = []
    runTime = runDate.hour
    year = runDate.year
    month = runDate.month
    day = runDate.day
    fcsthr = indexHour
    runTime_str = str(runTime).zfill(2)
    fcsthr_str = str(fcsthr).zfill(3)
    runDate = rf'{year}{str(month).zfill(2)}{str(day).zfill(2)}'
    byte_ranges = defaultdict(list)

    if model == 'nbm':
        url_name = rf'https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.{runDate}/{runTime_str}/core/blend.t{runTime_str}z.core.f{fcsthr_str}.co.grib2'
    elif model == 'gfs':
        url_name = rf'https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{runDate}/{runTime_str}/atmos/gfs.t{runTime_str}z.pgrb2.0p25.f{fcsthr_str}'
    elif model == 'gfs-graph':
        url_name = rf'https://noaa-nws-graphcastgfs-pds.s3.amazonaws.com/graphcastgfs.{runDate}/{runTime_str}/forecasts_13_levels/graphcastgfs.t{runTime_str}z.pgrb2.0p25.f{fcsthr_str}'
        print('gfs-graph url', url_name)        
    else:
        raise ValueError('Invalid model type')

    index_url = f"{url_name}.idx"
    print(f"Index URL: {index_url}")
    
    response = requests.get(index_url)
    index_content = response.text.splitlines()

# Conditions for matching variables and levels dynamically
    conditions = {
        'ASNOW': lambda param, level: param == 'ASNOW' and ((len(level) < 30) and (level.split(":"))[-1] == ''),
        'WIND': lambda param, level: param == 'WIND' and (((level.split(":"))[-1] == '') and (level.split(":"))[0][0] != 's'),
        'TMP': lambda param, level: param == 'TMP' and '2 m above ground' in level,
        'APCP': lambda param, level: param == 'APCP' and ((len(level) < 30) and (level.split(":"))[-1] == ''),
        'DSWRF': lambda param, level: param == 'DSWRF' and 'surface' in level
    }

    conditions = {
        'TMIN': lambda param, level: param == 'TMIN' and (level.split(":"))[-1] == '',
        'TMAX': lambda param, level: param == 'TMAX' and (level.split(":"))[-1] == '',
        'TMP': lambda param, level: param == 'TMP' and '2 m above ground' in level,
    }

    conditions_graph = {
        'TMP': lambda param, level: param == 'TMP' and '2 m above ground' in level,
    }

    if model == 'gfs-graph':
        conditions = conditions_graph

    prev_param = None  # Variable to track the previous parameter
    prev_level = None  # Track the previous level for handling the end byte
    get_next_startbyte = False

    for line in index_content:
        indexDict = line.split(":")
        startByte = indexDict[1]
        prev_startByte = startByte

        #add current startbyte as endbyte for previous param
        if get_next_startbyte:
            pass
            #print('next startbyte: ', prev_startByte)


        param = indexDict[3].strip()  
        #level = indexDict[4].strip() 
        level = ":".join(indexDict[4:]).strip()  # Combine everything after index 3 to form the level

        # Check if the current line matches any of the conditions
        if get_next_startbyte and (len(byte_ranges) > 0):
            #print('adding endbyte', prev_startByte, prev_param )
            byte_ranges[prev_param][-1]['end'] = prev_startByte
            #print(byte_ranges)
            get_next_startbyte = False

        """ if param in conditions:
            condition = conditions[param]  # Get the condition for the param """

        for condition_name, condition_func in conditions.items():
            if condition_func(param, level):
                #if condition(param, level):  # Call the condition function with param and level
                print(f'Matched {param} {level}: Start byte {startByte}')
                #print('get_next_startbyte set to True')
                get_next_startbyte = True

                # If it's a new range, start tracking it
                if prev_param != param or prev_level != level:
                    byte_ranges[param].append({'start': startByte, 'end': None})

                    # Update tracking variables
                prev_param = condition_name
                prev_level = level

            else:
                pass

        else:
            pass

    # Combine byte ranges as needed for each variable
    index_list = []
    for var, ranges in byte_ranges.items():
        for byte_range in ranges:
            start = byte_range['start']
            end = byte_range['end']  # If end is None, use start as the end
            #print(f"Appending range for {var}: {start}-{end}")
            index_list.append(f"{start}-{end}")


        # Download and merge GRIB file
    #gribFile = f"data/gribs/{model.lower()}/{model.lower()}-{runDate}_{runTime_str}_{fcsthr_str}.grb2"
    #gribFile = f"data/gribs/{model.lower()}/latest/{model.lower()}-{fcsthr_str}.grb2"
    gribFile = f"data/gribs/{model.lower()}/maxmin/{model.lower()}-{fcsthr_str}.grb2"

    if os.path.exists(gribFile):
        os.remove(gribFile)

    if len(index_list) > 0:
        for byte_range in index_list:
            print(f"Downloading byte range: {byte_range}")
            command = rf'curl --range {byte_range} {url_name} >> {gribFile}'
            os.system(command)
        else:
            print(f'no matches for forecast hour {fcsthr_str} ')

    #return gribFile
    return index_list, gribFile

def getFilterGrib(runDate, indexHour, model='gfs-test'):
    index_list = []
    runTime = runDate.hour
    year = runDate.year
    month = runDate.month
    day = runDate.day
    fcsthr = indexHour
    runTime_str = str(runTime).zfill(2)
    fcsthr_str = str(fcsthr).zfill(3)
    runDate = rf'{year}{str(month).zfill(2)}{str(day).zfill(2)}'
    byte_ranges = defaultdict(list)

    if model == 'nbm':
        url_name = rf'https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.{runDate}/{runTime_str}/core/blend.t{runTime_str}z.core.f{fcsthr_str}.co.grib2'
    elif model == 'gfs-test':
        url_name = rf'https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{runDate}/{runTime_str}/atmos/gfs.t{runTime_str}z.pgrb2.0p25.f{fcsthr_str}'
    elif model == 'gfs-graph':
        url_name = rf'https://noaa-nws-graphcastgfs-pds.s3.amazonaws.com/graphcastgfs.{runDate}/{runTime_str}/forecasts_13_levels/graphcastgfs.t{runTime_str}z.pgrb2.0p25.f{fcsthr_str}'
    else:
        raise ValueError('Invalid model type')

    index_url = f"{url_name}.idx"
    print(f"Index URL: {index_url}")
    
    response = requests.get(index_url)
    index_content = response.text.splitlines()

# Corrected conditions dictionary with unique keys
    conditions = {
        'PRMSL': lambda param, level: param == 'PRMSL',
        'REFC': lambda param, level: param == 'REFC',
        'CSNOW': lambda param, level: param == 'CSNOW',
        'CFRZR': lambda param, level: param == 'CFRZR',
        'CICEP': lambda param, level: param == 'CICEP',
        'CFRZR': lambda param, level: param == 'CFRZR',
        'HGT_500mb': lambda param, level: param == 'HGT' and '500 mb' in level,
        'HGT_1000mb': lambda param, level: param == 'HGT' and '1000 mb' in level,
        'TMP_2m_above_ground': lambda param, level: param == 'TMP' and '2 m above ground' in level,
        'UGRD_10m_above_ground': lambda param, level: param == 'UGRD' and '10 m above ground' in level,
        'VGRD_10m_above_ground': lambda param, level: param == 'VGRD' and '10 m above ground' in level,
        'UGRD_100m_above_ground': lambda param, level: param == 'UGRD' and '100 m above ground' in level,
        'VGRD_100m_above_ground': lambda param, level: param == 'VGRD' and '100 m above ground' in level,
        'APCP': lambda param, level: param == 'APCP' and ((len(level) < 30) and (level.split(":"))[-1] == ''),
    }

    index_length = len(index_content)

    # Process each line in the index file
    for i in range(index_length):
        line = index_content[i]
        indexDict = line.split(":")
        startByte = indexDict[1]
        param = indexDict[3].strip()
        level = ":".join(indexDict[4:]).strip()

        # Check if the current line matches any condition
        matched = False
        for condition_name, condition_func in conditions.items():
            if condition_func(param, level):
                matched = True
                # Determine the end byte
                if i < index_length - 1:
                    next_line = index_content[i + 1]
                    next_indexDict = next_line.split(":")
                    endByte = next_indexDict[1]
                    byte_length = int(endByte) - int(startByte)
                else:
                    # Last line; we don't have a next start byte
                    byte_length = 0  # Or handle accordingly if you know the total file size

                # Append the byte range to the index list
                #index_list.append((int(startByte), int(endByte)))
                index_list.append(f'{startByte}-{endByte}') #USE FOR CURL
                #index_list.append((int(startByte), byte_length)) #USE FOR EARTKKIT.DATA
                print(f"Matched {param} {level}: Start byte {startByte}, Length {byte_length}")
                break  # No need to check other conditions once matched

    gribFile = f"data/gribs/{model.lower()}/latest/{model.lower()}-{fcsthr_str}.grb2"
    #gribFile = f"data/gribs/{model.lower()}/maxmin/{model.lower()}-{fcsthr_str}.grb2"

    if os.path.exists(gribFile):
        os.remove(gribFile)

    if len(index_list) > 0:
        for byte_range in index_list:
            print(f"Downloading byte range: {byte_range}")
            command = rf'curl --range {byte_range} {url_name} >> {gribFile}'
            os.system(command)
        else:
            print(f'no matches for forecast hour {fcsthr_str} ')

    #return gribFile
    return index_list, gribFile