from app import *
from app_plot import *

url_max ='https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus/VP.001-003/ds.maxt.bin'
url_min ='https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus/VP.001-003/ds.mint.bin'
url_max2 ='https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus/VP.004-007/ds.maxt.bin'
url_min2 ='https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus/VP.004-007/ds.mint.bin'
url_list = [url_max, url_min, url_max2, url_min2]
url_def = ['maxt3', 'mint3', 'maxt7', 'mint7']
result_list = []

for url, def_ in zip(url_list, url_def):
    indexFile = requests.get(url)
    indexFile = (indexFile.content)
    tempfilename = f"data/gribs/ndfd/maxmin/ndfd_{def_}.grb2"
    result_list.append(tempfilename)
    f = open(tempfilename, 'wb')
    f.write(indexFile)
    f.close()

model = 'ndfd'
latestGrb = f'data/gribs/{model.lower()}/maxmin/{model.lower()}-latest.grb2'
grbs = appendGrib(model, result_list)
tree = buildTree(model, grbs)

df_coords, nearest_indices = targetPointIndex(tree)

createOutput('Maximum temperature', nearest_indices, df_coords, grbs=grbs, model=model)
createOutput('Minimum temperature', nearest_indices, df_coords, grbs=grbs, model=model)

plot_image(latestGrb, model)