import urllib.request, json
import pandas as pd
from pandas.io.json import json_normalize

mbta_key = 'rDbpDS_NFE24sgRLU5z60A'

url = urllib.request.urlopen('http://realtime.mbta.com/developer/api/v2/routes?api_key='+mbta_key+'&format')
data = json.loads(url.read().decode())

route_df = pd.DataFrame(columns=['mode_name', 'route_id', 'route_name'])
for i in data['mode']:
    dft = json_normalize(i['route'])
    dft['mode_name'] = i['mode_name']
    route_df = pd.concat([route_df, dft])
del route_df['route_hide']
route_df = route_df.set_index('route_id')
route_df.to_csv('route_ids.csv')

stops_df = pd.DataFrame(columns=['route_id', 'direction', 'parent_station', 'parent_station_name', 'stop_id', 'stop_lat', 'stop_lon', 'stop_name', 'stop_order'])
for i in route_df.index:
    url = "http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key="+mbta_key+"&route="+i+"&format=json"
    url_i = urllib.request.urlopen(url)
    data_i = json.loads(url_i.read().decode())
    for j in data_i['direction']:
        dft = json_normalize(j['stop'])
        dft['direction'] = j['direction_name']
        dft['route_id'] = i
        stops_df = pd.concat([stops_df, dft])

stops_df = stops_df.reset_index()
del stops_df['index']
stops_df.to_csv('boston_transit_stop.csv')
