import pandas

dataset_dates = ['20151211', '20160309', '20160513', '20160615', '20160819', '20160922', '20161103']

for i in dataset_dates:
    df_times = pandas.read_csv('datasets\\subway\\'+i+'\\stop_times.txt', error_bad_lines=False)
    df_trips = pandas.read_csv('datasets\\subway\\'+i+'\\trips.txt', error_bad_lines=False)
    df_calender = pandas.read_csv('datasets\\subway\\'+i+'\\calendar.txt', error_bad_lines=False)
    df_times['trip_id'] = df_times['trip_id'].astype(str)
    df_trips['trip_id'] = df_trips['trip_id'].astype(str)
    df_calender['service_id'] = df_calender['service_id'].astype(str)
    df_trips['service_id'] = df_trips['service_id'].astype(str)

    df = df_times.merge(df_trips.merge(df_calender, on='service_id', how='left'), on='trip_id', how='left')
    subway_only = pandas.concat([df[df['service_id'].str.startswith('LRV', na=False)], df[df['service_id'].str.startswith('RTL', na=False)]])

    subway_only['arrival_time'] = subway_only['arrival_time'].map(lambda x: str(int(x[:2])-24)+x[2:] if int(x[:2])>23 else x)
    subway_only['departure_time'] = subway_only['departure_time'].map(lambda x: str(int(x[:2])-24)+x[2:] if int(x[:2])>23 else x)
    subway_only['hour'] = subway_only['arrival_time'].map(lambda x: int(x[:2] if x[2] == ':' else x[:1]))

    subway_only['arrival_time'] = pandas.to_datetime(subway_only['arrival_time'])
    subway_only['departure_time'] = pandas.to_datetime(subway_only['departure_time'])

    subway_only['time_taken'] = subway_only.arrival_time - subway_only.departure_time.shift(1)
    subway_only['time_taken'] = subway_only['time_taken'].astype('timedelta64[m]').map(lambda x: x*60)
    subway_only.loc[subway_only['stop_sequence'] == 1, 'time_taken'] = -1
    subway_only['src_id'] = subway_only.stop_id.shift(1)
    subway_only.loc[subway_only['stop_sequence'] == 1, 'src_id'] = -1
    subway_only = subway_only[subway_only['stop_sequence'] != 1]

    def days_list(row):
        temp = [i for i in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'] if row[i] == 1]
        return ','.join(temp)

    subway_only['days'] = subway_only.apply(days_list, axis=1)
    subway_only['days'] = subway_only['days'].map(lambda x: x.split(','))
    col = list(subway_only.columns)
    for i in ['days', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
        col.remove(i)
    subway_stacked = subway_only.set_index(col)['days'].apply(pandas.Series).stack().reset_index()

    def month_list(row):
        start = int(str(row['start_date'])[4:6])
        end = int(str(row['end_date'])[4:6])
        if end<start:
            end = end+12
        temp = [str(i if i<=12 else (i-12)) for i in range(start, end+1)]
        return ','.join(temp)

    subway_stacked['month'] = subway_stacked.apply(month_list, axis=1)
    subway_stacked['month'] = subway_stacked['month'].map(lambda x: x.split(','))
    col = list(subway_stacked.columns)[:-3]
    col.append('level')
    col.append('day')
    col.append('month')
    subway_stacked.columns = col
    col.remove('month')
    subway_stacked = subway_stacked.set_index(col)['month'].apply(pandas.Series).stack().reset_index()
    col = list(subway_stacked.columns)[:-1]
    col.append('month')
    subway_stacked.columns = col
    subway_stacked.to_csv('stacked_'+i+'.csv')
