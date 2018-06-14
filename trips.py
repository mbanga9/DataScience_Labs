import pandas as pd
import datetime
from datetime import timezone

def date_to_integer(row, origin=datetime.datetime(2017, 9, 13, 2, 0, 0)):
    '''  transform a date to an integer. Dates within the same 24 hours
         interval map to the same integer. the interval is specied by the
         origin argument. 
         
         example: 
         
         origin = 2017.09.13 2h00
         date_to_integer(2017.09.13 14h05) returns 0
         date_to_integer(2017.09.14 1h28) returns 0
         date_to_integer(2017.09.14 8h38) returns 1
     '''
    # set reference time in seconds
    origin_seconds = origin.replace(tzinfo=timezone.utc).timestamp()
    # transform datetime into seconds    
    try:
        # transform datetime into seconds
        # row['scheduled_departure_time'].
        curr_datetime_seconds = row[0].replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        try:
            # row['scheduled_arrival_time']
            curr_datetime_seconds = row[1].replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            print('Time information not found !')
    # set target
    target = curr_datetime_seconds - origin_seconds
    # set offset
    day_offset = 60*60*24
    # find integer value to associate with a day
    x = -2
    while not (x*day_offset <= target < (x+1)*day_offset):
        x += 1
    return x

def get_trips(df, trip_ids):
    ''' Creates a dataframe that contains all trips in the network. 
        Each row of the dataframe contains information about one specific
        stop of a trip. A trip is uniquely specified by a trip_id 
        and a date_id.
        Rows are ordered by trip_id and date_id values and increasing
        scheduled_arrival_time. This means that consecutive rows with the
        same trip_id and date_id are successive stops that occured during
        the trip for that date. Additional informations in a row include
        scheduling information and geographical information.
        
        
        Assumption: A trip contains at least 2 stops. '''
        
    info_trips = pd.DataFrame()
    for id_ in trip_ids:#df['trip_id'].unique()[400:410]:
        temp = df.loc[df['trip_id'] == id_]#.copy()
        temp['date_id'] = temp[['scheduled_departure_time', 'scheduled_arrival_time']].apply(date_to_integer, axis=1)
        # We consider trips having at least 2 stops
        if len(temp) > 1:
            # For a given trip_id and date_id sort arrival times at the stops
            temp = temp.sort_values(by=['trip_id', 'date_id', 'scheduled_arrival_time'], axis='index', ascending=True, inplace=False, na_position='first')
            # Select only relevant columns in the dataframe
            temp = temp[['operator_name', 'product_id', 'trip_id', 'date_id', 'scheduled_departure_time', 'actual_departure_time', 'scheduled_arrival_time', 'actual_arrival_time', 'stop_name', 'longitude', 'latitude']]
            # Concatenate the result into a final DataFrame
            info_trips = pd.concat([info_trips, temp], axis=0, levels=['trip_id', 'product_id'], ignore_index=False)
           
    return info_trips

def visu_trips(info_trips, trip_ids):
    ''' Creates a dataframe that contains all trips in the network.
        Each row of the dataframe contains information about one
        specific trip. A trip is uniquely specified by a trip_id
        and a date_id
        Rows are ordered by trip_id and date_id. It contains the 
        sequence of stops along the trip and scheduling information
        as the scheduled_departure_time from the first stop and the
        scheduled_arrival_time to the last stop. '''
    visu_trips = pd.DataFrame()
    for trip_id in trip_ids: #info_trips['trip_id'].unique()[400:410]:
        for date_id in info_trips['date_id'].unique():
            if(date_id != float('NaN')) and (date_id > -1):   
                    visu = info_trips[(info_trips['trip_id'] == trip_id) & (info_trips['date_id'] == date_id)]#.copy()
                    # define sequence of stops for the trip
                    visu['stop_list'] = ' | '.join(visu['stop_name'])
                    # Departure time at the start the first stop
                    st_departure_time = visu['scheduled_departure_time'].iloc[:1]
                    st_departure_time = st_departure_time.reset_index(drop=True)
                    # Schedule arrival time at the last stop
                    st_arrival_time = visu['scheduled_arrival_time'].iloc[-1:]
                    st_arrival_time = st_arrival_time.reset_index(drop=True)

                    # Select only relevant columns in the dataframe
                    visu = visu[['operator_name', 'product_id', 'trip_id', 'date_id', 'stop_list']].iloc[:1]
                    visu = visu.reset_index(drop=True)
                    visu['scheduled_terminus_departure_time'] = pd.Series(st_departure_time, index=visu.index)
                    visu['scheduled_terminus_arrival_time'] = pd.Series(st_arrival_time, index=visu.index)
                    
                    # Concatenate the result into a final DataFrame
                    visu_trips = pd.concat([visu_trips, visu], axis=0, ignore_index=False)
    return visu_trips