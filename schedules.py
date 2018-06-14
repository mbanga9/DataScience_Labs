import pandas as pd

def partial_schedules(visu_trips):
    ''' Creates a dataframe that contains all distinct directed 
        edges in the Network. Each row contains the origin node
        the destination node and the mean of transport used 
        through the edge.
    '''
    

    df_partial_schedules = pd.DataFrame()
    # Get all trips and their associated mean of transport
    trips = visu_trips[['stop_list', 'product_id']].drop_duplicates()
    # Iterate on each trip to construct arcs
    for index, row in trips.iterrows():
        stops_list = row[0]
        product_id = row[1]

        # list of stop names along the trip
        stop_names = stops_list.split(' | ')
        # create edges associated with the trip
        trip_edges = list(zip(stop_names[0:-1:1], stop_names[1::1]))
        # temporary dataframe
        to_concat = pd.DataFrame(trip_edges, columns=['origin', 'destination'])
        to_concat['product_id'] = product_id
        
        # concatenate new arcs with existing arcs in a single dataframe
        df_partial_schedules = pd.concat([df_partial_schedules, to_concat])
        
        
    df_partial_schedules = df_partial_schedules.sort_values(by=['origin', 'destination'], axis='index', ascending=True, inplace=False, na_position='first')
    df_partial_schedules = df_partial_schedules.drop_duplicates()
    df_partial_schedules = df_partial_schedules.reset_index(drop=True)
    return df_partial_schedules

def schedules(info_trips, df_partial_schedules):
    ''' Creates a dataframe that contains all directed edges
        in the network together with their scheduling information.
        Rows are ordered by origin, destination pair, date_id and
        by scheduled_departure_time.
        
    '''
    df_schedules = pd.DataFrame()
    for index, edge in df_partial_schedules.iterrows():
        origin = edge['origin']
        destination = edge['destination']
        
        info_trips_filtered = info_trips[((info_trips['stop_name'] == origin) | (info_trips['stop_name'] == destination))]
        # temporary dataframe with all lines having either origin, destination or both in their stop list
        #temp = info_trips[((info_trips['stop_name'] == origin) | (info_trips['stop_name'] == destination))]
        #temp = temp.reset_index('trip_id')
        # Get arcs
        for trip_id in info_trips_filtered['trip_id'].unique():
            for date_id in info_trips_filtered['date_id'].unique():
            # utility variable to find arcs in the dataframe 'info_trips'
            #cond = temp[temp['trip_id'] == trip_id]
                df_potential_edge = info_trips_filtered[(info_trips_filtered['trip_id'] == trip_id) & (info_trips_filtered['date_id'] == date_id)]
            # for date_id in cond['date_id'].unique():
                # new_schedule = cond[cond['date_id'] == date_id]
                # 1. A trip must contain both stops included in the arc
                # 2. As stops are stored in the order they are visited, origin must be above destination
                #print('origin :', str(origin), 'destination :', str(destination))
                #print('trip_id :', trip_id, 'date_id :', date_id)
                #print(len(df_potential_edge))
                #print(df_potential_edge, '\n\n\n\n')
                if len(df_potential_edge) == 2 and (df_potential_edge.iloc[:1]['stop_name'] == origin).bool():
                    df_edge = df_potential_edge
                    #print('origin :', str(origin), 'destination :', str(destination))
                    #print('trip_id :', trip_id, 'date_id :', date_id)
                    
                    #temp = temp.set_index(['line_id'])
                    # compute scheduled arc departure time
                    scheduled_departure_time = df_edge['scheduled_departure_time'].iloc[0]
                    # compute scheduled arc arrival time
                    scheduled_arrival_time = df_edge['scheduled_arrival_time'].iloc[1]
                    # compute scheduled arc travel time
                    scheduled_travel_time = scheduled_arrival_time - scheduled_departure_time

                    # compute actual arc departure time
                    actual_departure_time = df_edge['actual_departure_time'].iloc[0]
                    # compute actual arc arrival time
                    actual_arrival_time = df_edge['actual_arrival_time'].iloc[1]
                    # compute actual arc travel time
                    actual_travel_time = actual_arrival_time - actual_departure_time

                    # Partial construction of the edge 
                    to_concat = df_edge.iloc[:1].drop(['latitude', 'longitude', 'stop_name'], axis=1)
                    to_concat['scheduled_departure_time'] = scheduled_departure_time
                    to_concat['scheduled_arrival_time'] = scheduled_arrival_time
                    to_concat['scheduled_travel_time'] = scheduled_travel_time
                    to_concat['actual_departure_time'] = actual_departure_time
                    to_concat['actual_arrival_time'] = actual_arrival_time
                    to_concat['actual_travel_time'] = actual_travel_time
                    to_concat['origin'] = origin
                    to_concat['origin_coord'] = df_edge['longitude'].iloc[0] + ':' + df_edge['latitude'].iloc[0]
                    to_concat['destination'] = destination
                    to_concat['destination_coord'] = df_edge['longitude'].iloc[1] + ':' + df_edge['latitude'].iloc[1]
                    to_concat['date_id'] = date_id
                    to_concat = to_concat.reset_index()
                    to_concat = to_concat.drop('operator_name', axis=1)
                    to_concat = to_concat.reset_index().drop(['index'], axis=1)

                    df_schedules = pd.concat([df_schedules, to_concat])
    df_schedules['actual_travel_time'] = pd.to_datetime(df_schedules['actual_travel_time'], format='%d %H:%M:%S', errors='ignore')
    df_schedules = df_schedules[['origin', 'origin_coord', 'destination', 'destination_coord', 'product_id', 'trip_id', 'date_id', 'scheduled_departure_time', 'scheduled_arrival_time', 'scheduled_travel_time', 'actual_departure_time', 'actual_arrival_time', 'actual_travel_time']]
    df_schedules = df_schedules.reset_index(drop=True)
    df_schedules = df_schedules.sort_values(by=['origin', 'origin_coord', 'destination', 'destination_coord', 'date_id', 'scheduled_departure_time'], axis='index', ascending=True, inplace=False, na_position='first')
    return df_schedules