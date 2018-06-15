import pandas as pd
import datetime
from datetime import timezone
import numpy as np

def groupBy_timeslots(df, index, col, defined_timeslots):
    target = df[col].dt.time.loc[index]
    for slot in defined_timeslots:
        time_left = datetime.datetime.strptime(slot.split('-')[0], '%H:%M:%S').time()
        time_right = datetime.datetime.strptime(slot.split('-')[1], '%H:%M:%S').time()
        if time_left <= target < time_right:
            return slot
    return float('NaN')

def get_edges_params(df_schedules, defined_timeslots):    
    df_edges = pd.DataFrame(columns=['fromNode', 'fromCoord', 'toNode', 'toCoord', 
                                 'product_id', 'timeslots', 'edge_params [µ, s]'])
    # List of all edges
    cols = ['origin', 'origin_coord', 'destination', 'destination_coord', 'product_id']
    edges = df_schedules[cols].drop_duplicates()

    for index, values in edges.iterrows():
            fromNode = values.iloc[0]
            fromCoord = values.iloc[1]
            toNode = values.iloc[2]
            toCoord = values.iloc[3]
            transport = values.iloc[4]

            # Get all travels through the edge
            edge_travels = df_schedules[((df_schedules['origin'] == fromNode) & (df_schedules['origin_coord'] == fromCoord) &
                                        (df_schedules['destination'] == toNode) & (df_schedules['destination_coord'] == toCoord) & (df_schedules['product_id'] == transport))]
            # keep only relevant columns
            edge_travels = edge_travels[['origin', 'origin_coord', 'destination', 'destination_coord', 'product_id', 
                                         'scheduled_departure_time', 'scheduled_travel_time', 'actual_travel_time']]
            # add travel delays for the edge
            edge_travels['travel_delay [sec]'] = (edge_travels['actual_travel_time'] - edge_travels['scheduled_travel_time']) / np.timedelta64(1, 's')
            # Group edges by scheduled departure time slots
            edge_travels_grouped = edge_travels.groupby(lambda index: groupBy_timeslots(edge_travels, index, 'scheduled_departure_time', defined_timeslots), axis=0)

            # timeslots with at least one element in the dataframe 'edge_travels'
            observed_timeslots = list(edge_travels_grouped.groups.keys())
            # instantiate parameter list
            edge_params = []
            for name in defined_timeslots:
                if name in observed_timeslots:
                    # indices in the dataframe 'edge_travels' associated with the timeslot 'name'
                    group_indices = edge_travels_grouped.groups[name]
                    # rows in the dataframe 'edge_travels' associated with the timeslot 'name'
                    group_elems = edge_travels.loc[group_indices]['travel_delay [sec]']
                    # add mean and standard deviation of departure delays for the time slot 'name'
                    edge_params.append((round(group_elems.mean(), 1), round(group_elems.std(), 1)))
                else:
                    nan = float('NaN')
                    edge_params.append((nan, nan))


            # Generate node information and add it to the dataframe df_edges
            if len(defined_timeslots) == len(edge_params):
                to_concat = pd.DataFrame({'fromNode': fromNode, 'fromCoord': fromCoord, 'toNode': toNode, 'toCoord': toCoord, 
                                          'product_id': transport, 'timeslots': defined_timeslots, 
                                          'edge_params [µ, s]': edge_params})
                df_edges = pd.concat([df_edges, to_concat], axis=0)

    df_edges = df_edges[['fromNode', 'fromCoord', 'toNode', 'toCoord', 'product_id', 'timeslots', 'edge_params [µ, s]']]
    df_edges = df_edges.reset_index(drop=True)
    df_edges.head()
    return df_edges