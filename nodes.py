import edges
import pandas as pd
import numpy as np

def get_nodes(df_schedules):
    # Get list of all nodes in the network
    # Origin nodes
    nodes_1 = df_schedules[['origin', 'origin_coord', 'product_id']].drop_duplicates()
    nodes_1.columns = ['node', 'coord', 'product_id']
    # Destination nodes
    nodes_2 = df_schedules[['destination', 'destination_coord', 'product_id']].drop_duplicates()
    nodes_2.columns = nodes_1.columns
    # Construct dataframe of nodes
    nodes = pd.concat([nodes_1, nodes_2], axis=0).drop_duplicates().reset_index(drop=True)
    return nodes


def get_nodes_params(df_schedules, defined_timeslots):
        # Create dataframe with necessary node information to perform routing
    df_nodes = pd.DataFrame(columns=['node', 'coord', 'product_id', 'timeslots', 'departure_params [µ, s]', 
                                     'arrival_params [µ,s]'])

    nodes = get_nodes(df_schedules)
    for index, values in nodes.iterrows():
            node = values.iloc[0]
            coord = values.iloc[1]
            transport = values.iloc[2]    
            # Get all edges leaving the current node
            from_node = df_schedules[((df_schedules['origin'] == node) & (df_schedules['origin_coord'] == coord))]
            # Keep only relevant columns
            from_node = from_node[['origin', 'origin_coord', 'destination','destination_coord', 'product_id', 'scheduled_departure_time', 'actual_departure_time']]
            # Add departure delay values for the node 
            from_node['departure_delay [sec]'] = (from_node['actual_departure_time'] - from_node['scheduled_departure_time']) / np.timedelta64(1, 's')
            # Group leaving edges by scheduled departure time slots
            #from_grouped = from_node.groupby(departure_date_range, axis=0)['departure_delay [sec]']
            from_grouped = from_node.groupby(lambda index: edges.groupBy_timeslots(from_node, index, 'scheduled_departure_time', defined_timeslots), axis=0)['departure_delay [sec]']
            # mean and standard deviation of departure delays for each time slot of the current node

            # timeslots with at least one element in the dataframe 'from_node'
            observed_timeslots = list(from_grouped.groups.keys())
            dep_params = []
            for name in defined_timeslots:
                if name in observed_timeslots:
                    # indices in the dataframe 'from_node' associated with the timeslot 'name'
                    group_indices = from_grouped.groups[name]
                    # rows in the dataframe 'from_node' associated with the timeslot 'name'
                    group_elems = from_node.loc[group_indices]['departure_delay [sec]']
                    # add mean and standard deviation of departure delays for the time slot 'name'
                    dep_params.append((round(group_elems.mean(), 1), round(group_elems.std(), 1)))
                else:
                    nan = float('NaN')
                    dep_params.append((nan, nan))

            # Get all edges arriving at the current node
            to_node = df_schedules[((df_schedules['destination'] == node) & (df_schedules['destination_coord'] == coord))]
            # Keep only relevant columns
            to_node = to_node[['origin', 'origin_coord', 'destination','destination_coord', 'product_id',  'scheduled_arrival_time', 'actual_arrival_time']]
            # Add arrival delay values for the node
            to_node['arrival_delay [sec]'] = (to_node['actual_arrival_time'] - to_node['scheduled_arrival_time']) / np.timedelta64(1, 's')
            # Group arriving edges by scheduled arrival time
            #to_grouped = to_node.groupby(arrival_date_range, axis=0)['arrival_delay [sec]']
            to_grouped = to_node.groupby(lambda index: edges.groupBy_timeslots(to_node, index, 'scheduled_arrival_time', defined_timeslots), axis=0)['arrival_delay [sec]']
            # mean and standard deviation of arrival delays for each time slot of the current node
            # arrival_params = [(round(group.mean(), 1), round(group.std(), 1)) for _, group in to_grouped]

            # timeslots with at least one element in the dataframe 'to_node'
            observed_timeslots = list(to_grouped.groups.keys())
            arr_params = []
            for name in defined_timeslots:
                if name in observed_timeslots:
                    # indices in the dataframe 'from_node' associated with the timeslot 'name'
                    group_indices = to_grouped.groups[name]
                    # rows in the dataframe 'from_node' associated with the timeslot 'name'
                    group_elems = to_node.loc[group_indices]['arrival_delay [sec]']
                    # add mean and standard deviation of departure delays for the time slot 'name'
                    arr_params.append((round(group_elems.mean(), 1), round(group_elems.std(), 1)))
                else:
                    nan = float('NaN')
                    arr_params.append((nan, nan))


            # Generate node information and add it to the dataframe df_nodes
            #print(len(timeslots), len(departure_params), len(arrival_params))
            if len(defined_timeslots) == len(dep_params) == len(arr_params):
                to_concat = pd.DataFrame({'node': node, 'coord': coord, 'product_id': transport, 'timeslot': defined_timeslots, 
                                       'departure_params [µ, s]': dep_params, 'arrival_params [µ, s]': arr_params})
                df_nodes = pd.concat([df_nodes, to_concat], axis=0)
    df_nodes = df_nodes[['node', 'coord', 'product_id', 'timeslot', 'departure_params [µ, s]', 'arrival_params [µ, s]']]
    df_nodes = df_nodes.reset_index(drop=True)
    return df_nodes