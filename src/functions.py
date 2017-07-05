from datetime import datetime
from classes import Person
import numpy as np

#
# Compute the anomaly amount, mean, and std and store in the Person
#
def compute_anomaly_amount(this_index,neighbor_list,person_list,num_items):
    
    # Generate the list of id, timestamp, amount
    purchase_list = []
    for index in neighbor_list:
        
        if (index != this_index):
        
            local_person = person_list[index]
            purchase_history = local_person.purchase_array
            time_history     = local_person.time_array     
    
            for item in range(0,len(purchase_history)):
            
                purchase_list.append( (index,time_history[item],purchase_history[item]) )
    
    # Only compute anomaly amount if more than two entries
    if (len(purchase_list) > 2):
        #                  0      1         2
        # Format of list: id, timestamp, amount
        sorted_list = sorted(purchase_list, key=lambda purchase: purchase[1], reverse=True) #sort by timestamp
    
        # Cut-off everything after a number of transactions!
        sorted_list = sorted_list[0:num_items]
    
        # Grab the purchase amounts
        amount_list = [el[2] for el in sorted_list]
        amount_list = np.array(amount_list)
    
        # Compute the average and standard deviation of the array
        p_avg = amount_list.mean()
        p_std = amount_list.std()
    
        # Compute the anomaly flag amount
        compare_val = (p_avg + 3.0*p_std)
        person_list[this_index].setAnomAmount(compare_val)
        person_list[this_index].setMean(p_avg) 
        person_list[this_index].setSTD(p_std)
        

        
#
# Parse an entry of the batch file
#
def parse_batch_record(record,person_dict,connection_list,unique_id):
   
    # Determine the type of entry
    event_type = record['event_type']
        
    # Inspect the connection
    if event_type != 'purchase':
            
        person_id = int(record['id1'])
        friend_id = int(record['id2'])
            
        # Check whether to create node
        if (person_id not in unique_id):
            unique_id.add(person_id)
                
            # Create the new person and add to the list
            new_person = Person(person_id)
            person_dict[person_id] = new_person
                
        # Check whether to create node    
        elif (friend_id not in unique_id):
            unique_id.add(friend_id)            
            
            # Create the new person and add to the list
            new_person = Person(friend_id)
            person_dict[friend_id] = new_person
                
        # Add (or delete) the connection
        if event_type == 'befriend':             
            connection_list.append((person_id,friend_id))
        else:
                
            forward_connection = (person_id,friend_id)
            backward_connection = (friend_id,person_id)
                
            if forward_connection in connection_list:
                connection_list.remove(forward_connection)
            if backward_connection in connection_list:
                connection_list.remove(backward_connection)
                
    else:
            
        # Add transaction to the node
        person_id = int(record['id'])
            
        # Create the new person
        if ( person_id not in unique_id ):
                
            unique_id.add(person_id)
                
            # Create the new person
            new_person = Person(person_id)
                
            # Add the purchase record
            purchase = float(record['amount'])
            timevar = datetime.strptime(str(record['timestamp']),'%Y-%m-%d %H:%M:%S')
            new_person.addPurchase(purchase,timevar)
                
            # Add person to the list
            person_dict[person_id] = new_person
                
        else:
                
            # Find the person with the index
            new_person = person_dict[person_id]
            
            # Add the purchase record
            purchase = float(record['amount'])
            timevar = datetime.strptime(str(record['timestamp']),'%Y-%m-%d %H:%M:%S')
            new_person.addPurchase(purchase,timevar)        
            


#            
# Parse a record in the stream file
#
def parse_stream_record(record,person_dict,unique_id,strecord,network):
    
    # Determine the type of entry
    event_type = record['event_type']
        
    # Inspect the connection
    if event_type != 'purchase':
            
        person_id = int(record['id1'])
        friend_id = int(record['id2'])
            
        # Check whether to create node
        if (person_id not in unique_id):
            unique_id.add(person_id)
                
            # Create the new person and add to the list
            new_person = Person(person_id)
            person_dict[person_id] = new_person
                
            # Check whether to create node    
        elif (friend_id not in unique_id):
            unique_id.add(friend_id)            
            
            # Create the new person and add to the list
            new_person = Person(friend_id)
            person_dict[friend_id] = new_person
                
            # Add (or delete) the connection
        if event_type == 'befriend':             
            #connection_list.append((person_id,friend_id))
            network.add_edges_from([person_id,friend_id])
        else:
            network.remove_edges_from([person_id,friend_id])
            
            #forward_connection = (person_id,friend_id)
            #backward_connection = (friend_id,person_id)
                
            #if forward_connection in connection_list:
            #    connection_list.remove(forward_connection)
            #if backward_connection in connection_list:
            #    connection_list.remove(backward_connection)
                
    if event_type == 'purchase':
            
        # Extract node information
        person_id = int(record['id'])
            
        purchase = float(record['amount']) 
        timevar = datetime.strptime(str(record['timestamp']),'%Y-%m-%d %H:%M:%S')
            
        # Do we need to add a new person?
        if ( person_id not in unique_id ):
                
            unique_id.add(person_id)
                
            # Create the new person
            new_person = Person(person_id)
                
            # Add the purchase record
            new_person.addPurchase(purchase,timevar)
                
            # Add person to the list
            person_dict[person_id] = new_person
            
        # Existing person
        else:
                
            # Find the person with the index
            new_person = person_dict[person_id]
            
            # Add the purchase record
            new_person.addPurchase(purchase,timevar)
                
                
        # Check for anomaly
        
        # Grab anomaly, mean, and std
        local_mean = person_dict[person_id].mean
        local_std = person_dict[person_id].std
        local_amount = person_dict[person_id].anom_amount
        
        if (local_amount < purchase):            
            
            out_log = strecord
            out_log = out_log[:-1] # remove the \n
            #out_log = out_log[:-1] # remove the bracket
            
            mean_str = ', "mean": "%(mean_val).2f" ' % {"mean_val":local_mean} 
            std_str = ', "sd": "%(sd_val).2f"}' % {"sd_val":local_std} 
            
            out_log = out_log + mean_str
            out_log = out_log + std_str

            return out_log

        else:

            return ""


    else:

        return ""
        
            # print out_log
            
            # write to file!!!!!!
