#
#
# Packages that need to be imported
#
#


# A reader for json type entries/files
import json

# Our Node Class
from classes import Person

# The networkx Graph Class
import networkx as nx

# Our Functions
from functions import *

# To handle io
import sys


# Grab the filenames/locations from the command line
#
# (there should be some checks on this ideally)
#
batch_file = sys.argv[1]
stream_file = sys.argv[2]
out_file = sys.argv[3]


# Bogus Values
D = -1000
T = -1000

# Open each line in the file and close upon finish
connection_list = []
unique_id = set()
person_dict = {}
with open(batch_file) as f:
    
    # Grab information in the first-line
    first_record = json.loads(f.readline())
    D = int(first_record['D'])
    T = int(first_record['T'])
    
    # Loop over the remaing lines that contain the real data entries
    for line in f:
        
        
        # Try block
        try:
        
            # Grab the entry information
            record = json.loads(line)
    
            parse_batch_record(record,person_dict,connection_list,unique_id)
        
        except ValueError:
            
            print "skipping line"
    

my_network = nx.Graph()
my_network.add_edges_from(connection_list)            

node_list = nx.nodes(my_network)
#
# There is probably a more slick way of performing the computation,
# but I am going to use brute force due to lack of time
#
# Loop over nodes
#
# (This loop no longer works since node_list is not returning a list of ints)
#
for node in node_list:
    
    # Grab cutoff-level neighbors
    neighbors = nx.single_source_shortest_path_length(my_network, node, cutoff=D)
    
    # Find the anomaly amount
    compute_anomaly_amount(node,neighbors,person_dict,T)
    




g = open(out_file,'a')
with open(stream_file) as f:
    
    # Loop over the lines that contain the real data entries
    for line in f:
        
        # Grab the entry information
        try: 
            
            record = json.loads(line)
            
            out_str = parse_stream_record(record,person_dict,unique_id,line,my_network)

            if len(out_str)>0:

                g.write(out_str)
            
        except ValueError:
            
            print "skip line"
            # Something was wrong with the event entry...

#g.write("\n")
g.close()
