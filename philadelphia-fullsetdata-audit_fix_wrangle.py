#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Our task in this exercise has three steps:

- audit the OSMFILE(philadelphia.osm full data) and change the variable 'mapping' to reflect the changes needed to fix 
    the unexpected street types to the appropriate ones in the expected list.
    We have to add mappings only for the actual problems you find in this OSMFILE,
    not a generalized solution, since that may and will depend on the particular area you are auditing.
- write the update_name function, to actually fix the street name.
    The function takes a string with street name as an argument and should return the fixed name
    We have provided a simple test so that you see what exactly is expected
    
-  Our task is also to wrangle the data and transform the shape of the data
into the model as below. The output should be a list of dictionaries
that look like this:

{
"id": "2406124091",
"type: "node",
"visible":"true",
"created": {
          "version":"2",
          "changeset":"17206049",
          "timestamp":"2013-08-03T16:43:42Z",
          "user":"linuxUser16",
          "uid":"1219059"
        },
"pos": [41.9757030, -87.6921867],
"address": {
          "housenumber": "5157",
          "postcode": "60625",
          "street": "North Lincoln Ave"
        },
"amenity": "restaurant",
"cuisine": "mexican",
"name": "La Cabana De Don Luis",
"phone": "1 (773)-271-5176"
}


we should return a dictionary, containing the shaped data for that element.
We have also provided a way to save the data in a file, so that you could use
mongoimport later on to import the shaped data into MongoDB. 

In particular the following things should be done:
- We should process only 2 types of top level tags: "node" and "way"
- all attributes of "node" and "way" should be turned into regular key/value pairs, except:
    - attributes in the CREATED array should be added under a key "created"
    - attributes for latitude and longitude should be added to a "pos" array,
      for use in geospacial indexing. Make sure the values inside "pos" array are floats
      and not strings. 
- if second level tag "k" value contains problematic characters, it should be ignored
- if second level tag "k" value starts with "addr:", it should be added to a dictionary "address"
- if second level tag "k" value does not start with "addr:", but contains ":", you can process it
  same as any other tag.
- if there is a second ":" that separates the type/direction of a street,
  the tag should be ignored, for example:

<tag k="addr:housenumber" v="5158"/>
<tag k="addr:street" v="North Lincoln Avenue"/>
<tag k="addr:street:name" v="Lincoln"/>
<tag k="addr:street:prefix" v="North"/>
<tag k="addr:street:type" v="Avenue"/>
<tag k="amenity" v="pharmacy"/>

  should be turned into:

{...
"address": {
    "housenumber": 5158,
    "street": "North Lincoln Avenue"
}
"amenity": "pharmacy",
...
}

- for "way" specifically:

  <nd ref="305896090"/>
  <nd ref="1719825889"/>

should be turned into
"node_refs": ["305896090", "1719825889"]


"""
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint
import json
import codecs
OSMFILE = "philadelphia.osm"
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
city_name = re.compile(r'[\,\s]+Philadelphia, PA?')
#state_name = 

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Trail", "Parkway",
            "Commons", "Circle", "Highway", "Broadway", "Lane", "Pike", "Run", "South", "Walk", "Way","Road",
            "33", "40", "70"]

# Variable updated based on what we need to fix
mapping = { "St": "Street",
            "St.": "Street",
            "street" : "Street",
            "Rd.": "Road",
            "Rd" : "Road",
            "Ave" : "Avenue",
            "AVE." : "Avenue",
            "Ave." : "Avenue",
            "Blvd": "Boulevard",
            "Blvd." : "Boulevard",
            "Hwy" : "Highway",
            "Ln" : "Lane",
            "ST" : "Street",
            "Garden" : "Garden Street",
            "Spruce" : "Spruce Street",
            "Atreet" : "Street",
            "41st" : "41st Street",
            "8th" : "8th Street",
            "Bigler" : "Bigler Street",
            "Chestnut" : "Chestnut Street",
            "Chippendale" : "Chippendale Street",
            "Cricket" : "Cricket Avenue",
            "Ct" : "Court",
            "Dr" : "Drive",
            "Front" : "Front Street",
            "Hook" : "Hook Road",
            "Maple" : "Maple Street",
            "Market" : "Market Street",
            "Master" : "Master Street",
            "Moore" : "Moore Avenue",
            "Nixon" : "Nixon Street",
            "Pl" : "Place",
            "Reed" : "Reed Street",
            "Ridge" : "Ridge Avenue",
            "Sheffield" : "Sheffield Street",
            "Sreet" : "Street",
            "Sstreet" : "Street",
            "Stiles" : "Stiles Street",
            "StreetPhiladelphia" : "Street",
            "Sts." : "Street",
            "Sycamore" : "Sycamore Street",
            "Vine" : "Vine Street",
            "Walnut" : "Walnut Street",
            "ave" : "Avenue",
            "avenue" : "Avenue",
            "st" : "Street",
            "south" : "South",
            "road" : "Road",
            "rd" : "Road",
            "st." : "Street",
            "king" : "King Street",
            "susquahana" : "Susquehanna"
            
            
           
            }
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

# audit the street names to find the names to be fixed
def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    #for event, elem in ET.iterparse(osm_file, events=("start",)):
    #for _, element in ET.iterparse(osm_file):
    context = ET.iterparse(osm_file,events=('start','end'))
    context = iter(context)
    event, root = context.next()
    for event, element in context:
        if event=='end':
            if element.tag == "node" or element.tag == "way":
                for tag in element.iter("tag"):
                    if is_street_name(tag):
                        audit_street_type(street_types, tag.attrib['v'])
        root.clear()
    pprint.pprint(dict(street_types))
    return street_types

# Update the street names
def update_name(name, mapping):
    # 
    m = street_type_re.search(name)
    #print "m:", m
    if m:
        street_type = m.group()
        if street_type in mapping:
            name = name[:m.start()]+mapping[street_type]
        elif street_type == "08611":
            name = "Cass Street"
        elif street_type == "19067":
            name = "East Trenton Avenue"
        elif street_type == "446-1234":
            name = "Brookline Boulevard"
        elif street_type == "19047":
            name = "East Lincoln Highway"
        
            #print " updated street type:" , updated_street_type
            #name = street_type_re.sub(updated_street_type, name)       
            #print "updated name:", name
    
    return name

# Shape_element function will return a dictionary, containing the shaped data for that element

def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        # YOUR CODE HERE
        for k in element.attrib.keys():
            #print element.attrib
            val = element.attrib[k]
            node["type"] = element.tag
            if k in CREATED:
                if not "created" in node.keys():
                    node["created"] = {}
                node["created"][k] = val
            elif k == "lat" or k == "lon":
                if not "pos" in node.keys():
                    node["pos"] = [0.0, 0.0]
                pos = node["pos"]
                #print pos
                if k == "lat":
                    new_pos = [float(val), pos[1]]
                else:
                    new_pos = [pos[0], float(val)]
                node["pos"] = new_pos
            else:
                node[k] = val
                  
        for tag in element.iter("tag"):
            tag_key = tag.attrib["k"]
            tag_val = tag.attrib["v"]
            if problemchars.match(tag_key):
                continue
            if tag_key.startswith("addr:"):
                if not "address" in node.keys():
                    node["address"] = {}
                addr_key = tag.attrib["k"][len("addr:"):]
                #print "Address Key is:", addr_key
                if lower_colon.match(addr_key):
                    continue
                # Use update name function to fix the street names for tag key "addr:street"
                elif addr_key == "street":
                    if city_name.search(tag_val):
                        print "actual tag_value:", city_name.search(tag_val)
                        node["address"][addr_key] = city_name.sub('',tag_val)
                        print node["address"][addr_key]
                    elif tag_val== "200 Manor Ave. Langhorne, PA 19047":
                        #print "actual tag_value:", tag_val
                        node["address"][addr_key] = "Manor Avenue"
                        print "new tag", node["address"][addr_key]
                    else:
                        #print "actual tag_value:", tag_val
                        node["address"][addr_key] = update_name(tag_val,mapping)
                        #node["address"][addr_key] = tag_val
                else:
                    node["address"][addr_key] = tag_val
            elif lower_colon.match(tag_key):
                node[tag_key] = tag_val
            else:
                node[tag_key] = tag_val
        #if element.tag == "way":
        for tag in element.iter("nd"):
            if not "node_refs" in node.keys():
                node["node_refs"] = []
            node_ref = node["node_refs"]
            val = tag.attrib["ref"]
            node_ref.append(val)
            #print node["node_refs"]
            node["node_refs"] = node_ref
     
        #print "Node or Way:" , node
        return node
    else:
        return None
# Process_map function save the data in a json file format before it can be loaded in Mongo DB

def process_map(file_in, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        #for _, element in ET.iterparse(file_in):
        context = ET.iterparse(file_in,events=('start','end'))
        context = iter(context)
        event, root = context.next()
        for event, element in context:
            if event=='end':
                el = shape_element(element)
                if el:
                    data.append(el)
                        #if pretty:
                            #fo.write(json.dumps(el, indent=2)+"\n")
                        #else:
                            #fo.write(json.dumps(el) + "\n")
                    fo.write(json.dumps(el) + "\n")
            root.clear()
    return data

def test():
    st_types = audit(OSMFILE)
    #assert len(st_types) == 3
    pprint.pprint(dict(st_types))

    #for st_type, ways in st_types.iteritems():
        #for name in ways:
            #better_name = update_name(name, mapping)
            #print name, "=>", better_name

    # NOTE: For running this code on local computer, with a larger dataset, 
    # call the process_map procedure with pretty=False. The pretty=True option adds 
    # additional spaces to the output, making it significantly larger.
    data = process_map('philadelphia.osm', pretty = False)
    
if __name__ == '__main__':
    test()
