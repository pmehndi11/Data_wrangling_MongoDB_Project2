#!/usr/bin/env python
"""
Use an aggregation query to answer the following question.

What are the biggest top 5 fast food places  ( amenity = 'fast food' counts by Christian religion)  in our philly collection.


The 'make_pipeline' function creates and returns an aggregation pipeline
that can be passed to the MongoDB aggregate function. 
The aggregation pipeline should be a list of one or more dictionary objects.
Our code will be run against a MongoDB instance on the local machine.

"""

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    # complete the aggregation pipeline
    pipeline = [{"$match" : {"amenity" : "fast_food"}},
                 {"$group" : {"_id" : "$name",
                              "count" : {"$sum" : 1}}},
                {"$sort" : {"count" : -1}},
                {"$limit" : 7}]
    return pipeline

def aggregate(db, pipeline):
    result = db.philly.aggregate(pipeline)
    return result

if __name__ == '__main__':
    # The following statements will be used to test our code.
    
    db = get_db('philly')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    import pprint
    for doc in result:
        pprint.pprint(doc)
