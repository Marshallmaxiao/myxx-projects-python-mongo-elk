
# coding: utf-8

# In[23]:

#schedule to start on every Monday morning， search for the last monday - sunday

# initialize. 
from elasticsearch5 import Elasticsearch
import pymongo
from pymongo import MongoClient
import pandas as pd
pd.options.display.max_columns=99
# import winsound
import datetime as dt
from tqdm import tqdm

# connect to MongoDB, myxx database and Elasticsearch server
client= MongoClient('xyz' )
db = client['xyz']
es = Elasticsearch(['xyz0'])


# In[24]:


def datelist(end_date):
    end_date = dt.datetime(end_date.year,end_date.month,end_date.day)
    start_date = end_date - dt.timedelta(days=7)
    no_days = (end_date-start_date).days
    return( [start_date + dt.timedelta(days=i) for i in range(no_days+1)] )
def epoch(date):
    return(int(date.timestamp()*1000))
def epoch_midnight(date):
    return( epoch(date+dt.timedelta(hours=23, minutes=59, seconds=59)) )


# In[25]:


# search criteria
date_list = datelist(dt.date.today() - dt.timedelta(days=1))
aggs = { 'views': [], 'shares': [], 'favs': [] }


# In[26]:


coll = db['recipes']

# search
df = pd.DataFrame()

for date in tqdm(date_list, desc='dates loop'): #tqdm is just to show a progress bar
    
    aggs = { 'views': [], 'shares': [], 'favs': [] } #initialize info container
    
    # get views
    query = {
                  "query": {
                            "bool": {
                                "must": [
                                    {
                                        "query_string": {
                                            "query": "recipe cartrecipe",
                                            "analyze_wildcard": True
                                        }
                                    },
                                    {
                                        "range": {
                                            "@timestamp": {
                                                "gte": epoch(date),
                                                "lte": epoch_midnight(date),
                                                "format": "epoch_millis"
                                            }
                                        }
                                    }
                                ],
                                "must_not": []
                            }
                        },
                        "size": 0,
                        "_source": {
                            "excludes": []
                        },
                        "aggs": {
                            "aggdata": {
                                "terms": {
                                    "field": "myxxid.keyword",
                                    "size": 100,
                                    "order": {
                                        "_count": "desc"
                                    }
                                }
                            }
                        }}
    
    resp = es.search(index='logstash-*', body = query )
    aggs['views'] = resp['aggregations']['aggdata']['buckets']
    
# get shares
    query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "query_string": {
                                            "query": "share",
                                            "analyze_wildcard": True
                                        }
                                    },
                                    {
                                        "range": {
                                            "@timestamp": {
                                                "gte": epoch(date),
                                                "lte": epoch_midnight(date),
                                                "format": "epoch_millis"
                                            }
                                        }
                                    }
                                ],
                                "must_not": []
                            }
                        },
                        "size": 0,
                        "_source": {
                            "excludes": []
                        },
                        "aggs": {
                            "aggdata": {
                                "terms": {
                                    "field": "myxxid.keyword",
                                    "size": 100,
                                    "order": {
                                        "_count": "desc"
                                    }
                                }
                            }
                        } }   

    resp = es.search(index='logstash-*', body = query )
    aggs['shares'] = resp['aggregations']['aggdata']['buckets']

# get favs
    query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "query_string": {
                                            "query": "savefav",
                                            "analyze_wildcard": True
                                        }
                                    },
                                    {
                                        "range": {
                                            "@timestamp": {
                                                "gte": epoch(date),
                                                "lte": epoch_midnight(date),
                                                "format": "epoch_millis"
                                            }
                                        }
                                    }
                                ],
                                "must_not": []
                            }
                        },
                        "size": 0,
                        "_source": {
                            "excludes": []
                        },
                        "aggs": {
                            "aggdata": {
                                "terms": {
                                    "field": "myxxid.keyword",
                                    "size": 100,
                                    "order": {
                                        "_count": "desc"
                                    }
                                }
                            }
                        }
                    }

    resp = es.search(index='logstash-*', body = query )
    aggs['favs'] = resp['aggregations']['aggdata']['buckets']    

# collect
    ids =  [i['key'] for i in aggs['views']] + [i['key'] for i in aggs['shares']] + [i['key'] for i in aggs['favs']] 
    ids = list(set(ids)) # drop duplicates
    docs = list( coll.find( {"myxxid" : {"$in" : ids}} ))
    docs = pd.DataFrame(docs)
    df_views = pd.DataFrame(aggs['views'])
    df_shares = pd.DataFrame(aggs['shares'])
    df_favs = pd.DataFrame(aggs['favs'])
    
    df_views = df_views.rename(index=str, columns={'doc_count':'views'})
    df_shares = df_shares.rename(index=str, columns={'doc_count':'shares'})
    df_favs = df_favs.rename(index=str, columns={'doc_count':'favs'})
    

    if len(df_views) == 0:
        df_views = pd.DataFrame({'key':None, 'views':None},index=[0])
    if len(df_shares) == 0:
        df_shares = pd.DataFrame({'key':None, 'shares':None},index=[0])
    if len(df_favs) == 0:
        df_favs = pd.DataFrame({'key':None, 'favs':None},index=[0])
        
    data = df_views.merge(df_shares, how='left', on='key').merge(df_favs, how = 'left', on='key')
#     data = merge_data(df_views,df_shares,df_favs)
    joined = docs.merge(data, how='left', left_on='myxxid', right_on='key')
    #joined.sort_values('views', ascending=False)
    
    def get_mapped_ingredients(x):
        try:
            return([i['mappedingredient'] for i in x])
        except:
            return(None)
    
    joined['mappedingredients'] = joined['ingredients_edited'].apply(lambda x: get_mapped_ingredients(x))
    
# clean and export
    final = joined[['myxxid', 'name', 'views','shares', 'favs', 'mappedingredients', 'format', 'sourcename']]
    final['date'] = date
    final = final.fillna(0)
    df = df.append(final)

# winsound.Beep(1000,1000)

df = df.reset_index(drop=True)

df.head()


# In[27]:


# df.to_csv('recipe_views.csv', index=False)


# In[28]:


# upload to Mongo
client= MongoClient('xyzprimary&replicaSet=myxxrs01' )
db = client['xyz']
coll = db['xyz']
olddates = coll.distinct("date")

if len(olddates)==0: # if no data in collection with this date range
    print("No data in collection. Will insert search results.")
    coll.insert_many(df.to_dict(orient='records'))
    print("Added "+str(len(df))+" rows of new data.")
    
else:    # if collection is not empty, compare and insert new data
    dfdates = df["date"].drop_duplicates().tolist()
    newdates = [i for i in dfdates if i not in olddates]
    newdata = df.loc[df['date'].isin(newdates)]
    if len(newdata)>0:
        coll.insert_many(newdata.to_dict(orient='records'))  # add data without touching the old one
        print("Added "+str(len(newdata))+" rows of new data.")
    else:
        print("No new data found. No new data added.")
        
print('Finished.')


# In[7]:


# search recipe by list of mappedingredients
# ingredients = [ 'salt' ,'brisket']
# def f(x):  # in case a recipe has empty mappedingredients
#     return [] if type(x) is not list else x

# df['mappedingredients'] = df['mappedingredients'].apply(lambda x: f(x))
# results = df.loc[ df['mappedingredients'].apply(lambda x: set(ingredients).issubset(x)) ]
# results


# In[ ]:


# JUNK BELOW


# In[89]:




# In[ ]:
