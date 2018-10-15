
# coding: utf-8

# In[1]:


''' 
06/2018 @Marshall 
refer: https://www.elastic.co/blog/a-practical-introduction-to-elasticsearch 
#schedule to start on every Monday morning， search for the last monday - sunday


'''


# In[2]:

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
client= MongoClient('xyzngo-rs02.eastus.cloudapp.azure.com:27017,myxx-prod-mongo-rs03.eastus.cloudapp.azure.com:27017/myxx?readPreference=primary&replicaSet=myxxrs01' )
db = client['xyz']
es = Elasticsearch(['hxyzus.cloudapp.azure.com:9100'])


# In[3]:
#schedule to start on every Monday morning

def datelist(end_date):
    end_date = dt.datetime(end_date.year,end_date.month,end_date.day)
    start_date = end_date - dt.timedelta(days=7)
    no_days = (end_date-start_date).days
    return( [start_date + dt.timedelta(days=i) for i in range(no_days+1)] )
def epoch(date):
    return(int(date.timestamp()*1000))
def epoch_midnight(date):
    return( epoch(date+dt.timedelta(hours=23, minutes=59, seconds=59)) )


# In[4]:


def elastic_query(metric):
    
    def must_body(metric):
        word = words[metric]
        if word in ["recipe", "cartrecipe", "share"]:
            body = [{"match": {"page": {"query": word, "type": "phrase"}}}]
        elif word in ["savefav", "printrecipe"]:
            body = [{"query_string": {"query": word, "analyze_wildcard": True}}]
        elif word == "addrecipetocart":
            body = [{
                                        "query_string": {
                                            "query": "addrecipetocart",
                                            "analyze_wildcard": True
                                        }
                                    }]
        elif word == "ingsToCarts":
            body = [{
                                        "match": {
                                            "atype": {
                                                "query": "addingredienttocart",
                                                "type": "phrase"
                                            }
                                        }
                                    },
                                    {
                                        "match": {
                                            "ing.keyword": {
                                                "query": item,
                                                "type": "phrase"
                                            }
                                        }
                                    }]
        
        return(body)
    
    query = {
                    "query":{
                    "bool": {
                        "must": must_body(metric) + [timerange],
                        "must_not": []
                    }},
                    "size": 0,
                    "_source": {
                        "excludes": []
                    },
                    "aggs": {
                        "aggdata": {
                            "terms": {
                                "field": "myxxid.keyword",
                                "size": 2000,
                                "order": {
                                    "_count": "desc"
                                }
                            }
                        }
                    }
                }

    return(query)


# In[5]:


# full brands and items list
brands={
'Perdue':[
 'chicken breasts',
 'chicken thighs',
 'ground chicken',
 'chicken breasts (boneless skinless)',
 'chicken (whole)',
 'chicken (cooked)',
 'chicken tenders (uncooked)',
 'chicken (thighs)',
 'chicken tenders',
 'chicken strips (breaded)',
 'chicken',
 'chicken nuggets (frozen)',
 'cornish hens',
 'chicken thighs (bone-in) (skin on)'],
'McCormick':[
 'black pepper',
 'crushed red pepper',
 'garlic powder',
 'vanilla extract',
 'chili powder',
 'paprika',
 'paprika (smoked)',
 'sweet paprika',
 'cumin powder',
 'oregano',
 'cinnamon',
 'ground cinnamon',
 'italian seasoning',
 'nutmeg',
 'curry powder',
 'lemon pepper',
 'tumeric powder',
 'oregano (dried)',
 'white pepper',
 'parsley',
 'parsley (dried)',
 'cajun seasoning',
 'taco seasoning',
 'cloves (ground)',
 'sesame seeds',
 'sage',
 'pumpkin spice',
 'gravy powder',
 'black peppercorns',
 'allspice',
 'cinnamon stick',
 'celery seeds',
 'garlic salt',
 'cream of tartar',
 'poppy seeds',
 'seasoning salt',
 'cumin seeds',
 'dill weed (dried)',
 'old bay',
 'almond extract',
 'bacon bits',
 'black pepper (cracked)'],
"Egglands":['eggs'],
    
#French's
"French's Mustard":['brown mustand',
 'dijon mustard',
 'mustard',
 'mustard (gluten-free)',
 'mustard (honey)',
 'mustard (horseradish)',
 'mustard (yellow)',
 'mustard',
 'spicy',
 'mustard (honey dijon)'],
"French's Worcestershire":['worcestershire'],
"Frank's Red Hot":['hot sauce'],
"French's Cripsy Onion's": ['onions (french)', 'crispy onions'],
    
#Barilla
'Barilla Pasta':['pasta',
 'pasta (shell)',
 'pasta (rotoni)',
 'pasta (linguine)',
 'pasta (orecchiette)',
 'whole wheat elbow',
 'pasta (penne',
 'whole wheat)',
 'pasta (bucatini)',
 'pasta (tagliatelle)',
 'pasta (tortiglioni)',
 'pasta (pappardelle)',
 'pasta (rotoni',
 'whole grain)',
 'pasta (orzo, wheat)',
 'pasta (farfalle)',
 'pasta (fettuccine)',
 'pasta (angel hair)',
 'pasta (penne)',
 'pasta (elbow',
 'whole wheat)',
 'pasta (elbow)',
 'pasta (ziti)',
 'pasta (gemelli)',
 'pasta (capellini)',
 'pasta',
 'shells (jumbo)',
 'pasta (ditalini)',
 'pasta (fusilli)',
 'spaghetti (wheat)',
 'spaghetti',
 'lasagna noodles',
 '**_tortellini (cheese)',
 'tortellini (cheese)',
 'tortellini (cheese and spinach)'],
'Barilla Sauce':['pasta sauce',
 'marinara sauce',
 'pesto',
 'basil pesto',
 'red pesto',
 'alfredo sauce',
 'light alfredo sauce',
 'tomato sauce',
 'pizza sauce'],

#Campbell's
'Swanson':['beef broth',
 'beef broth (concentrated)',
 'broth',
 'chicken broth',
 'vegetable broth'],
'Pace':['salsa', 'salsa verde', 'queso'],
"Campbell's Soup":['cheddar cheese soup',
 'chicken noodle soup (canned)',
 'chicken soup',
 'cream of celery soup',
 'cream of chicken soup',
 'cream of chicken soup with herbs',
 'cream of mushroom soup',
 'mushroom soup',
 'soup (golden mushroom)',
 'soup (tomato)',
 'tomato soup (canned)',
 'vegetable soup (canned)'],
 
#Kraft  
'Kraft Cheese': ['american cheese',
 'cheddar cheese',
 'cheese',
 'cheese (american, white)',
 'cheese (american, yellow)',
 'cheese (blend, cheddar, shredded)',
 'cheese (cheddar, shard, shredded)',
 'cheese (cheddar, shredded)',
 'cheese (cheddar, sliced)',
 'cheese (cheddar, white)',
 'cheese (colby-jack)',
 'cheese (jack)',
 'cheese (jack, shredded)',
 'cheese (mexican blend)',
 'cheese (monterey jack)',
 'cheese (monterrey jack, shredded)',
 'cheese (mozzarella & provolone)',
 'cheese (mozzarella)',
 'cheese (mozzarella, sticks)',
 'cheese (parmesan)',
 'cheese (parmesan, shredded)',
 'cheese (pepper jack)',
 'cheese (sliced)',
 'shredded cheese',
 'shredded jack cheese',
 'shredded mozzarella cheese',
 'cheese (mozzarella and parmesan)',
 'cheese (cheddar and asadero)',
 'cheese (cheddar and swiss, shredded)',
 'cheese (cheddar and swiss)'],
'Philadelphia':['cream cheese', 'cream cheese (1/3 less fat)', 'cream cheese (chive and onion)'],

#Danone
"Dannon":[
 'greek yogurt',
 'greek yogurt (blueberry)',
 'greek yogurt (vanilla)',
 'greek yogurt (vanilla, nonfat)',
 'non-fat greek yogurt',
 'non-fat greek yogurt (plain)',
 'vanilla yogurt',
 'yogurt (strawberry)',
 'yogurt (low-fat)',
 'yogurt'],   
"Horizon":['milk',
 'milk (1%)',
 'milk (fat free)',
 'milk (chocolate)',
 'milk (skim)',
 'half & half',
 'heavy whipping cream'],    
"Silk":['almond milk',
 'almond milk (chocolate)',
 'milk (almond, unsweetened)',
 'vanilla almond milk'], 
"International Delight":['coffee cream'],
    
'Ghirardelli':['chocolate',
 'bittersweet chocolate',
 'chocolate',
 'dark chocolate chips',
 'bittersweet chocolate chips',
 'chocolate chips',
 'white chocolate',
 'dark chocolate',
 'baking chocolate',
 'chocolate powder',
 'bittersweet (chocolate bar)',
 'dark chocolate chips',
 'chocolate (semi-sweet bar)',
 'chocolate (dark, chips)',
 'mini chocolate chips',
 'chocolate (unsweetened)',
 'chocolate (unsweetened bar)',
 'white chocolate (melting)',
 'chocolate (milk chips)',
 'chocolate (semi-sweet chips)',
 'chocolate (white chips)',
 'white chocolate morsels',
 'chocolate (bittersweet bar)'],
'SunMaid':['bread (raisin)', 
           'bread (cinnamon raisin)'],
'Aidells':['aidells meatballs (chicken)',
 'meatballs (chicken)',
 'meatballs',
 'aidells meatballs (chicken teriyaki)',
 'aidells meatballs (carmelized onion)',
 'aidells meatballs (chicken italian)',
 'aidells sausage',
 'aidells sausage (andouille)',
 'aidells sausage (chicken and apple)',
 'aidells pork sausage (cajun andouille style)',
 'aidells sausage (chorizo)',
 'aidells sausage (chicken mango jalapeno)',
 'aidells sausage (italian mozzarella)',
 'sausage',
 'chorizo sausage',
 'chicken sausage links',
 'sausage (andouille)',
 'sausage (chorizo)',
 'sausage (italian)'],
'Filippo Berio':['olive oil',
                 'olive oil (extra virgin)'],
'Crisco':['vegetable oil', 
          'vegetable shortening', 'canola oil', 'coconut oil', 'cooking spray'],
'VeeTee':['rice (ready, basmati)', 'rice (ready, long grain)', 'rice (ready, whole grain)']
}


# In[6]:

ingre_to_brands ={}
for brand in brands:
    for item in brands[brand]:
        ingre_to_brands[item]=brand
    

# In[7]:


print("##Full list of sponsoring brands and ingredients:")
for brand in brands:
    print(brand)
    print(" + ".join(brands[brand]))

# In[8]:


# search criteria
date_list = datelist(dt.date.today() - dt.timedelta(days=1))
metrics = ['recipeviews', 'cartviews', 'shares', 'favs', 'addToCarts', 'prints',"ingsToCarts"] # , addToCarts means adding recipe to cart
words = {
    "recipeviews": "recipe",
    "cartviews": "cartrecipe",
    "shares": "share",
    "favs": "savefav",
    "prints": "printrecipe",
    "addToCarts": "addrecipetocart",
    "ingsToCarts":"ingsToCarts"
} #words are key words for elasticsearch


# In[9]:


# search
df = []

for date in tqdm(date_list, desc='date loop'): #tqdm is just to show a progress bar
    
    es = Elasticsearch(['http://elasticuser:server_name.eastus.cloudapp.azure.com:9100'])
    coll = db['recipes']
    timerange =  {    "range": {
                                            "@timestamp": {
                                                "gte": epoch(date),
                                                "lte": epoch_midnight(date),
                                                "format": "epoch_millis"
                                            }
                                        }
                                    }
    
    for brand in tqdm(brands, desc='brand loop', leave=True):
        
        for item in tqdm(brands[brand], desc='item loop', leave=False):
            aggs = { 'recipeviews': [], 'cartviews': [], 'shares': [], 'favs': [], 'addToCarts': [], 'prints': [] }
            reportData = {'date': None, 'item':None, 'default_brand':None, 'scraped_brand':None, 'ingprod':None, 
                          'recipeimpressions': 0, 'cartimpressions': 0, 'favs': 0, 'addToCarts': 0, 'prints':0, 'shares': 0, 'ingsToCarts': 0 }
            
            for metric in metrics:
                resp = es.search(index='logstash-*', body = elastic_query(metric) )
                aggs[metric] = resp['aggregations']['aggdata']['buckets']
                if metric == "ingsToCarts":
                    reportData['ingsToCarts'] = resp['hits']['total']
                else:
                    ids = [i['key'] for i in aggs[metric]]
                    mongo_query = {"myxxid" : {"$in" : ids}, "ingredients_edited.mappedingredient":item}
                    docs = list( coll.find( mongo_query ))
                    df1 = pd.DataFrame(docs)
                    df2 = pd.DataFrame(aggs[metric])
                    
                    try:
                        joined = df1.merge(df2, how='left', left_on='myxxid', right_on='key') #left = left outer join
                    except:
                        joined = pd.DataFrame({'doc_count':[None]})
                        
                    if metric == "recipeviews":
                        reportData['recipeimpressions'] = joined['doc_count'].sum()
                    elif metric == "cartviews":
                        reportData['cartimpressions'] = joined['doc_count'].sum()
                    else:
                        reportData[metric] = joined['doc_count'].sum()

            reportData['item'] = item
            reportData['date'] = date.date()
            reportData['default_brand'] = brand
            df.append(reportData)

df  = pd.DataFrame(df)[['date','item','default_brand','recipeimpressions' ,'cartimpressions','favs','addToCarts','prints','shares','scraped_brand']]

# winsound.Beep(1000,1000)


# In[10]:



# In[12]:

# sync with Mongo
client= MongoClient('xyz,server_name.eastus.cloudapp.azure.com:27017/myxx?readPreference=primary&replicaSet=myxxrs01' )
db = client['xyz']
coll = db['xyz']
query = {"date" : {"$in": date_list}}

cursor = coll.find(query)
old = pd.DataFrame(list(cursor))
cursor.close()

if len(old)==0: # if no data in collection with this date range
    print("No data found for the query time period. Will insert results.")
    df['date'] = pd.to_datetime(df['date'])
    coll.insert_many(df.to_dict(orient='records'))
    print("Added "+str(len(df))+" rows of new data.")
    
else:    # if collection is not empty, compare and insert new data
    old['date'] = pd.to_datetime(old['date'], format="%Y-%m-%d")
    old['date'] = old['date'].apply(lambda x: x.date()) # make the date format consistent on old and df
    olddates = old['date'].tolist()
    olddates = list(set(olddates))
    olditems = old['item'].unique().tolist()
    
    newdata = df.loc[(df['date'].isin(olddates)==False) | (df['item'].isin(olditems)==False) ]  # only add new dats and/or items
    if len(newdata)>0:
        newdata['date'] = pd.to_datetime(newdata['date']) # change the datetime format back to Mongo-friendly
        coll.insert_many(newdata.to_dict(orient='records'))  # add data without touching the old one
        print("Added "+str(len(newdata))+" rows of new data.")
    else:
        print("No new data found. No new data added.")


print('Finished.')

# winsound.Beep(1500,500)


# In[ ]:


# JUNK BELOW #


# In[ ]:


# set uuid for each row as index (turns out not necessay since Mongo will automatically assign _id for each new input)
# import uuid
# df['_id'] = df.apply(lambda row: uuid.uuid4().hex, axis=1)
# df = df.set_index('_id', drop=True)


# In[ ]:



# newdata = df.loc[(df['date'].isin(olddates)==False) | (df['item'].isin(olditems)==False) ]  # only add new dats and/or items
# if len(newdata)>0:
#     coll.insert_many(json.loads(newdata.to_json(orient='records')))  # add data without touching the old one
#     print("Finished updating")
# else:
#     print("No new data found")

# # a smarter way is 1:only query data in the same date range as elasticsearch; 2:upload all elasticsearch results using updateMany() and filter by date and item


# In[ ]:


# remove all data from collection
# coll.delete_many(filter={})


# In[ ]:



# In[ ]:


# # get uuid for each row


# # get old data
    
# # Mongo query 
# coll = db['impressions']
# start_date = date_list[0]
# end_date   =   date_list[-1] + dt.timedelta(hours=23, minutes=59, seconds=59)
# query = {}
# query["$and"] = [
#     {
#         u"date": {
#             u"$gte": start_date 
#         }
#     },
#     {
#         u"date": {
#             u"$lte": end_date 
#         }
#     }
# ]

# # run query and save as pandas dataframe
# cursor = collection.find(query)
# df = pd.DataFrame(list(cursor))
# df['_id'] = df['_id'].astype(str)
# print('Found',len(df),'carts')




# # compare and update new data to MongoDB
# import os
# import pandas as pd
# import pymongo
# import json

# def import_content(filepath):
#     mng_db = mng_client['myxx']
#     collection_name = 'Ecom_Sales'
#     db_cm = mng_db[collection_name]
#     cdir = os.path.dirname(__file__)
#     file_res = os.path.join(cdir, filepath)

#     data = pd.read_csv(file_res)
#     data_json = json.loads(data.to_json(orient='records'))
#     db_cm.remove()
#     db_cm.insert(data_json)

    
    
# def df_to_mongo(df,coll):
#     mng_db = mng_client['myxx']
#     collection_name = 'Ecom_Sales'
#     db_cm = mng_db[collection_name]
#     cdir = os.path.dirname(__file__)
#     file_res = os.path.join(cdir, filepath)

#     data = pd.read_csv(file_res)
#     data_json = json.loads(data.to_json(orient='records'))
#     db_cm.remove()
#     db_cm.insert(data_json)    

# # if __name__ == "__main__":
# #     filepath = 'D://csv//Ecommerce_sales.csv'  #change the csv file path
# #     import_content(filepath)

