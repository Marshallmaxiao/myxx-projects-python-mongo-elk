
# coding: utf-8

# In[15]:


'''
marshall@myxx.it
target columns: mappedingredient, name, brand, retailer, default brand, final brand, cart id, user id,sku, price,isonsale, qty, completedon
create sales = price * qty
swap = False if default brand == final brand
'''

'''
### Price differences
05/30/18
The prices of items are still a bit different from the rawcart item report. The function get_price_str needs to be improved.

'''


# In[16]:


# initialize
import pymongo
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import re
# import winsound
pd.options.display.max_columns=99


# In[17]:


# connect to MongoDB, myxx database, and cart-complete collection
client= MongoClient('mongodb://myxx_read:myxx24!myxx24!@myxx-prod-mongo-rs01.eastus.cloudapp.azure.com:27017,myxx-prod-mongo-rs02.eastus.cloudapp.azure.com:27017,myxx-prod-mongo-rs03.eastus.cloudapp.azure.com:27017/myxx?readPreference=primary&replicaSet=myxxrs01' )
db = client['myxx']
collection = db['cart-complete']


# In[18]:


# query
start_date = datetime.strptime("2018-07-16 00:00:15.499000", "%Y-%m-%d %H:%M:%S.%f")
end_date =   datetime.strptime("2018-07-22 23:59:15.499000", "%Y-%m-%d %H:%M:%S.%f")
query = {}
query["ischeckout"] = True
query["$and"] = [{
        u"completedon": {
            u"$gte": start_date 
        }
    },
    {
        u"completedon": {
            u"$lte": end_date 
        }  }]
excluding = {'createdon':False, 'entrystore':False, 'entrystore':False,'itemcart':False, 'itemlist':False, 
             'modifiedon':False, 'originSite':False, 'productcart':False, 'productlist':False, 'type':False,'user':False}
# run query and save as pandas dataframe
cursor = collection.find(query, projection=excluding)
df = pd.DataFrame(list(cursor))
if len(df) >0:
    df['_id'] = df['_id'].astype(str)
    print('Found',len(df),'carts')
else:
    print("No carts found. The program will stop here.")

rawdf = df.copy()
cursor.close()
df = df.drop(columns=['ischeckout'])
df.head(2)


# In[19]:


# unfold lists of raggs to row-by-row basis
# source: https://gist.github.com/jlln/338b4b0b55bd6984f883

def splitDataFrameList(df,target_column):
    ''' df = dataframe to split,
    target_column = the column containing the values to split
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row. 
    The values in the other columns are duplicated across the newly divided rows.
    '''
    def splitListToRows(row,row_accumulator,target_column):
        split_row = row[target_column]
        if(len(split_row)!=0):
            for s in split_row:
                new_row = row.to_dict()
                new_row[target_column] = s
                row_accumulator.append(new_row)

    new_rows = []
    df.apply(splitListToRows,axis=1,args = (new_rows,target_column))
    new_df = pd.DataFrame(new_rows)
    return new_df

df = splitDataFrameList(df,'ragg')
if len(df)==0:
    print("No products(ragg) info. Program will stop here.")


# In[20]:


# better price scraping functions

def get_size(x):
    if re.search(r'\d+\.\d{1,2}', x):
        return re.search(r'\d+\.\d{1,2}', x).group(0)
    elif re.search(r'\d+' , x): 
        return re.search(r'\d+' , x).group(0)
    else:
        None

def get_price_str(x):
    price = 'unknown'
        
    if 'products'in x and x['products'] is not None:
        if 'selectedproduct' in x['products'] and x['products']['selectedproduct'] is not None:
            selectedProduct = x['products']['selectedproduct']
            retailerName = selectedProduct['retailer'] if ('retailer' in selectedProduct) else 'unknown' 
            
            if retailerName == 'harristeeter':
                price = selectedProduct["CurrentPrice"]
            elif retailerName == 'walmart':
                try:
                    price = selectedProduct['RegularPrice']
                except:
                    pass
            elif retailerName in ['kroger','kingsoopers', 'ralphs','fredmeyer'] :
                price = selectedProduct["RegularPrice"]
            elif retailerName == 'shoprite':
                
                if selectedProduct["Sale"] is None:
                    price = selectedProduct['CurrentPrice']
                else:
                    price = selectedProduct['RegularPrice']
         
        else:
            return 'unknown'
    else:
        return 'unknown'
    
    # get sale price
    def getSalePrice(retailerName, selectedProduct):
        
        # for kroger
        if (retailerName in ['kroger','kingsoopers', 'ralphs','fredmeyer'] and 
            selectedProduct['RegularPrice']!= selectedProduct['CurrentPrice']):
            if selectedProduct['CurrentPrice'] != "":
                return selectedProduct['CurrentPrice']
            
        # if there is alternative sale price  
        if 'AlternateSale' in selectedProduct and selectedProduct['AlternateSale'] is not None:
            saleprice = selectedProduct['AlternateSale']['Description1']
            #finalPrice = finalPrice.replace("(",'').replace(")", '').replace("VIC Card Members", '').replace("Single Price Displayed.", "")
            saleprice = re.search(r'\d+\.\d{1,2}', saleprice)
            size = get_size(selectedProduct['Size'])
            if selectedProduct['Brand'] in ['Perdue'] and size and saleprice:   # perdue sale price (in HT) is per unit
                finalPrice = float(size) * float(saleprice.group(0)) # size * unit sale price, if they both exist
                if 'oz' in selectedProduct['Size']:
                    finalPrice = finalPrice * 0.0625  # oz to lb (Perdue uses lb and somtimes oz)
                return finalPrice
            else:
                return saleprice
        
        # if it is on sale
        if 'Sale' in selectedProduct and selectedProduct['Sale'] is not None:
            return selectedProduct["CurrentPrice"]
    
        return None
    
    # decide the final price to return
    price2 = getSalePrice(retailerName, selectedProduct)
    if price2 is not None and price2 != price:
        return price2
    else:
        return price


# In[21]:


# exract item info from each ragg
def split_ragg(df):
    
    def get_attribute(x,attr):
        try:
            return(x['products']['selectedproduct'][attr])
        except:
            return('unknown')
        
    df['mappedingredient'] = df['ragg'].apply(lambda x: x['mappedingredient'])
    df['name'] = df['ragg'].apply(lambda x: get_attribute(x,'Name'))    
    df['aggQty'] = df['ragg'].apply(lambda x: int(x['aggQty']))    
    df['brand'] = df['ragg'].apply(lambda x: get_attribute(x,'Brand') )
    df['retailer'] = df['ragg'].apply(lambda x: get_attribute(x,'retailer') )
    df['onsale'] = df['ragg'].apply(lambda x: get_attribute(x,'OnSale'))
    df['sku'] = df['ragg'].apply(lambda x: get_attribute(x,'Sku'))
    df['raw_price'] = df['ragg'].apply(lambda x: get_price_str(x))
    
    def  get_numeric_price(i):
        if(re.search(r'\$\d+\.\d{1,2}', i)    ): # search for $float 
            return(re.search(r'\$\d+\.\d{1,2}', i).group(0).replace('$','')  )
        elif(re.search(r'\$\d{1,3}', i) ):       # search for $integer
            return(re.search(r'\$\d{1,3}', i).group(0).replace('$','') )
        elif(re.search(r'\d+\.\d{1,2}', i)    ): # search for float
            return(re.search(r'\d+\.\d{1,2}', i).group(0) )
        elif(re.search(r'\d{1,3}', i)         ): # search for integer
            return(re.search(r'\d{1,3}', i).group(0)  )
        else:
            return('unknown')
    
    df['price'] = pd.to_numeric(df['raw_price'].apply(lambda x: get_numeric_price(str(x))), errors='ignore')

    return(df)

df = split_ragg(df)
df['sales'] = pd.to_numeric(df['price'], errors='coerce') * df['aggQty'].fillna(0)
df.head()


# In[22]:


# get df_recipe and df_ingredient from raw data
df_recipe = splitDataFrameList(rawdf,'recipes')[['recipes']]
df_recipe['myxxid'] = df_recipe['recipes'].apply(lambda x: x['myxxid'])
df_recipe['ingredients_edited'] = df_recipe['recipes'].apply(lambda x: x['ingredients_edited'])
df_recipe = df_recipe.drop_duplicates(subset=['myxxid'])

def get_attribute(x, attr):
    try:
        if 'products' in x and 'selectedproduct' in x['products']:
            return(x['products']['selectedproduct'][attr])    
    except:
        return(None)
df_ingredient = splitDataFrameList(df_recipe,'ingredients_edited' )
df_ingredient['default_brand'] = df_ingredient['ingredients_edited'].apply(lambda x: get_attribute(x,'Brand'))
df_ingredient['default_brand'] = df_ingredient['default_brand'].str.lower()
df_ingredient['default_price'] = df_ingredient['ingredients_edited'].apply(lambda x: get_attribute(x,'CurrentPrice'))
df_ingredient['mappedingredient'] = df_ingredient['ingredients_edited'].apply(lambda x: x['mappedingredient'])
df_ingredient['default_sku'] = df_ingredient['ingredients_edited'].apply(lambda x: get_attribute(x,'Sku'))


# In[23]:


# get is_default_brand

df['is_default_brand'] = "none" # set default as no match (no default brand or not matched with recipes)

# for ind, row in df.iterrows():
def is_default_brand(row):
    mapped_ing = row['mappedingredient']
    brand = row['brand']
    if mapped_ing==None or mapped_ing=='unknown' or brand==None or brand=='unknown':
        return("none") 
    
    recipes = row['recipes']
    if recipes != []:
        recipe_ids = [i['myxxid'] for i in recipes]
        related_ingres = df_ingredient.loc[df_ingredient['myxxid'].isin(recipe_ids)]
        related_maps = related_ingres['mappedingredient'].tolist()
        if mapped_ing in related_maps: #in case item does not any mappedingredients in related recipes
            default_brands = related_ingres['default_brand'].tolist()
            if brand in default_brands: #finally compare
                return("yes")
            else:
                return("no")

            
# df['is_default_brand'] = df.apply(is_default_brand,axis=1)


# In[24]:


# get is_default_sku

df['is_default_sku'] = None # set default as no match (no default sku or not matched with recipes)

# for ind, row in df.iterrows():
def is_default_sku(row):
    mapped_ing = row['mappedingredient']
    sku = row['sku']
    if mapped_ing==None or mapped_ing=='unknown' or sku==None or sku=='unknown':
        return("none") 
    
    recipes = row['recipes']
    if recipes != []:
        recipe_ids = [i['myxxid'] for i in recipes]
        related_ingres = df_ingredient.loc[df_ingredient['myxxid'].isin(recipe_ids)]
        related_maps = related_ingres['mappedingredient'].tolist()
        if mapped_ing in related_maps: #in case item is not any mappedingredient in related recipes
            default_skus = related_ingres['default_sku'].tolist()
            if sku in default_skus: #finally compare
                return("yes")
            else:
                return("no")

            
# df['is_default_sku'] = df.apply(is_default_sku, axis=1)


# In[25]:


# find default brand for each cart item if it comes from recipes
df['default_brand'] = None

def get_default_brand(row):
    try:
        mapped_ing = row['mappedingredient']
        recipes = row['recipes']
        recipe_ids = [i['myxxid'] for i in recipes]
        related_ingres = df_ingredient.loc[df_ingredient['myxxid'].isin(recipe_ids)]
        related_maps = related_ingres['mappedingredient'].tolist()
        if mapped_ing in related_maps: #in case item is not any mappedingredient in related recipes
            default_brand = related_ingres.loc[related_ingres['mappedingredient']==mapped_ing]['default_brand'].values[0]
            return(default_brand)
    except:
        return(None)

df['default_brand'] = df.apply(get_default_brand,axis=1)


# In[26]:


# find default sku for each cart item if it comes from recipes
df['default_sku'] = None

def get_default_sku(row):
    try:
        mapped_ing = row['mappedingredient']
        recipes = row['recipes']
        recipe_ids = [i['myxxid'] for i in recipes]
        related_ingres = df_ingredient.loc[df_ingredient['myxxid'].isin(recipe_ids)]
        related_maps = related_ingres['mappedingredient'].tolist()
        if mapped_ing in related_maps: #in case item is not any mappedingredient in related recipes
            default_sku = related_ingres.loc[related_ingres['mappedingredient']==mapped_ing]['default_sku'].values[0]
            return(default_sku)
    except:
        return(None)

df['default_sku'] = df.apply(get_default_sku, axis=1)


# In[27]:


# clean brand names

df["brand"] = df["brand"].str.lower()
def correct_brand_names(x):
    if x in ['dannon', 'dannon oikos','light & fit','activia','horizon','horizon organic','silk','international delight']:
        return('danone')
    elif x in ['sun-maid']:
        return('sunmaid')
    elif x in ["eggland's best", 'egglands best']:
        return('egglands')
    elif x in ["mccormick gourmet", 'mccormick grill mates']:
        return('mccormick')
    elif x in ["ghirardelli", 'ghirardelli chocolate']:
        return('ghirardelli')
    elif x in ['tyson']:
        return('tyson/aidells')
    elif x in ['sun dry']:
        return('california sun dry')
    else:
        return(x)

df["brand"] = df["brand"].apply(lambda x: correct_brand_names(x))
df['default_brand'] = df['default_brand'].apply(lambda x: correct_brand_names(x))


# In[28]:


#compare brand and sku
# tableau will convert True/False to 1/0 which is bad, so I use yes, no and none
def compare_brand(row):
    if row['brand']!=None and row['default_brand']!=None and row['brand']!='unknown':
        is_default = 'yes' if row['brand']==row['default_brand'] else 'no' 
        return(is_default)
    return('none')


def compare_sku(row):
    if row['sku']!=None and row['default_sku']!=None and row['sku']!='unknown':
        is_default = 'yes' if row['sku']==row['default_sku'] else 'no'
        return(is_default)
    return('none')


df['is_default_brand'] = df.apply(compare_brand, axis=1)
df['is_default_sku'] = df.apply(compare_sku, axis=1)


# In[29]:


# clean columns
target_cols= ['_id', 'completedon', 'mappedingredient', 'name', 'brand','default_brand','is_default_brand', 'price', 'aggQty','sales','sku','default_sku', 'is_default_sku', 'retailer', 'onsale', 'userid']
df2 = df[target_cols]
df2 = df2.rename(index=str, columns={'_id':'cartid'})
# winsound.Beep(1000,500)
df2.tail()


# In[32]:


# sync with MongoDB
client= MongoClient('xyzeastus.cloudapp.azure.com:27017,myxx-prod-mongo-rs03.eastus.cloudapp.azure.com:27017/myxx?readPreference=primary&replicaSet=myxxrs01' )
db = client['xyz']
coll = db['xyz']
olddates = coll.distinct("completedon")

if len(olddates)==0: # if no data in collection with this date range
    print("No data in collection. Will insert search results.")
    coll.insert_many(df2.to_dict(orient='records'))
    print("Added "+str(len(df2))+" rows of new data.")
    
else:    # if collection is not empty, compare and insert new data
    df2dates = list(df2["completedon"].drop_duplicates())
    newdates = [i for i in df2dates if i not in olddates]
    newdata = df2.loc[df2['completedon'].isin(newdates)]
    if len(newdata)>0:
        coll.insert_many(newdata.to_dict(orient='records'))  # add data without touching the old one
        print("Added "+str(len(newdata))+" rows of new data.")
    else:
        print("No new data found. No new data added.")
        
print('Finished.')
# winsound.Beep(1500,800)


# In[139]:


# old.to_csv('ecom_sales.csv', index=False)


# END

# In[51]:


# JUNK BELOW #


# In[26]:


