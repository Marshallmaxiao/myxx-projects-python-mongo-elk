# In[13]:


'''general idea:

impressions and adspends only have brands we partner with, but ecom_sales have every brand that we sold. 
#schedule to start on every Monday morning， search for the last monday - sunday

'''

# In[14]:

# initialize. 
import pymongo
from pymongo import MongoClient
import pandas as pd
import datetime as dt
pd.options.display.max_columns=20
import numpy as np

# connect to MongoDB myxx database
client= MongoClient('xyz,myxx-prod-mongo-rs02.eastus.cloudapp.azure.com:27017,myxx-prod-mongo-rs03.eastus.cloudapp.azure.com:27017/myxx?readPreference=primary&replicaSet=myxxrs01' )
db = client['xyz']

# ref: https://github.com/burnash/gspread and console.developers.google.com 
# Import the Google Sheet file
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import gspread2df as g2d
import pandas as pd
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('xyz15.json', scope)
gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_key("xyz") # open Monthly-Ad-Spending GoogleSheet
worksheet = spreadsheet.worksheet('sheet1')


# In[15]:
# get data from mongo for the last 7 days
def datelist(end_date):
    end_date = dt.datetime(end_date.year,end_date.month,end_date.day)
    start_date = end_date - dt.timedelta(days=7)
    no_days = (end_date-start_date).days
    return( [start_date + dt.timedelta(days=i) for i in range(no_days+1)] )

date_list = datelist(dt.date.today())
coll = db['ecom_sales']
query = {"completedon":{"$gte":date_list[0], "$lte":date_list[-1]}}
excluding = {'cartid':False, 'onsale':False, 'sku':False,'is_default_sku':False, 'default_brand':False, 'is_default_brand':False,
             'retailer':False, 'userid':False, 'price':False}
cursor = coll.find(query,projection=excluding)
ecomsales = pd.DataFrame(list(cursor))
cursor.close()

coll = db['impressions']
query = {"date":{"$gte":date_list[0], "$lte":date_list[-1]}}
excluding = {'scraped_brand':False}
cursor = coll.find(query,projection=excluding)
impressions = pd.DataFrame(list(cursor))  
cursor.close()


# In[16]:

# initialize data
adspend = pd.DataFrame(worksheet.get_all_records())
adspend['year-month'] = pd.to_datetime(adspend['year-month'], format="%Y-%m")
ecomsales["brand"] = ecomsales["brand"].str.lower()
impressions["default_brand"] = impressions["default_brand"].str.lower()
adspend["brand"] = adspend["brand"].str.lower()
[print(df.head(2),'\n' "======================================") for df in [ecomsales,impressions,adspend]]

# In[17]:


# keep brand names consistent across dataframes
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
    elif x in ['sun dry']:
        return('california sun dry')
    else:
        return(x)

ecomsales["brand"] = ecomsales["brand"].apply(lambda x: correct_brand_names(x))


# In[18]:


# full brands and items list
# this is the place if need to change default brands
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

# reverse the full list to "ingredient-brand" like
ingre_to_brands ={}
for brand in brands:
    for item in brands[brand]:
        ingre_to_brands[item]=brand
# In[6]:

# break down family brands to specific subbrand names

def break_down_brands(row):
    if row['brand'] in ['kfrat','barilla',"campbell's","french's",'danone']:
        try:
            return(ingre_to_brands[row['mappedingredient']])
        except:
            return(row['brand'])
    else:
        return(row['brand'])    
     
ecomsales["brand"] = ecomsales.apply(break_down_brands, axis=1)
ecomsales["brand"] = ecomsales["brand"].str.lower()

def break_down_brands(row):
    if row['default_brand'] in ['kfrat','barilla',"campbell's","french's",'danone']:
        try:
            return(ingre_to_brands[row['item']])
        except:
            return(row['default_brand'])
    else:
        return(row['default_brand'])

impressions['default_brand'] = impressions.apply(break_down_brands, axis=1)
impressions['default_brand'] = impressions['default_brand'].str.lower()


# In[19]:


#group to day level
impressions['date'] = impressions['date'].apply(lambda x: dt.datetime.date(x))
daily_brand_impression = impressions.groupby(['date','default_brand'])[['recipeimpressions','cartimpressions','favs','addToCarts','prints','shares']].sum().reset_index()

ecomsales['completedon'] = ecomsales['completedon'].apply(lambda x: dt.datetime.date(x))
daily_brand_ecomsales = ecomsales.groupby(['completedon','brand'])['sales','aggQty'].sum().reset_index()


# In[20]:


# join impression with ecommerce sales
join1 = daily_brand_impression.merge(daily_brand_ecomsales, how='left', left_on=['date','default_brand'], right_on=['completedon','brand'])
join1 = join1.drop(columns=['brand','completedon'])
join1 = join1.rename(index=str, columns={'default_brand':'brand'})

# join adspend
join1['year'] = join1['date'].apply(lambda x: x.year)
join1['month'] = join1['date'].apply(lambda x: x.month)
adspend['year'] = adspend['year-month'].apply(lambda x: x.year)
adspend['month'] = adspend['year-month'].apply(lambda x: x.month)

join2 = join1.merge(adspend, on=['year','month','brand'], how='left')
join2 = join2.drop(columns=['year-month','year','month'])
join2 = join2.rename(index=str, columns={'AdSpend':'MonthlyAdSpend','aggQty':'Ecom_units','sales':'Ecom_sales'})


# In[21]:


df=join2.fillna(0)
df['brand'] = df['brand'].apply(lambda x: 'danone-others' if x=='danone' else x)


# In[22]:



# In[23]:


# calculation

df["Avg Price per Unit"] = df["Ecom_sales"]/df["Ecom_units"]
# correct #  avg_price/unit (if NA, use avg price for the brand)
avgprices = df.groupby('brand')['Avg Price per Unit'].mean().to_dict()
for ind,row in df.iterrows(): #  avg_price/unit (if NA, use avg price for the brand)
    if (row['Avg Price per Unit']>0)==False:
        df.loc[ind, 'Avg Price per Unit'] = avgprices[row['brand']]

df["In-Store Mobile Units Sold"] = (df['addToCarts']-df['Ecom_units']) * 0.25
def exclude_negative(x): # correct negative mobile units (caused by addtoCart>Ecom_units)
    if x>=0:
        return(x)
    else:
        return(0) 
df["In-Store Mobile Units Sold"] = df["In-Store Mobile Units Sold"].apply(lambda x: exclude_negative(x) )
df["In-Store Mobile Revenue"] = df["In-Store Mobile Units Sold"] * df["Avg Price per Unit"] 

df["In-StorePrinted List Units Sold"] = df["prints"] * 0.5
df["In-StorePrinted List Revenue"] = df["In-StorePrinted List Units Sold"] * df["Avg Price per Unit"]

df["Daily Ad Spend"] = df["MonthlyAdSpend"]/30
df["Total Units Sold"] = df["Ecom_units"] + df["In-Store Mobile Units Sold"] + df["In-StorePrinted List Units Sold"]
df["Total Revenue"] = df["Ecom_sales"] + df["In-Store Mobile Revenue"] + df["In-StorePrinted List Revenue"]
df["Daily ROAS"] = df["Total Revenue"]/df["Daily Ad Spend"]
df = df.replace(np.inf, 'NoAdSpend')


# In[24]:


# clean
daily = df


# In[13]:


# daily.to_csv("report_daily.csv" , index=False )


# In[27]:


# update dailly report to MongoDB
client= MongoClient('xyzdapp.azure.com:27017,myxx-prod-mongo-rs03.eastus.cloudapp.azure.com:27017/myxx?readPreference=primary&replicaSet=myxxrs01' )
db = client['xyz']
coll = db['xyz']
olddates = coll.distinct("date")
daily['date'] = pd.to_datetime(daily['date'])

if len(olddates)==0: # if no data in collection with this date range (actually this if statment is useless?)
    print("No data in collection. Will insert search results.")
    coll.insert_many(daily.to_dict(orient='records'))
    print("Added "+str(len(daily))+" rows of new data.")
    
else:    # if collection is not empty, compare and insert new data
    dailydates = daily["date"].drop_duplicates().tolist()
    newdates = [i for i in dailydates if i not in olddates]
    newdata = daily.loc[daily['date'].isin(newdates)]
    if len(newdata)>0:
        coll.insert_many(newdata.to_dict(orient='records'))  # add data without touching the old one
        print("Added "+str(len(newdata))+" rows of new data.")
    else:
        print("No new data found. No new data added.")
        
print('Finished.')


# In[ ]:


#JUNK BELOW


# In[26]:





# In[20]:

'''
# get weekly report

df['date'] = pd.to_datetime(df['date'])
weekly = df.set_index('date').groupby( ['brand',pd.Grouper(freq='W-SAT')] ).sum().reset_index()
weekly['week'] = weekly['date'].apply(lambda x: (x-dt.timedelta(days=6)).strftime('%m/%d/%Y')+" - "+x.strftime('%m/%d/%Y'))

df = weekly
df["Avg Price per Unit"] = df["Ecom_sales"]/df["Ecom_units"]
# correct #  avg_price/unit (if NA, use avg price for the brand)
avgprices = df.groupby('brand')['Avg Price per Unit'].mean().to_dict()
for ind,row in df.iterrows(): #  avg_price/unit (if NA, use avg price for the brand)
    if (row['Avg Price per Unit']>0)==False:
        df.loc[ind, 'Avg Price per Unit'] = avgprices[row['brand']]

df["In-Store Mobile Units Sold"] = (df['addToCarts']-df['Ecom_units']) * 0.25
def exclude_negative(x): # correct negative mobile units (caused by addtoCart>Ecom_units)
    if x>=0:
        return(x)
    else:
        return(0) 
df["In-Store Mobile Units Sold"] = df["In-Store Mobile Units Sold"].apply(lambda x: exclude_negative(x) )
df["In-Store Mobile Revenue"] = df["In-Store Mobile Units Sold"] * df["Avg Price per Unit"] 

df["In-StorePrinted List Units Sold"] = df["prints"] * 0.5
df["In-StorePrinted List Revenue"] = df["In-StorePrinted List Units Sold"] * df["Avg Price per Unit"]

df["Daily Ad Spend"] = df["MonthlyAdSpend"]/30
df["Total Units Sold"] = df["Ecom_units"] + df["In-Store Mobile Units Sold"] + df["In-StorePrinted List Units Sold"]
df["Total Revenue"] = df["Ecom_sales"] + df["In-Store Mobile Revenue"] + df["In-StorePrinted List Revenue"]
df["Daily ROAS"] = df["Total Revenue"]/df["Daily Ad Spend"]
df = df.replace(np.inf, 'NoAdSpend')

weekly = df
weekly = weekly.rename(index=str, columns={'Daily Ad Spend': 'Weekly Ad Spend', 'Daily ROAS':'ROAS'})
weekly = weekly.drop(columns=['date', 'MonthlyAdSpend'])
weekly.head()

'''
# In[22]:


# weekly.to_csv('report_weekly.csv',index=False)


# In[163]:


# update weekly report to MongoDB
# client= MongoClient(xyz.azure.com:27017,myxx-prod-mongo-rs02.eastus.cloudapp.azure.com:27017,myxx-prod-mongo-rs03.eastus.cloudapp.azure.com:27017/myxx?readPreference=primary&replicaSet=myxxrs01' )
# db = client['xyz']
# coll = db['xyz']

# query = {}
# cursor = coll.find(query)
# old = pd.DataFrame(list(cursor))
# cursor.close()

# if len(old)==0:
#     print("No data in collection. Will insert search results.")
#     coll.insert_many(weekly.to_dict(orient='records'))
# else:
#     olddates = old['week'].unique().tolist()
#     newdata = weekly.loc[weekly['week'].isin(olddates)==False]
#     if len(newdata)>0:
#         coll.insert_many(newdata.to_dict(orient='records'))  # add data without touching the old one
#         print("Added "+str(len(newdata))+" rows of new data.")
#     else:
#         print("No new data found. No new data added.")
# print('Finished')


# In[23]:


# JUNK BELOW


# In[19]:


