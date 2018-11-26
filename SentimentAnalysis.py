# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 11:30:22 2018

@author: Siyan
"""

import requests, json, time, datetime, nltk, os
from sys import platform
import pandas as pd
from requests_oauthlib import OAuth1
from textblob import TextBlob
from nltk.tokenize import TweetTokenizer
from pymongo import MongoClient





#%%  define functions

def Streaming (params, process_time=10, store_local= True):
    # stream data form twitter and save raw file in json format
    # params (dict): keywords to track; eg. {'track':'Trump', 'lang': 'en'}
    # process_time (int): how many seconds to stream
    # write_method (str): 'w' to overwrite old data; 'a' append data at end
    # store_local (bool): True to store raw_cumulative_file only locally, False to store only at Mongodb 
    raw_temp_file= open(temp_file_name, 'w', encoding='utf8') # clean temp file
    raw_temp_file.close()
    
    start=time.time()
    global reconnect_counter
    # ignore all conection errors and restart stream
    while True:
        try:
            print ('Streaming {}s'.format(process_time))
            response= requests.get(url, params=params, auth=auth, stream=True)
            r= response.iter_lines(chunk_size=1) # return byte objects; generator
            while time.time() - start < process_time:
                tweet=next(r).decode('UTF-8')
                if tweet != '': # filter out empty lines
                    data = json.loads(tweet)
                    if 'text' in data.keys(): # validation 

                        # get full text, quote, retweet if truncated
                        # if NO comment when retweet, 'text' and 'retweet' field agree with each other
                        # if comment when retweet, comment is called 'text' and original tweet is 'quote', no 'retweet'
                        quote_status= data['is_quote_status'] # bool
                        if quote_status== True:
                            quote_truncated= data['quoted_status']['truncated'] # bool
                            if quote_truncated== True:
                                quote= data['quoted_status']['extended_tweet']['full_text']
                            else: quote= data['quoted_status']['text']
                        else: quote= None

                        if 'retweeted_status' in data.keys():
                            text= None
                            retweeted_truncated= data['retweeted_status']['truncated'] # bool
                            if retweeted_truncated== True:
                                retweeted_text= data['retweeted_status']['extended_tweet']['full_text']
                            else: retweeted_text= data['retweeted_status']['text']
                        else:
                            retweeted_text= None
                            text_truncated= data['truncated'] # bool
                            if text_truncated== True:
                                text= data['extended_tweet']['full_text']
                            else: text= data['text']


                        # "collected_at" is local/computer time
                        # times are all in UTC +0
                        created_at_format= "%a %b %d %H:%M:%S %z %Y"
                        field= {"collected_at": datetime.datetime.utcnow().isoformat(sep=' ', timespec= 'seconds'),
                                "user_created_at": datetime.datetime.strptime(data['user']['created_at'], created_at_format)\
                                .isoformat(sep=' ', timespec= 'seconds'),
                                "user_name": data['user']['name'],
                                "user_screen_name": data['user']['screen_name'],
                                "user_lang": data['user']['lang'],
                                "text": text,
                                "retweeted_text": retweeted_text,
                                "quote": quote,
                                "user_location": data['user']['location']}
                        if store_local== True:
                            # encoding is platform dependent if not specified
                            with open(raw_file_name, 'a', encoding='utf8') as raw_cumulative_file:
                                raw_cumulative_file.write(json.dumps(field)) # "json dumps" make a better format
                                raw_cumulative_file.write('\n')
                        
                        with open(temp_file_name, 'a', encoding='utf8') as raw_temp_file: 
                            raw_temp_file.write(json.dumps(field)) # raw_temp is kept in any cases
                            raw_temp_file.write('\n')
            break
        except KeyboardInterrupt: # allow KeyboardInterrupt
            raise KeyboardInterrupt()
        except:
            reconnect_counter+=1
            print('Error! Reconnect in 3s...')
            time.sleep(3)
            continue # start at the next iteration
    response.close()






def AnalyzeData(analysis_method= 'afinn', store_local= True, geo_filter= True):
    # load json; upload to MongoDB; sentiment analysis (importance: text > retweeted_text > quote); save processed data to csv
    # analysis_method (str): 'afinn' to match afinn-165 file; 'textblob' to use <TextBlob(x).sentiment>
    # store_local (bool): True to store processed_file only locally, False to store only at Mongodb
    # geo_filter (bool): True to keep tweets only with valid location
    df= pd.read_json(temp_file_name, orient='records', lines=True)
    if df.empty:
        raise ValueError('Temp file "{}" is empty. Nothing is collected during last batch. Increase "process_time" value.'\
                         .format(temp_file_name))

    if store_local== False: # upload raw data to MongoDB
        client = MongoClient(mongoport)
        db = client[db_name] # create a database <Twitter>
        raw_cumulative = db[raw_file] # create a collection/file <raw_file> inside database
        raw_cumulative.insert_many(df.to_dict('records')) 

    if geo_filter== True:
        df= df[df['user_location'].isin(df_city_state.tolist())] # keeps tweets only with valid location
        geo= df['user_location'].str.split(', ', expand=True)
        df= df.assign(user_city= geo.iloc[:,0], user_state= geo.iloc[:,1])
        df['user_location']= None # keep 'user_location', 'user_city', and 'user_state' columns for both <geo_filter> statues
    else: df= df.assign(user_city= None, user_state= None)

    # select text to analyze, importance (content_category): text > retweeted_text > quote
    for index, row in df.iterrows():
        if (row.text is not None) and (row.text== row.text): # not None and not NaN
            df.loc[index, 'content']= row.text
            df.loc[index, 'content_category']= "text"
        elif (row.retweeted_text is not None) and (row.retweeted_text== row.retweeted_text):
            df.loc[index, 'content']= row.retweeted_text
            df.loc[index, 'content_category']= "retweet"
        elif (row.quote is not None) and (row.quote== row.quote):
            df.loc[index, 'content']= row.quote
            df.loc[index, 'content_category']= "quote"
        else: raise ValueError('Can not analyze. Invalid data in "text" or "retweeted_text" or "quote" in temp file.')
    df.drop(['text', 'retweeted_text', 'quote'], axis=1, inplace= True)


    if analysis_method=='textblob':
        # polarity/sentiment range [-1.0, 1.0]
        # subjectivity range [0.0, 1.0] where 0.0 is very objective and 1.0 is very subjective
        sentiment_analysis= df['content'].apply(lambda x: TextBlob(x).sentiment).tolist()
        df['polarity'], df['subjectivity']= zip(*sentiment_analysis)
        df= df[df['polarity'] != 0.0] # filter out neutral comments

    elif analysis_method== 'afinn':
        token= TweetTokenizer(strip_handles=True, reduce_len=True)
        lemm = nltk.stem.WordNetLemmatizer()

        # prepare stop words
        slist=['rt'] # remove specific words
        stopwords= nltk.corpus.stopwords.words('english')
        stopwords= stopwords + slist

        # tokenize, remove stopwords, lemmatize, find afinn score and match words for each tweet
        text= df['content'].str.lower().tolist()
        emotion,keywords=[],[]
        for i, t in enumerate(text): #loop over each tweet
            emo,kwd=[],[]
            text[i]= [lemm.lemmatize(word) for word in token.tokenize(t) if word not in stopwords]
            for w in text[i]: # loop over words in each tweet
                if w in term: # term: list of words in afinn file
                     emo.append(term_score[w])
                     kwd.append(w)
                else:
                     emo.append(0) # 0 if no match
            emotion.append(sum(emo))
            keywords.append(','.join(kwd))

        df=df.assign(sentiment=emotion, matched_words=keywords)
        df= df[df['matched_words'] != ''] # filter out rows with no matched word
    else:
        print('Error with analysis_method!')
        return None

    # store processed data locally or to MongoDB
    if store_local== True:
        header_switch= False # create header only for first batch
        if not os.path.isfile(processed_file_name):
            header_switch= True
        df.to_csv(processed_file_name, index=False, header=header_switch, mode= 'a') # append data to csv
    else:
        client = MongoClient(mongoport)
        db = client[db_name] # create a database <Twitter>
        processed = db[processed_file] # create a collection/file <processed_file> inside database
        processed.insert_many(df.to_dict('records'))
    return df # only return last batch for review





def ProcessControl (params, process_time= 30, sleep_time= 30, store_local= True, analysis_method= 'afinn', geo_filter= True):
    # params (dict): keywords to track; eg. {'track':'Trump', 'lang': 'en'}
    # process_time (int): how many seconds to stream
    # sleep_time (int): how many seconds to pause
    # store_local (bool): True to store raw_cumulative and processed file only locally, False to store only at Mongodb
    # analysis_method (str): 'afinn' to match afinn-165 file; 'textblob' to use <TextBlob(x).sentiment>
    user_command='r'
    while user_command=='r': # ask user to repeat the process
        hour= input('How many hours to stream?\t')
        if hour=='':
            hour= 12
        else:
            hour= float(hour)
        batch= round(hour*60*60/(process_time + sleep_time))
        print('\n--------------------------')
        print('streaming length per batch: {}s'.format(process_time))
        print('sleeping/pause length per batch: {}s'.format(sleep_time))
        print('total length: {}h'.format(hour))
        print('total number of batch: {}\n'.format(batch))

        global reconnect_counter
        reconnect_counter=0
        counter=1
        if batch<= 0: batch=1
        while counter <= batch:
            print('Current batch: {}/{}'.format(counter, batch))
            Streaming(params= params, process_time= process_time, store_local= store_local)
            print('Done! Total reconnected {} times'.format(reconnect_counter))
            print('Get sentiment score')
            df= AnalyzeData(analysis_method= analysis_method, store_local= store_local, geo_filter= geo_filter)
            print('Sleep {}s...\n'.format(sleep_time))
            for s in range(sleep_time): time.sleep(1)
            counter+=1

        print(df.head())
        print('\nLast batch review:')
        print('Shape: {}'.format(df.shape))
        print('Current tracking keyword is "{}"'.format(params['track']))
        print('Avg. Sentiment: {}'.format(df['sentiment'].sum()/len(df)))
        print('Total reconnected {} times'.format(reconnect_counter))
        print('--------------------------')
        user_command= input('All Done! Enter [r] to repeat?\t')






#%% prepare collection and control

# Tweeter API credential
ckey="WrPEiGACszN0gFii0HjZce7Ml"
csecret="MX41ldSo9U8A8qxOVJGdZE9eSaGlnayaT30XHlFFiplOYXLUaW"
atoken="4822491025-XrAoSNCMu26n4UsviYwZ9syPHFbiTkLrJAD6oKN"
asecret="IPMlkJs6TpXahO9jojwb5HlNBGIA4r9fUrNBJH3iKabbM"
mongoport= 'mongodb://18.207.108.2:31156/'
auth = OAuth1(ckey, csecret, atoken, asecret) # OAuth 1.0a
url= 'https://stream.twitter.com/1.1/statuses/filter.json' # do Not need to change

# exact matching of phrases is not supported in API
# case sensitive, commas as logical ORs, spaces are equivalent to logical ANDs
# language= en will only stream Tweets detected to be in the English language
params={'track':'Trump', 'language': 'en'}



if __name__ == '__main__':
    # import city, state list; source https://github.com/grammakov/USA-cities-and-states
    df_location= pd.read_csv('Supplemental Files/us_cities_states_counties.csv', header=0, sep='|')
    df_city_state= df_location['City'] + ', ' + df_location['State short'] # new format [city, state]
    
    # import afinn file
    afinnfile = open('Supplemental Files/AFINN-en-165.txt')
    term_score = {}
    for l in afinnfile:
        term, score  = l.split("\t")
        term_score[term] = int(score)
    term= term_score.keys()
    afinnfile.close()
    
    # ask user to change params
    change_switch= input('Current tracking keyword is "{}". Press [c] to change.\t'.format(params['track']))
    if change_switch== 'c':
        params['track']= input('Enter new keywords (case sensitive). Use comma as logical OR, space as logical AND.\t')  
      
        
     
# setup file names that reflect tracking words
raw_file= 'tweets_raw_{}'.format(params['track'])
raw_file_name= raw_file + '.json'
processed_file= 'tweets_processed_{}'.format(params['track'])
processed_file_name= processed_file + '.csv'
temp_file_name= 'tweets_temp_{}.json'.format(params['track'])
db_name= 'Twitter'



#%% Start!


# =============================================================================
#  process_time (int): length of streaming during each batch
#  sleep_time (int): length of sleep/pause during each batch
#  geo_filter (bool): True to generate 'processed_file' after filtering raw file based on US city name list
#  store_local (bool): True to store 'processed_file' only locally, False to store only at Mongodb
#  analysis_method (str): 'afinn' to match afinn-165 file; 'textblob' to use <TextBlob(x).sentiment> method
#
#  if tracking word is not hot topic (recieved nothing during the batch), increase process_time
#  check <db_name>, Tweeter API credential, and <mongoport> before start
#  geo_filter= True: 'user_location' is empty, 'user_city' and 'user_state' are filled
#  geo_filter= False: 'user_location' is filled or empty, 'user_city' and 'user_state' are empty
#
#  'processed_file' has reduced amount of record compare to 'raw_file'. 
#  (eg. 'today is sunny' has no setiment, so entire record is removed; without valid location, record may also be removed)
#
#  limited accuracy on sarcasm...
# =============================================================================

# when first time use this script, download needed files
if platform == "linux" or platform == "linux2":
    # Linus system: check whether files are in parent directory
    if 'nltk_data' not in os.listdir(os.path.dirname(os.getcwd())): 
        nltk.download('stopwords')
        nltk.download('wordnet')

if __name__ == '__main__':
    ProcessControl(params, process_time= 30, sleep_time= 30, geo_filter= False, store_local= False)


# Janasena,janasena






