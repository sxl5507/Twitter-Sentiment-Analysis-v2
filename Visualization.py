# -*- coding: utf-8 -*-
"""
Created on Mon Jun 11 21:02:07 2018

@author: Siyan
"""
#from SentimentAnalysis import mongoport, db_name # automate the process but slower
from pymongo import MongoClient
import datetime, time, pytz, json
import pandas as pd
from dateutil import tz
from sys import platform

import plotly, re # plotly version 2.7.0
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.dashboard_objs as dashboard



# =============================================================================
# Check following notes and parameters before start!

# SentimentAnalysis.py file MUST be placed in the same folder
# Plotly account user name should be in format n of letter plus n of numbers (eg. abc000), otherwise modify <fileId_from_url> function
# All time showed in Plotly is in <tmzone>, user input local time is automatically converted to UTC for querying purpose
# Text table can be filtered by time range and sentiment range, check <TablePlot> function


tmzone= 'America/New_York' # user's timezone, check full TZ list at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
groupby_freq= 'hour' # time series plot groupby 'month', 'day' or 'hour'
update_freq= 75 # in minutes (no less than 72), freq to retrieve data from MongoDB
color= ['#F4D03F', '#EC7063', '#58D68D'] # color for 'Neutral','Negative', and 'Positive' sentiment

# case sensitive, change <tracklist> to decide which database collection you want to open
# they should be the same words when you run SentimentAnalysis.py
tracklist= ['Trump','Hillary'] # must be a list
user= 'sxl5507' # Plotly user name
key= 'HxccyaYsnHquLoYutlK6' # Plotly API key
mongoport= 'mongodb://18.207.108.2:31156/' # Mongodb server address
db_name= 'Twitter' # database name
# =============================================================================












#%% setting: count and avg score for each type of sentiment, using ALL data

# validation
if len(tracklist)==0: raise ValueError('No empty <tracklist>!')
if len(tracklist)>2: raise ValueError('You need to modify <fill_percent> in <MakeBoard> function for a good layout')
plotly.tools.set_credentials_file(username= user, api_key= key)# connect to Plotly
client = MongoClient(mongoport)
db = client[db_name] # look up a database <Twitter>


pipe_neu = [{'$match': {'sentiment': 0}},
            {'$group': {'_id': 'null', 'count': {'$sum': 1}, 'avg': {'$avg': '$sentiment'}}}]
pipe_neg = [{'$match': {'sentiment': {'$lt': 0}}},
            {'$group': {'_id': 'null', 'count': {'$sum': 1}, 'avg': {'$avg': '$sentiment'}}}]
pipe_pos = [{'$match': {'sentiment': {'$gt': 0}}},
            {'$group': {'_id': 'null', 'count': {'$sum': 1}, 'avg': {'$avg': '$sentiment'}}}]


# setting: count and avg score for each type of sentiment, group by <groupby_freq>; returned time in <tmzone>
group= {'year':{'$year': {'date': '$collected_at', 'timezone': tmzone}},
       'month': {'$month': {'date': '$collected_at', 'timezone': tmzone}}}
if groupby_freq== 'month': group= group
elif (groupby_freq== 'day') or (groupby_freq== 'hour'):
    group['day']= {'$dayOfMonth': {'date': '$collected_at', 'timezone': tmzone}}
    if groupby_freq== 'hour': group['hour']= {'$hour': {'date': '$collected_at', 'timezone': tmzone}}
else: raise ValueError('wrong groupby_freq')
pipe_avg_time= [{'$group': {'_id': group, 'count': {'$sum': 1}, 'avg': {'$avg': '$sentiment'}}},
                {'$sort': {'_id': 1}}]
pipe_neg_time= [{'$match': {'sentiment': {'$lt': 0}}},
              {'$group': {'_id': group, 'count': {'$sum': 1}, 'avg': {'$avg': '$sentiment'}}},
              {'$sort': {'_id': 1}}]
pipe_pos_time= [{'$match': {'sentiment': {'$gt': 0}}},
              {'$group': {'_id': group, 'count': {'$sum': 1}, 'avg': {'$avg': '$sentiment'}}},
              {'$sort': {'_id': 1}}]






#%% filter data from MongoDB and make a pie plot
def PiePlot(trackword):
    # trackword (str): used for opening correct daatabase collection and naming the chart (eg."Sentiment Analysis for <trackword>")

    # prepare data for pie plot
    print('Pie Chart: current track word is "{}"'.format(trackword))
    print('Pie Chart: querying neutral sentiment')
    processed= db['tweets_processed_{}'.format(trackword)]
    neu= list(processed.aggregate(pipe_neu, allowDiskUse= True))[0]
    print('Pie Chart: querying negative sentiment')
    neg= list(processed.aggregate(pipe_neg, allowDiskUse= True))[0]
    print('Pie Chart: querying positive sentiment')
    pos= list(processed.aggregate(pipe_pos, allowDiskUse= True))[0]
    neu_ct= neu['count']
    neg_ct= neg['count']
    pos_ct= pos['count']
    neu_avg= round(neu['avg'], 2)
    neg_avg= round(neg['avg'], 2)
    pos_avg= round(pos['avg'], 2)

    print('Pie Chart: ploting')
    pie = go.Pie(labels=['Neutral','Negative','Positive'], values=[neu_ct, neg_ct, pos_ct],
                marker=dict(colors=color,line=dict(color='#797D7F', width=2)),
                # use '<br>' to show in next line
                text= ['avg sentiment score: '+str(neu_avg)+'<br>'+'total records: '+'{0:,}'.format(neu_ct),
                       'avg sentiment score: '+str(neg_avg)+'<br>'+'total records: '+'{0:,}'.format(neg_ct),
                       'avg sentiment score: '+str(pos_avg)+'<br>'+'total records: '+'{0:,}'.format(pos_ct)],
                textinfo= 'label+percent', showlegend= False, hoverinfo= 'text')

    margin= dict(l=55,r=55,t=10,b=10)
    pie_layout= go.Layout(margin= margin)
    pie_figure= go.Figure(data= [pie], layout=pie_layout)
    pie_url= py.plot(pie_figure, auto_open=False, filename='pie_{}'.format(trackword), fileopt='overwrite')
    print('Done!\n')
    return pie_url






# filter data from MongoDB and make a time series plot
def TimeSeries (tracklist):
    # tracklist (list of str): used for opening correct database collection,
    #                          and labling and plot several datasets on single chart with different line types

    # Plotly will convert all uploaded time to UTC based on machine time zone, can't be disabled
    # therefore time in MongoDB need to be converted twice:
    # 1. if <tmzone> is set to EST, Mongodb UTC time will be converted to EST and then recieved by Python
    # 2. recieved EST time will be TREATED AS UTC time and converted into local machine timeone, then sent to Plotly


    border_col= '#A6ACAF'
    line_type= ['solid', 'dot', 'dash', 'longdash', 'dashdot', 'longdashdot']

    data=[]
    for track_i, key in enumerate(tracklist):
        print('Time Series: current track word is "{}"'.format(key))
        print('Time Series: querying average sentiment')
        processed= db['tweets_processed_{}'.format(key)]
        avg_df= {'timeline':[], 'avg_score':[], 'count':[]}
        avg_time= processed.aggregate(pipe_avg_time, allowDiskUse= True)
        for v in avg_time:
            y, m, d, h= v['_id']['year'], v['_id']['month'], v['_id']['day'], v['_id']['hour']
            avg_df['timeline'].append(datetime.datetime(y,m,d,h,tzinfo=pytz.utc).astimezone(tz.tzlocal()))
            avg_df['avg_score'].append(round(v['avg'], 2))
            avg_df['count'].append(v['count'])

        print('Time Series: querying negative sentiment')
        neg_df= {'timeline': [], 'avg_score': [], 'count':[]}
        neg_time= processed.aggregate(pipe_neg_time, allowDiskUse= True)
        for v in neg_time:
            y, m, d, h= v['_id']['year'], v['_id']['month'], v['_id']['day'], v['_id']['hour']
            neg_df['timeline'].append(datetime.datetime(y,m,d,h,tzinfo=pytz.utc).astimezone(tz.tzlocal()))
            neg_df['avg_score'].append(round(v['avg'], 2))
            neg_df['count'].append(v['count'])

        print('Time Series: querying positive sentiment')
        pos_df= {'timeline': [], 'avg_score': [], 'count':[]}
        pos_time= processed.aggregate(pipe_pos_time, allowDiskUse= True)
        for v in pos_time:
            y, m, d, h= v['_id']['year'], v['_id']['month'], v['_id']['day'], v['_id']['hour']
            pos_df['timeline'].append(datetime.datetime(y,m,d,h,tzinfo=pytz.utc).astimezone(tz.tzlocal()))
            pos_df['avg_score'].append(round(v['avg'], 2))
            pos_df['count'].append(v['count'])

        # label for each point and plot
        print('Time Series: adding labels then ploting')
        pos_line_text, neg_line_text, avg_line_text=[], [], []
        for i, x in enumerate (pos_df['count']):
            pos_line_text.append(key+' avg: '+str(pos_df['avg_score'][i])+'<br>'+'records: '+'{0:,}'.format(x))
        for i, x in enumerate (neg_df['count']):
            neg_line_text.append(key+' avg: '+str(neg_df['avg_score'][i])+'<br>'+'records: '+'{0:,}'.format(x))
        for i, x in enumerate (avg_df['count']):
            avg_line_text.append(key+' avg: '+str(avg_df['avg_score'][i])+'<br>'+'records: '+'{0:,}'.format(x))
        data.append(go.Scatter(x=avg_df['timeline'], y=avg_df['avg_score'], text= avg_line_text, line= dict(dash= line_type[track_i]),
                              hoverinfo= 'x+text', opacity= 0.8, mode = 'lines',
                              name='Average ({})'.format(key), yaxis='y3', marker=dict(color='gray')))
        data.append(go.Scatter(x=pos_df['timeline'], y=pos_df['avg_score'], text= pos_line_text, line= dict(dash= line_type[track_i]),
                              hoverinfo= 'x+text', opacity= 0.8, mode = 'lines',
                              name='Positive ({})'.format(key), yaxis='y2', marker=dict(color=color[2])))
        data.append(go.Scatter(x=neg_df['timeline'], y=neg_df['avg_score'], text= neg_line_text, line= dict(dash= line_type[track_i]),
                              hoverinfo= 'x+text', opacity= 0.8, mode = 'lines',
                              name='Negative ({})'.format(key), marker=dict(color=color[1])))
        print('---------Done with this track word---------')


    rangeselector= dict(buttons=list([dict(count=7, label='1w', step='day', stepmode='backward'),
                                      dict(count=1, label='1m', step='month', stepmode='backward'),
                                      dict(count=6,label='6m',step='month',stepmode='backward'),
                                      dict(step='all')]))
    margin= dict(t=50, b=50)
    line_layout= go.Layout(margin= margin,
                           xaxis=dict(domain=[0, 1], rangeselector=rangeselector, rangeslider=dict(),
                                      linecolor=border_col, mirror=True),
                           yaxis=dict(domain=[0, 0.3], linecolor=border_col, mirror=True),
                           yaxis2=dict(domain=[0.35, 0.65], linecolor=border_col, mirror=True),
                           yaxis3=dict(domain=[0.7, 1], linecolor=border_col, mirror=True),
                           shapes= [dict(type='line', line= {'width': 1, 'color': border_col},
                                         xref='paper', yref='paper', x0=0, x1=1, y0=0.35, y1=0.35),
                                    dict(type='line', line= {'width': 1, 'color': border_col},
                                         xref='paper', yref='paper', x0=0, x1=1, y0=0.65, y1=0.65),
                                    dict(type='line', line= {'width': 1, 'color': border_col},
                                         xref='paper', yref='paper', x0=0, x1=1, y0=0.7, y1=0.7),
                                    dict(type='line', line= {'width': 1, 'color': border_col},
                                         xref='paper', yref='paper', x0=0, x1=1, y0=1, y1=1)])
    line_figure= go.Figure(data= data, layout= line_layout)
    line_url= py.plot(line_figure, auto_open=False, filename='line', fileopt='overwrite')
    print('Done!\n')
    return line_url







# filter data from MongoDB and make a table
def TablePlot(trackword, time_from=None, time_to=None, senti_from=None, senti_to=None, re_limit=100):
    # trackword (str): used for opening correct database collection, and naming the table
    # time_from (list): included; [year, month, day, hour, minute] in user's timezone <tmzone>; use single digit if applicable (eg. use 1 not 01) 
    # time_to (list): included; [year, month, day, hour, minute] in user's timezone <tmzone>; use single digit if applicable (eg. use 1 not 01)
    # senti_from (int): included; lower boundary of sentiment
    # senti_to (int): included; upper boundary of sentiment
    # re_limit (int): how many records to return
    # return earliest <re_limit> results in ascending if <time_from> is defined
    # return lastest <re_limit> results in decending if <time_to> is defined; higher priority, ignore <time_from> rule if <time_to> is defined
    # time shown in table is in <tmzone>


    # set query method
    # convert local/input time to UTC, adjust with Daylight Savings rules
    print('Text Table: current track word is "{}"'.format(trackword))
    if time_from!= None and time_to!= None:
        from_y, from_m, from_d, from_h, from_min= time_from
        to_y, to_m, to_d, to_h, to_min= time_to
        from_utc= pytz.timezone(tmzone).localize(datetime.datetime(from_y, from_m, from_d, from_h, from_min)).astimezone(pytz.utc)
        to_utc= pytz.timezone(tmzone).localize(datetime.datetime(to_y, to_m, to_d, to_h, to_min)).astimezone(pytz.utc)
        time_query= {'$gte': from_utc, '$lte': to_utc}
        sort_query= {'_id': 1} # '-1' for decending
        note= 'lower time boundary'
    elif time_from!= None and time_to== None:
        from_y, from_m, from_d, from_h, from_min= time_from
        from_utc= pytz.timezone(tmzone).localize(datetime.datetime(from_y, from_m, from_d, from_h, from_min)).astimezone(pytz.utc)
        time_query= {'$gte': from_utc}
        sort_query= {'_id': 1}
        note= 'lower time boundary'
    elif time_from== None and time_to!= None:
        to_y, to_m, to_d, to_h, to_min= time_to
        to_utc= pytz.timezone(tmzone).localize(datetime.datetime(to_y, to_m, to_d, to_h, to_min)).astimezone(pytz.utc)
        time_query= {'$lte': to_utc}
        sort_query= {'_id': -1}
        note= 'upper time boundary'
    elif time_from== None and time_to== None:
        to_utc= datetime.datetime.utcnow()
        time_query= {'$lte': to_utc}
        sort_query= {'_id': -1}
        note= 'the most recent time'
        print('Text Table: time boundary is not set, getting most recent data')


    senti_query= None
    if senti_from!=None and senti_to==None:
        senti_query= {'$gte':senti_from}
    elif senti_from==None and senti_to!=None:
        senti_query= {'$lte':senti_to}
    elif senti_from!=None and senti_to!=None:
        senti_query= {'$gte':senti_from, '$lte':senti_to}
    # 1 or True to include the field, 0 or False to exclude the field
    pipe_table = [{'$match': {'sentiment': senti_query, 'collected_at': time_query}},
                  {'$sort': sort_query}, # '-1' for decending
                  {'$limit': re_limit},
                  {'$project': {'collected_at':{'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$collected_at','timezone': tmzone}},
                                '_id':0, 'content':1, 'content_category':1, 'sentiment':1, 'user_location':1}}]
    if senti_from==None and senti_to==None:
        print('Text Table: sentiment range/limit is not set, considering all data')
        pipe_table = [{'$match': {'collected_at': time_query}},
                      {'$sort': sort_query}, # '-1' for decending
                      {'$limit': re_limit},
                      {'$project': {'collected_at':{'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$collected_at','timezone': tmzone}},
                                    '_id':0, 'content':1, 'content_category':1, 'sentiment':1, 'user_location':1}}]


    print('Text Table: querying')
    processed= processed= db['tweets_processed_{}'.format(trackword)]
    table= processed.aggregate(pipe_table, allowDiskUse= True)

    # write returned MongoDB data to json temp file, then load to df
    temp_file_name= 'table_temp.json'
    temp= open(temp_file_name, 'w', encoding='utf8') # clean temp file
    temp.close()
    with open(temp_file_name, 'a', encoding='utf8') as file:
        for d in table:
            file.write(json.dumps(d))
            file.write('\n')
    df_table= pd.read_json(temp_file_name, orient='records', lines=True)
    content_col_name= 'Content (first {} records from '.format(re_limit)+note+')'
    df_table.columns= ['Collected At', content_col_name, 'Content Category', 'Sentiment', 'User Location']

    # table plot
    rowEvenColor = 'lightgrey'
    rowOddColor = 'white'
    print('Text Table: generating table')
    table = go.Table(columnwidth = [70,470,100,60,80],
                      header=dict(values =list(df_table.columns),
                                  font = dict(color = 'white', size = 13),
                                  line = dict(color = 'black'),
                                  fill = dict(color = 'grey'),
                                  align = ['left','center']),
                      cells = dict(values = [df_table['Collected At'], df_table[content_col_name], df_table['Content Category'],
                                             df_table['Sentiment'], df_table['User Location']],
                                   line = dict(color = 'black'),
                                   fill = dict(color = [rowOddColor,rowEvenColor,rowOddColor, rowEvenColor,rowOddColor]),
                                   align = ['left', 'left', 'center'],
                                   font = dict(color = '#506784', size = 11),
                                   height = 55))
    table_layout= go.Layout(margin= dict(l=20,r=20,t=30,b=30))
    table_figure= go.Figure(data= [table], layout= table_layout)
    table_url= py.plot(table_figure, auto_open=False, filename = 'table_{}'.format(trackword), fileopt='overwrite')
    print('Done!\n')
    return table_url






# return fileId from a url
def fileId_from_url(url):
    # Plotly account user name should be in format <n of letters plus n of numbers>
    # otherwise modify the function
    raw_fileId = re.findall("[A-z]+[0-9]+/[0-9]+", url)[0]
    return raw_fileId.replace('/', ':')





# build dashboard
def MakeBoard (pie_url, line_url, table_url):
    # pie_url (list), line_url (not a list), table_url (list)


    print('Dashboard: assembling plots')
    dboard = dashboard.Dashboard()
    table_tracklist= tracklist[::-1]
    table_box0= {'type': 'box','boxType': 'plot','fileId': fileId_from_url(table_url[table_tracklist[0]]),'shareKey': None,
                 'title': 'Tweets Containing Keyword: {}'.format(table_tracklist[0])}
    dboard.insert(table_box0) # insert table
    if len(table_url)>=2:
        for i in table_tracklist[1:]:
            table_box= {'type': 'box','boxType': 'plot','fileId': fileId_from_url(table_url[i]),'shareKey': None,
                        'title': 'Tweets Containing Keyword: {}'.format(i)}
            dboard.insert(table_box, 'above', 1, fill_percent=70)



    line_box= {'type': 'box','boxType': 'plot','fileId': fileId_from_url(line_url),'shareKey': None,
               'title': 'Average Sentiment (per {})'.format(groupby_freq)}
    dboard.insert(line_box, 'above', 1, fill_percent=57) # insert time series
    
    
    
    fmt= '%Y-%m-%d %H:%M:%S %Z%z'
    time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(tmzone))
    time_str= time.strftime(fmt)
    m= 'Last updated at : {} ({}). \nRefreshing every {} minutes.'.format(time_str, tmzone, update_freq)
    text_box= {'type': 'box', 'boxType': 'text', 'text': '', 'title': m}
    dboard.insert(text_box, 'above', 1, fill_percent= 4) # insert message box



    pie_box0= {'type': 'box','boxType': 'plot','fileId': fileId_from_url(pie_url[tracklist[0]]),'shareKey': None,
               'title': 'Overall Sentiment ({})'.format(tracklist[0])}
    dboard.insert(pie_box0, 'left', 2, fill_percent=25) # insert pie
    if len(pie_url)>=2:
        for i in tracklist[1:]:
            pie_box= {'type': 'box','boxType': 'plot','fileId': fileId_from_url(pie_url[i]),'shareKey': None,
                      'title': 'Overall Sentiment ({})'.format(i)}
            dboard.insert(pie_box, 'below', 2)


    # dashboard setting   
    dboard['settings']['title']= 'Real-time Twitter Sentiment Analysis ({} vs. {})'.format(tracklist[0], tracklist[-1])
    dboard['settings']['fontFamily']= 'Overpass'
    dboard['settings']['logoUrl']= 'https://www.xtendlabs.com/static/images/xtendlabs.png'
    dboard['settings']['links'] = [{'title': '***Click here to check our website***', 'url': 'https://www.xtendlabs.com/'}]    
    dboard['layout']['size'] = 2000 # change length      

#    dboard['settings']['foregroundColor'] = '#000000'
#    dboard['settings']['backgroundColor'] = '#adcaea'
#    dboard['settings']['headerForegroundColor'] = '#ffffff'
#    dboard['settings']['headerBackgroundColor'] = '#D232C8'
#    dboard['settings']['boxBackgroundColor'] = '#ffffff'
#    dboard['settings']['boxBorderColor'] = '#000000'
#    dboard['settings']['boxHeaderBackgroundColor'] = '#ffffff'

    
    if platform == "linux" or platform == "linux2": auto_open= False
    else: auto_open= True
    dashboard_url= py.dashboard_ops.upload(dboard, 'Dashboard', auto_open= auto_open)   
    print('Done! Check Website: {}\n'.format(dashboard_url))
    return dashboard_url





#%% cal funtions to continuously update data

t0= time.time()
first_retrieve= True
while True:
    if (time.time() - t0 >= update_freq*60) or (first_retrieve== True):        
        # time printed in console
        cs_fmt= '%Y-%m-%d %H:%M:%S %Z%z'
        cs_time_str= datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(tmzone)).strftime(cs_fmt)        
        print('\n\n=============================================')
        print('Time zone: {}'.format(tmzone))
        print('Updating dashboard at {}'.format(cs_time_str))
        print('=============================================')



        pie_url={}
        for key in tracklist: pie_url[key]= PiePlot(key)
        
        # Text table can be filtered by time range and sentiment range, check <TablePlot> function
        table_url= {}
        for key in tracklist: table_url[key]= TablePlot(key, re_limit=200)
        
        line_url= TimeSeries(tracklist)
        
        dashboard_url= MakeBoard(pie_url, line_url, table_url)



        client.close()
        first_retrieve= False
        cs_time_str= datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(tmzone)).strftime(cs_fmt)
        t0= time.time()
        print('=============================================')
        print('Current time: {}'.format(cs_time_str))
        print('Pause {} mins before next update...'.format(update_freq))
        print('=============================================')
        for s in range(update_freq*60): time.sleep(1)









