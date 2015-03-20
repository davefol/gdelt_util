# Time Series
import datetime as dt 
import pandas as pd

# Plotting
import matplotlib # plotting (sorta ugly, we'll do better later)
import matplotlib.pyplot as plt
# Here we switch the aesthetics of matplotlib to seaborn
import seaborn as sns 
sns.set_palette("husl") # much better looks

import gc

from progressbar import ProgressBar

# Add PCA features
# Clustering output
# More plot features 

def timestampToDatetime(timestamp):
    """
        converts pandas timestamp to datetime
    """
    return dt.date(timestamp.year,timestamp.month,timestamp.day)

def datetimeToTimestamp(date):
    """ 
        convert datetime date to pandas timestamp
    """
    return  pd.tslib.Timestamp('%s-%s-%s' % (date.year,date.month,date.day))

def convertSQLDATE(sqldate):
    """ 
        input: string in SQLDATE format ie:  19970416 Year Month Day
        output: datetime object
    """
    return dt.datetime.strptime(str(sqldate),'%Y%m%d')

def selectFromData(dataframe,column,value):
    """ 
        input: dataframe, column(string), value
        output: new dataframe with only the rows where column = value
    """
    return dataframe[dataframe[column]==value]

def sqldateToTimestamp(dataframe):
    """ 
        input: datafame with SQLDATE column
        output: dataframe with Timestamp column replacing SQLDATE column
    """
    dataframe['SQLDATE'] = dataframe['SQLDATE'].apply(convertSQLDATE)
    dataframe = dataframe.rename(columns={'SQLDATE':'Timestamp'})
    return dataframe

def generateFrequencyEvent(dataframe,eventcode):
        """ Generate Pandas Series of eventcode frequency """

        subframe = dataframe[['SQLDATE','EventCode']]
        subframe = selectFromData(subframe,'EventCode',eventcode)
        by_date = subframe.groupby('SQLDATE')
        by_date_sum = by_date.size()
        by_date_sum.index = [convertSQLDATE(i) for i in by_date_sum.index]
        return by_date_sum.sort_index().resample('D').fillna(0)

    
def stdLabel(data):
    """ 
        This function looks at a Series that 
        contains a date index and a frequency column
        It returns a dataframe with date column and Response column
        the response value is 1 if the corresponding frequency is 
        higher than 1 standard deviation
        and -1 if otherwise
    """
    mean = float(data.mean())
    std = float(data.std())
    responses = []

    for row in data.iteritems():
        if (row[1] > mean + std):
            responses.append(1)
        else:
            responses.append(-1)
    responsesSeries = pd.Series(index = data.index,data = responses)
    return responsesSeries

def lastnDays(date,n):
    """ 
        input datetime date
        output list of the n previous days
    """ 
    previous_days = []
    for i in range (1,n+1):
        delta = dt.timedelta(days=i)
        new = date - delta
        previous_days.append(new)
    return previous_days

def plotEventCodes(data,eventcodes,freq='M'):
    """ 
    given a dataframe and a list of eventcodes, 
    this function plots frequency graphs
    for each of the event codes
    """
    for event in eventcodes:
        event_series = generateFrequencyEvent(data,event)
        # Resample our data by freq ('M' for monthly)
        resampled = event_series.resample(freq) 
        resampled.plot()
    plt.show()

def _countOnes(series):
    """ Return the number of 1s in a pandas Series """
    count = 0
    for i in series.iteritems():
        if i[1] == 1:
            count += 1
    return count

def _combineSeries(a,b):
    """ Combine two pandas Series """
    # find the smallest index and the largest index
    c = a.add(b,fill_value=0)
    return c

def _writeVWLine(line):
    """ Return a Vowpal Wabbit entry as a string """
    # line is a pandas series with a Timestamp, Label, and series of features
    entry = "%s '%s | " % (line['Label'],line['Timestamp'])
    for i in line.iteritems():
        if (i[0] != "Label") or (i[0] != "Timestamp"):
            entry = entry + "%s:%s " % (i[0],i[1])
    return entry

def writeVWFile(features,output):
    """ write Vowpal Wabbit input file to output """

    for line in features.iterrows():
        entry = _writeVWLine(line[1])
        with open(output,'a') as f:
            f.write(entry + '\n')



def generateFeatures(data,responses,predictors,labelingfunction,window=30):
    """ Return a pandas dataframe with response column feature columns

    Perform a moving window over the data to generate features

    """

    features = []

    # Aggregate the response event codes into one frequency Series.
    response_series = pd.Series()
    for eventcode in responses:
        event_series = generateFrequencyEvent(data,eventcode)
        response_series = _combineSeries(event_series,response_series)

    # Label the responseSeries.
    labeled_responses = labelingfunction(response_series)

    print "Number of Responses labeled as significant: %d" % _countOnes(labeled_responses)

    # Generate the frequency series for each of our predictor codes. 
    predictor_series_dict = {}
    for eventcode in predictors:
        predictor_series_dict[eventcode] = generateFrequencyEvent(data,eventcode)

    print "Processing..."
    pbar = ProgressBar(maxval=len(labeled_responses)).start()
    count_update = 0
    for row in labeled_responses.iteritems():
        try:
            observation = {}
            observation['Timestamp'] = row[0]
            observation['Label'] = row[1]

            for day in lastnDays(timestampToDatetime(row[0]),window):
                past_day  = datetimeToTimestamp(day)
                delta = row[0] - past_day
                for eventcode in predictors:
                    observation['%s-%s' % (eventcode,delta.days)] = [predictor_series_dict[eventcode][past_day]]
            gc.disable()
            features.append(observation)
            gc.enable()

        except KeyError:
            # There will always be window number of key errors.
            # The window looks for days that do not exists in the data.
            # We can safely ignore these errors. 
            pass
        
        count_update +=1
        pbar.update(count_update)

    pbar.finish()

    return pd.DataFrame(features)

    