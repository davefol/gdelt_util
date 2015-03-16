# Time Series
import datetime as dt 
import pandas as pd

# Plotting
import matplotlib # plotting (sorta ugly, we'll do better later)
import matplotlib.pyplot as plt
import seaborn as sns # Here we switch the aesthetics of matplotlib to seaborn
sns.set_palette("husl") # much better looks

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
	return dataframe.loc[dataframe[column]==value]

def sqldateToTimestamp(dataframe):
	""" 
		input: datafame with SQLDATE column
		output: dataframe with Timestamp column replacing SQLDATE column
	"""
	dataframe['SQLDATE'] = dataframe['SQLDATE'].apply(convertSQLDATE)
	dataframe = dataframe.rename(columns={'SQLDATE':'Timestamp'})
	return dataframe

def generateFrequencyEvent(dataframe,eventcode):
		""" 
			generates dataframe containing 
			frequency of suplied event code(int) 
		"""
		# Generate the dates
		earliest_date = timestampToDatetime(min(dataframe['Timestamp']))
		latest_date = timestampToDatetime(max(dataframe['Timestamp']))
		# Convert to datetime

		time_difference = latest_date - earliest_date

		dates = {str(earliest_date + dt.timedelta(days=x)) : 0 for x in range(0,time_difference.days+1)}

		# Count the number of events for each day
		for i in dataframe.iterrows():
			try:
				if i[1]['EventCode'] == eventcode:
					dates[str(timestampToDatetime(i[1]['Timestamp']))] += 1
			except:
				pass

		# Generate Data frame
		dates_DataFrame = pd.DataFrame.from_dict(dates, orient="index")
		dates_DataFrame = dates_DataFrame.sort()
		dates_DataFrame.columns = ['Frequency']

		# Convert index to timestamp
		dates_DataFrame.index = pd.to_datetime(dates_DataFrame.index)
		# Return Series
		return pd.Series(dates_DataFrame['Frequency'],index=dates_DataFrame.index)

def generateFeaturesAndResponses(data,features,response,window):
	"""
		Given features (list of event codes) and response (Event code prediction)
		and data, generate a list of responses and features
		window is the days before to look at
		NOT WORKING YET
	"""
	# This dictionary holds the frequency data for our predictor codes
	feat_set = {}

	# this Data set holds the frequency data for our response code
	freq_response = generateFrequencyEvent(data,response)

	# Generate frequency datasets for our predictors
	for i in features:
		feat_set[i] = generateFrequencyEvent(data,i)

	# This list holds our final responses
	res = []
	pred = []

	for i in enumerate(freq_response.iterrows()):
		if i[0] < window:
			pass
		else:
			# Append our response variable to our res list
			res.append(i[1][1]['Frequency'])

			# new list of feature values
			temp_feat = []

			# Create a moving window
			current = i[1][1].index
			before = current - dt.timedelta(days=window)
			for k in features:
				for day in [before + dt.timedelta(days=x) for x in range(0,window)]:
					temp_feat.append(feat_set[k][day])

			pred.append(temp_feat)

	return(pred,res)

def generateVW(data,eventcode,predictors,output,labelingfunction,window=14):
	"""
		Vowpal Wabbit format:
		response | features
		response is either 1 or -1 for classification
		features are numbered 1 to k with 1 being the first feature from the most recent day
		and k being the last feature from the most distant day
	"""
	# First generate the responses with some rule
	# Response is some event Code that we are testing for

	# First generate the frequency dataframe for a particular event
	freq = generateFrequencyEvent(data,eventcode)

	# Now run that dataframe through a labeling function, this returns a label either 1 or -1 for each
	# day in our data set
	responses = labelingfunction(freq)

	# print how many rows were labeled as 1
	count = 0
	for i in responses.iteritems():
		if i[1] == 1:
			count += 1
	print "Number of Responses labeled as significant: %d" % count

	# We also need the frequency series for each of our predictor event codes
	freq_pred = []
	for event in predictors:
		# predictors is a list of strings that are eventcodes
		# Append the frequency series to our list freq_pred
		freq_pred.append(generateFrequencyEvent(data,event))

	# Each row is a day
	# For each day we look back 'window' days in time
	for row in responses.iteritems():
		# We wrap this in a try in case we get index errors for timestamps that we dont have
		try:
			current_day = row[0]
			current_response = row[1]
			current_features = []
			for day in lastnDays(timestampToDatetime(current_day),window):
				# convert the day to pandas timestamp
				past_day  = datetimeToTimestamp(day)
				# For each of our predictor codes, we need to look up the frequency for the past_day
				for event_series in freq_pred:
					current_features.append(event_series[past_day])
			# We now have our current_features list filled with the frequency for our predictors
			# We can write our vowpal wabbit line now

			# First write the response and pipe( | )
			entry = "%d | " % current_response
			# Now write each of integers in our current_features list
			for i in enumerate(current_features):
				entry = entry + '%d:%d ' % (i[0]+1,i[1])

			# Finally, write out our line to file
			with open(output,'a') as f:
				f.write(entry + '\n')

			# Now write entry to file
		except KeyError:
			print "Key Error on day %s" % current_day
		print "Key Errors are safe to ignore, this error is likely a result of your window covering days that are not in your data"
	

def stdLabel(data):
	""" 
		This function looks at a Series that contains a date index and a frequency column
		It returns a dataframe with date column and Response column
		the response value is 1 if the corresponding frequency is higher than 1 standard deviation
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


# def plotRawData(csvfile):
# 	""" Takes a RAW CSV file from GDELT and generates several plots """
# 	data = pd.read_csv(csvfile,dtype=object)
# 	data = sqldateToTimestamp(data)
# 	# Plot the Frequency of Each event Code contained in the set 
# 	seenCodes = []
# 	subsets = []
# 	for row in data.iterrows():
# 		if row[1]['EventCode'] not in seenCodes:
# 			print "Generating Subset for: %s" % row[1]['EventCode']
# 			subsets.append(selectFromData(data,'EventCode',row[1]['EventCode']))
# 			seenCodes.append(row[1]['EventCode'])


	