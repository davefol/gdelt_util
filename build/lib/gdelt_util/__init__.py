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
		return dates_DataFrame

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


	