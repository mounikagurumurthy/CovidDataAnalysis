import json
import string
import sys
import requests
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
#Slack URL
webhook = "https://hooks.slack.com/services/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"
def convert_to_dict(columns, results):
"""
This method converts the resultset from postgres to dictionary
interates the data and maps the columns to the values in result set and converts to dictionary
:param columns: List - column names return when query is executed
:param results: List / Tupple - result set from when query is executed
:return: list of dictionary- mapped with table column name and to its values
"""
allResults = []
columns = [col.name for col in columns]
if type(results) is list:
  for value in results:
    allResults.append(dict(zip(columns, value)))
  return allResults
elif type(results) is tuple:
  allResults.append(dict(zip(columns, results)))
  return allResults
# Send Slack Message
def send_slack_message(payload, webhook):
"""Send a Slack message to a channel via a webhook.
Args:
payload (dict): Dictionary containing Slack message, i.e. {"text": "This is a test"}
webhook (str): Full Slack webhook URL for your chosen channel.
Returns:
HTTP response code, i.e. <Response [503]>
"""
from psycopg2.extensions import JSON
response = requests.post(webhook, data= json.dumps(payload),headers={'Content-type': 'application/json'})
#return requests.post(webhook, json.dumps("{Hi Hellow:Data }"))
#return requests.post(webhook, json.dumps(payload))
if response.status_code != 200:
  raise ValueError(
'Request to slack returned an error %s, the response is:\n%s'
% (response.status_code, response.text)
)
#PostGres Link
conn = psycopg2.connect(
database="CovidData", user='postgres', password='1803', host='127.0.0.1', port= '5432'
)
#Setting auto commit false
conn.autocommit = True
#Creating a cursor object using the cursor() method
cursor = conn.cursor()#cursor_factory=psycopg2.extras.DictCursor)
#retreive cumulative month data
cursor.execute('''select month_deaths from covid_19_total_deaths
WHERE month >= '2020-03-01'
AND month <= '2020-03-31' ''')
temp_result = cursor.fetchone()[0]
result_group_cumulative : string = str(temp_result).strip("[]")
#Retrieving data
query = '''select state, sum(today_deaths) as month_deaths,
DATE_TRUNC('month',date) as month_of_2020,ROUND(sum(today_deaths)/'''
query += str(result_group_cumulative)
query += str('''::numeric,4)*100 as percentage_deaths
from covid19_cases_deaths
WHERE date >= '2020-03-01'
AND date <= '2020-03-31'
group by state, month_of_2020 order by percentage_deaths desc limit 3''')
print(query)
cursor.execute(query)
result = cursor.fetchall();
print(result)
#Commit your changes in the database
conn.commit()
#Closing the connection
conn.close()
payload = result
test_data = 'Top 3 States with the highest number of covid deaths for the month of March \n Month - March \n'
print(payload)
for items in payload :
  test_data += '\nState #' + items[0]+' with number of deaths ' + str(items[1]) \ +' which is '+ str(items[3]) + ' % of total US deaths '
dict1= {'text': test_data}
print(json.dumps(dict1))
send_slack_message(dict1, webhook)
