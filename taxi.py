import requests
from datetime import datetime,timedelta
import json
from geopy.geocoders import Nominatim
import csv
from google.cloud import bigquery

'''
These code is written as an assignement for Love,Bonito for the
Job Role of Data Engineer

written by : Hamna Rashid
created at: 22-11-2022

'''

def create_datetime(date_val):
    #This function formats the date according to the argument of the end point url that is : 2022-12-07T09%3A00%3A
    date_val=datetime.strptime(date_val, '%Y-%m-%dT%H:%M:%S')
    hour=date_val.strftime( '%H')
    minute=date_val.strftime( '%M')
    second=date_val.strftime( '%S')
    date=date_val.strftime('%Y-%m-%d')
    final_date='{}T{}%3A{}%3A{}'.format(date,hour,minute,second)
    return [final_date,date_val]


def extracting_locations(date1):
    final_dates=create_datetime(date1)
    api_endpoint="https://api.data.gov.sg/v1/transport/taxi-availability?date_time={}".format(final_dates[0])
    response = requests.get(api_endpoint)
    json_data = json.loads(response.text)
    #getting the cordinates
    cordinates = json_data['features'][0]['geometry']['coordinates']
    #getting taxi count total
    total_count=json_data['features'][0]['properties']['taxi_count']
    geoLoc = Nominatim(user_agent="GetLoc")
    # passing the coordinates as ("lat,long") for example: "1.22,1.333"
    concat_cord= list (map (lambda x : "{},{}".format(str(x[1]),str(x[0])), cordinates))
    #making a dictionary where coordinate is the key and area is the value
    final_dict= dict(map(lambda x: (x ,str(geoLoc.reverse(x)).replace(","," ")),concat_cord))
    print(final_dict)
    #writing to the file using dict writer
    has_header=False
    file_link="C:/Users/Zunair Mahmood/Desktop/files_bonito/%s.csv" % (final_dates[0])
    with open(file_link, 'a', encoding='utf-8',newline='') as _file_handler:
        fieldnames = ['timestamp','coordinates','area','total_count']
        fw = csv.DictWriter(_file_handler,fieldnames=fieldnames)
        for (v,k) in final_dict.items():
            if(not has_header):
                fw.fieldnames =fieldnames
                fw.writeheader()
            has_header=True
            fw.writerow({'timestamp':final_dates[1],'coordinates':v,'area':k,'total_count':total_count})
    #uploading the file to bigquery table
    file_to_bq(file_link)
def file_to_bq (file_link):
    #this function upload the file to bigquery table
    client = bigquery.Client(project="nth-boulder-368917")
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/Zunair Mahmood/Downloads/nth-boulder-368917-feef12a2edd6.json"
    table_id = "nth-boulder-368917.love_bonito_data_lake.cord_location"
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=False,)
    with open(file_link, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete
    table = client.get_table(table_id) 
    print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id))

def start_thread ():
    #this is the function for the scheduling
    dt = datetime(2022, 11, 1,0,0,0)
    end = datetime(2022, 11, 18, 0, 0, 0)
    step =timedelta(hours=1)
    result = []

    while dt < end:
        result.append(dt.strftime('%Y-%m-%dT%H:%M:%S'))
        dt += step
    for date_stamp in result :
        extracting_locations(date_stamp)

def send_missing_alert_email():
    #Alert Mechanism. Fetching the data from the table which is prepared by the missing_taxi.sql and send to the data team
    client_bq = bigquery.Client(project='nth-boulder-368917')
    query_job7 = client_bq.query("select * from `love_bonito_data_lake.missing_taxi` where hour = (select timestamp from `love_bonito_data_lake.cord_location` where DATE(_PARTITIONTIME) IS NOT NULL order by timestamp desc limit 1 )")
    list1=[]
    for row in query_job7:
        list1.append(str(row[1]))
    return requests.post(
		"https://api.mailgun.net/v3/sandbox1fd9cae737244ff2a82d2f690b34b9e3.mailgun.org/messages",
		auth=("api", "e174dee442ef80b4d3dcd16d09b4578a-69210cfc-e408a93b"),
		data={"from": "Mailgun Sandbox <postmaster@sandbox1fd9cae737244ff2a82d2f690b34b9e3.mailgun.org>",
			"to": "hamna <hamnazunair@gmail.com>",
			"subject": "Missing Taxi Alert",
			"text": "These are the missing area: \n {} \n".format(list1)})
                
	



if __name__ == '__main__':
    #Scheduler
    start_thread()
    #Alert Mechanism
    send_missing_alert_email()

    

