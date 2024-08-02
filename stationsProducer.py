import os
import json
import time
from kafka import KafkaProducer
import urllib.request
from datetime import datetime
from dotenv import load_dotenv


#load the api key from env variables
def configure():
    load_dotenv()

# kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# main function of the producer
def main():

    #url with api key for requesting the stations data
    url = f"https://api.jcdecaux.com/vls/v1/stations?apiKey={os.getenv('API_KEY')}"
    # requesting data and send to kafka broker
    while True:
        response = urllib.request.urlopen(url)
        stations_data = json.loads(response.read().decode())

        for station in stations_data:
            utcfromtimestamp = datetime.utcfromtimestamp(station['last_update']/1000).strftime('%Y-%m-%d %H:%M:%S')
            position = {
                'lat': station['position']['lat'],
                'lon': station['position']['lng']
            }

            station_data = {
                'numbers': station['number'],
                'contract_name': station['contract_name'],
                'banking': station['banking'],
                'bike_stands': station['bike_stands'],
                'available_bike_stands': station['available_bike_stands'],
                'available_bikes': station['available_bikes'],
                'address': station['address'],
                'status': station['status'],
                'position':position,
                'timestamps': utcfromtimestamp
            }
            print(station_data)
            time.sleep(1)
            # Send formatted data to Kafka topic
            producer.send("velib-stations", station_data)

        print("{} Produced {} station records".format(time.time(), len(stations_data)))
        time.sleep(1)
if __name__=="__main__":
    configure()
    main()