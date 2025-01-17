from pyspark import SparkContext, SparkConf
import sys
sys.path.insert(0, '.')
from commons.Utils import Utils

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
    Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "St Anthony", 51.391944
    "Tofino", 49.082222
    ...
    '''

    configuration = SparkConf().setAppName("airports latitude").setMaster("local[*]")
    sc = SparkContext(conf = configuration)
    
    airports = sc.textFile("in/airports.text").map(lambda line: Utils.COMMA_DELIMITER.split(line))
    filtered_airports = airports.filter(lambda airport: float(airport[6])>40).collect()
    for ap in filtered_airports:
        print(f'{ap[1]}, {ap[6]}')