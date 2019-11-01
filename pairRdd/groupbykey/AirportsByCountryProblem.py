import sys
sys.path.insert(0, ".")

from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text,
    output the the list of the names of the airports located in each country.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    "Canada", ["Bagotville", "Montreal", "Coronation", ...]
    "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
    "Papua New Guinea",  ["Goroka", "Madang", ...]
    ...

    '''
    cf = SparkConf().setAppName("AirportsByCountry").setMaster("local[*]")
    sc = SparkContext(conf=cf)

    airports_text = sc.textFile("in/airports.text")
    airports_arr = airports_text.map(lambda ap: Utils.COMMA_DELIMITER.split(ap))
    country_airport_pairs = airports_arr.map(lambda ap: (ap[3], ap[1]))
    country_airports = country_airport_pairs.groupByKey().collectAsMap()
    for item in country_airports.items():
        print(f"{item[0]}: {list(item[1])}")

    print("---done---")

     