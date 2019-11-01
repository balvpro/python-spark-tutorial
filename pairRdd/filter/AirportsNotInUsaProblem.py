from pyspark import SparkContext, SparkConf
import sys
sys.path.insert(0, ".")
from commons.Utils import Utils

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text;
    generate a pair RDD with airport name being the key and country name being the value.
    Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located,
    IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    ("Kamloops", "Canada")
    ("Wewak Intl", "Papua New Guinea")
    ...

    '''
    conf = SparkConf().setAppName("Non US Airports").setMaster("local[1]")
    sc = SparkContext(conf=conf)
    airports_string_rdd = sc.textFile("in/airports.text")
    airports_arr_rdd = airports_string_rdd.map(lambda line: Utils.COMMA_DELIMITER.split(line))
    non_us_airports_rdd = airports_arr_rdd.filter(lambda ap: ap[3] != '"United States"')
    non_us_airports_pair_rdd = non_us_airports_rdd.map(lambda ap: (ap[1], ap[3]))
    print(non_us_airports_pair_rdd.count())


