from pyspark import SparkContext, SparkConf
import re

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
    
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    p = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')
    
    airports = sc.textFile("in/airports.text").map(lambda x : (p.split(x)))
    airportspairRDD = airports.map(lambda x : (x[1].strip('"'), x[3].strip('"')))
    airportsnotinUSA = airportspairRDD.filter(lambda x : x[1] != "United States")
    
    airportsnotinUSA.coalesce(1).saveAsTextFile("out/airports_not_in_usa_pair_rdd.text")