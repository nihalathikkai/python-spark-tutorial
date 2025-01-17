from pyspark import SparkContext, SparkConf
import re

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
    conf = SparkConf().setAppName("airports").setMaster("local[2]")
    sc = SparkContext(conf = conf)
    
    p = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')
    
    airports = sc.textFile("in/airports.text").map(lambda x : p.split(x))
    airportsLat40 = airports.filter(lambda x: float(x[6]) > 40)
    
    airportNameandLat = airportsLat40.map(lambda x : f'{x[1]}, {x[6]}')
    
    airportNameandLat.saveAsTextFile('out/airports_by_latitude.text')
        
    sc.stop()
