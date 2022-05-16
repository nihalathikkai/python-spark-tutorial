from pyspark import SparkContext, SparkConf
import re

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    ...
    '''
    conf = SparkConf().setAppName("airports)").setMaster("local[2]")
    sc = SparkContext(conf = conf)
    
    p = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')
    
    airports = sc.textFile('in/airports.text').map(lambda line : p.split(line))
    airportsinUSA = airports.filter(lambda line : line[3] == '"United States"')
    
    # airports = sc.textFile('in/airports.text')
    # airportsinUSA = airports.filter(lambda line : '"United States"' in line).map(lambda line : p.split(line))
    
    airportsNameandCity = airportsinUSA.map(lambda x : f"{x[1]}, {x[2]}")
    
    airportsNameandCity.saveAsTextFile("out/airports_in_usa.text")
    sc.stop()