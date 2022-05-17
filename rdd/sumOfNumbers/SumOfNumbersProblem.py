from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''
    
    conf = SparkConf().setAppName("reduce").setMaster("local[2]")
    sc = SparkContext(conf = conf)
    
    inputfile = sc.textFile("in/prime_nums.text")
    numbers = inputfile.flatMap(lambda x : x.split("\t")).filter(lambda x : x)
    prime = numbers.map(lambda x : int(x))
    
    print(f"Sum is {prime.reduce(lambda x,y : x+y)}")
