from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....    

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''
        
    conf = SparkConf().setAppName("nasa").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    logs1 = sc.textFile("in/nasa_19950701.tsv")
    logs2 = sc.textFile("in/nasa_19950801.tsv")
    
    logs1host = logs1.filter(lambda x : not x.startswith("host\tlogname")).map(lambda x : x.split("\t")[0])
    logs2host = logs2.filter(lambda x : not x.startswith("host\tlogname")).map(lambda x : x.split("\t")[0])
    logsunion = logs1host.union(logs2host)
    
    logsunion.saveAsTextFile("out/nasa_logs_same_hosts.csv")
    
    sc.stop()