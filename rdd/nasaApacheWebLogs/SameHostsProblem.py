from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....    

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''

    conf = SparkConf().setAppName("same host").setMaster("local[1]")
    sc = SparkContext(conf = conf)
    
    lines1 = sc.textFile("in/nasa_19950701.tsv")
    hosts1 = lines1.map(lambda line: line.split("\t")[0])
    
    lines2 = sc.textFile("in/nasa_19950801.tsv")
    hosts2 = lines2.map(lambda line: line.split("\t")[0])

    common_hosts = hosts1.intersection(hosts2).filter(lambda host: host != "host")
    common_hosts.saveAsTextFile("out/nasa_logs_common_hosts.csv")
    print('Done!')