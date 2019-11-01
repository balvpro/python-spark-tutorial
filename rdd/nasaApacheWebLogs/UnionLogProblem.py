from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
    take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''
    conf = SparkConf().setAppName("union log").setMaster("local[1]")
    sc = SparkContext(conf = conf)
    
    lines1 = sc.textFile("in/nasa_19950701.tsv")
    
    lines2 = sc.textFile("in/nasa_19950801.tsv")

    all_lines = lines1.union(lines2).filter(lambda line: not line.startswith("host")).sample(False, 0.1)
    all_lines.saveAsTextFile("out/sample_nasa_logs.tsv")
    print('Done!')