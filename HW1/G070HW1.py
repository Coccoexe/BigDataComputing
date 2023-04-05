# Homework 1 - Triangle Counting
# Group 70 - Alessio Cocco 2087635, Andrea Valentinuzzi 2090451, Giovanni Brejc 2096046

import pyspark
from pyspark import SparkContext, SparkConf
import random, sys, os, statistics, time
from CountTriangles import CountTriangles

global timer
timer = []

def stopwatch(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        timer.append(end - start)
        return result
    return wrapper

# Algorithm 1
@stopwatch
def MR_ApproxTCwithNodeColors(RDD: pyspark.RDD, C: int):
    p = 8191
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    h = lambda u: ((a * u + b) % p) % C
    t = [CountTriangles(RDD.filter(lambda x: h(x[0]) == i and h(x[1]) == i).collect()) for i in range(C)]
    return C**2 * sum(t)
    
# Algorithm 2
@stopwatch
def MR_ApproxTCwithSparkPartitions(RDD: pyspark.RDD, C: int):
    #generate C random partitions
    partitions = RDD.randomSplit([1/C]*C)
    t = [CountTriangles(partitions[i].collect()) for i in range(C)]
    
    return C**2 * sum(t)                                                        # return the final estimate

def main():
    # check arguments
    if len(sys.argv) != 4:
        print("Usage: python G070HW1.py <int C> <int R> <path of file>")
        exit(-1)
    if not sys.argv[1].isdigit():
        print("C must be an integer")
        exit(-1)
    if not sys.argv[2].isdigit():
        print("R must be an integer")
        exit(-1)
    if not os.path.exists(sys.argv[3]):
        print("File does not exist")
        exit(-1)
    if not sys.argv[3].endswith(".txt"):
        print("File must be a text file")
        exit(-1)      
    
    # parse arguments
    C = int(sys.argv[1])
    R = int(sys.argv[2])          
    file = sys.argv[3]
    
    # spark setup
    conf = SparkConf().setAppName('G070HW1')
    sc = SparkContext(conf=conf)
    
    # read input file and subdivide it into C random partitions
    # parse input with format: "node1,node2"
    docs = sc.textFile(file).map(lambda x: x.split(",")).map(lambda x: (int(x[0]), int(x[1]))).cache()
    
    print("Dataset = " + file)
    print("Number of Edges = " + str(docs.count()))
    print("Number of Partitions = " + str(C))
    print("Number of Rounds = " + str(R))
    
    # run algorithm 1
    tfinal = [MR_ApproxTCwithNodeColors(docs, C) for i in range(R)]
    
    print("Approximation through node coloring")
    print("- Number of triangles (median over ", R , " runs) = ", statistics.median(tfinal)) 
    print("- Running time (average over ", R , " runs) = ", round(statistics.mean(timer)*1000), " ms")
    timer.clear()
    print("Approximation through Spark partitions")
    print("- Number of triangles = ", MR_ApproxTCwithSparkPartitions(docs, C))
    print("- Running time = ", round(timer[0]*1000), " ms")

# main function
if __name__ == "__main__":
    main()
     