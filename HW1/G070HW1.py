# Homework 1 - Triangle Counting
# Group 70 - Alessio Cocco 2087635, Andrea Valentinuzzi 2090451, Giovanni Brejc 2096046

import pyspark
from pyspark import SparkContext, SparkConf
import random, sys, os, statistics, time

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
    # parameters
    p = 8191
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    h = lambda u: ((a * u + b) % p) % C
    
    # create C subsets of edges, where each i-th subset E(i) contains all edges (u,v) such that h(u) = h(v) = i, and if the 2 endpoints of an edge belong to different subsets, then the edge is discarded.
    E = [RDD.filter(lambda x: h(int(x[0])) == i and h(int(x[1])) == i).map(lambda x: (x[0], x[1])).cache() for i in range(C)]

    # compute the number t(i) of triangles in each subset E(i)
    t = [E[i].join(E[i]).map(lambda x: (x[1][0], x[1][1])).join(E[i]).map(lambda x: (x[0], x[1][0], x[1][1])).count() for i in range(C)]

    # compute the total number of triangles
    tfinal = sum(t)

    return tfinal

# Algorithm 2
def MR_ApproxTCwithSparkPartitions():
    return

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
    docs = sc.textFile(file).map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])).repartition(numPartitions=C).cache()
    docs.repartition(numPartitions=C)
    
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

# main function
if __name__ == "__main__":
    main()
     