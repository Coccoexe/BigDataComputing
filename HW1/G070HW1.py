# Homework 1 - Triangle Counting
# Group 70 - Alessio Cocco 2087635, Andrea Valentinuzzi 2090451, Giovanni Brejc 2096046

import pyspark
from pyspark import SparkContext, SparkConf
import random, sys, os, statistics, time
from collections import defaultdict

def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

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

# Algorithm 1 old version
@stopwatch
def MR_ApproxTCwithNodeColors_old(RDD: pyspark.RDD, C: int):
    p = 8191
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    h = lambda u: ((a * u + b) % p) % C    
    t = [CountTriangles(RDD.filter(lambda x: h(x[0]) == i and h(x[1]) == i).collect()) for i in range(C)]    
    return C**2 * sum(t)

#################### ALGORITHM 1 ####################
@stopwatch
def MR_ApproxTCwithNodeColors(RDD: pyspark.RDD, C: int):
    """ Compute an estimate of the number of triangles in the graph represented by the input RDD.

    Args:
        RDD (pyspark.RDD): Graph represented by an RDD of edges
        C (int): Number of colors

    Returns:
        int: An estimate of the number of triangles in the graph
    """

    p = 8191
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    h = lambda u: ((a * u + b) % p) % C                                                                               # hash function
    t = (RDD.map(lambda x: (h(x[0]), x) if h(x[0]) == h(x[1]) else None).filter(lambda x: x is not None).groupByKey() # ROUND 1.1: (color, (u, v)) if u and v have the same color, else None --> (color, [(u, v), (u, v), ...]])
            .map(lambda x: (x[0], CountTriangles(list(x[1])))).values().collect())                                    # ROUND 1.2: (color, number of triangles in the partition) --> [t1, t2, ...]
    return C**2 * sum(t)                                                                                              # ROUND 2: return an estimate of the number of triangles in the graph

#################### ALGORITHM 2 ####################
@stopwatch
def MR_ApproxTCwithSparkPartitions(RDD: pyspark.RDD, C: int):
    """ Compute an estimate of the number of triangles in the graph represented by the input RDD.
    
    Args:
        RDD (pyspark.RDD): Graph represented by an RDD of edges
        C (int): Number of colors
        
    Returns:
        int: An estimate of the number of triangles in the graph
    """
    
    # TODO: improve this shit
    t = (RDD.repartition(C)                                          # ROUND 1.1: subdivide the input RDD into C random partitions
            .mapPartitions(lambda x: [CountTriangles(x)]).collect()) # ROUND 1.2: count the number of triangles in each partition --> [t1, t2, ...]
    return C**2 * sum(t)                                             # ROUND 2: return an estimate of the number of triangles in the graph

#####################################################
def main():
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
    print("- Running time (average over ", R , " runs) = ", round(statistics.mean(timer)/C*1000), " ms")
    timer.clear()
    print("Approximation through Spark partitions")
    print("- Number of triangles = ", MR_ApproxTCwithSparkPartitions(docs, C))
    print("- Running time = ", round(timer[0]/C*1000), " ms")

# main function
if __name__ == "__main__":
    main()
     