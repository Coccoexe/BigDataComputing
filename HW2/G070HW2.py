# Homework 2 - Triangle Counting
# Group 70 - Alessio Cocco 2087635, Andrea Valentinuzzi 2090451, Giovanni Brejc 2096046

import pyspark
from pyspark import SparkContext, SparkConf
import random, os, statistics, time, argparse
from collections import defaultdict

global timer # global list to store the running time of each function
timer = []   # initialize the list

def stopwatch(func):
    """ Decorator function to measure the running time of a function. Appends the running time to the global list 'timer'.
    
    Args:
        func (function): Function to be measured
    
    Returns:
        function: Decorated function

    Usage:
        >>> @stopwatch
        >>> def foo():
        >>>     pass

        >>> foo()
        >>> print(round(statistics.mean(timer) * 1000), " ms")
        451 ms
    """

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        timer.append(end - start)
        return result
    return wrapper

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

def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)  
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:
        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

@stopwatch
def MR_ApproxTCwithNodeColors(RDD: pyspark.RDD, C: int):
    """ ALGORITHM 1: Compute an estimate of the number of triangles in the graph represented by the input RDD, using node coloring.

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

@stopwatch
def MR_ExactTC(RDD: pyspark.RDD, C: int):
    """ Compute the exact number of triangles in the graph represented by the input RDD, using node coloring.

    Args:
        RDD (pyspark.RDD): Graph represented by an RDD of edges
        C (int): Number of colors

    Returns:
        int: The exact number of triangles in the graph
    """

    p = 8191
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    h = lambda u: ((a * u + b) % p) % C
    # for each edge (u, v) create a separate C key-value pairs (k, (u, v)) where each i-th k is a triplet (h(u), h(v), i) sorted by non-decreasing order
    # then group by key and count the number of triangles for each key
    # finally sum the number of triangles for each key

    # ROUND 1.1: (color, (u, v)) if u and v have the same color, else None --> (color, [(u, v), (u, v), ...])
    # ROUND 1.2: (color, number of triangles in the partition) --> [t1, t2, ...]
    # ROUND 2: return the exact number of triangles in the graph

    t = (RDD.map(lambda x: []

def main():
    # argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('C', help = 'number of colors/partitions', type = int)
    parser.add_argument('R', help = 'number of runs', type = int)
    parser.add_argument('file', help = 'path to .txt graph file', type = lambda x: x if os.path.isfile(x) and x.endswith(".txt") else argparse.ArgumentTypeError())
    args = parser.parse_args()
    
    # spark setup
    conf = SparkConf().setAppName('G070HW1')
    sc = SparkContext(conf = conf)
    
    # RDD setup
    docs = sc.textFile(args.file).map(lambda x: tuple(map(int, x.split(",")))).repartition(args.C).cache().distinct()
    
    # info
    print("Dataset = " + args.file)
    print("Number of Edges = " + str(docs.count()))
    print("Number of Partitions = " + str(args.C))
    print("Number of Rounds = " + str(args.R))
    
    # ALGORITHM 1
    t1 = [MR_ApproxTCwithNodeColors(docs, args.C) for i in range(args.R)]
    print("Approximation through node coloring")
    print("- Number of triangles (median over ", args.R , " runs) = ", statistics.median(t1)) 
    print("- Running time (average over ", args.R , " runs) = ", round(statistics.mean(timer) * 1000), " ms")

    # reset timer
    timer.clear()

    # ALGORITHM 2
    t2 = MR_ExactTC(docs, args.C)
    print("Exact computation through node coloring")
    print("- Number of triangles = ", t2)
    print("- Running time = ", round(timer[0] * 1000), " ms")

# main function
if __name__ == "__main__":
    main()
