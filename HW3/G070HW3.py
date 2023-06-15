# Homework 3 - Streaming context
# Group 70 - Alessio Cocco 2087635, Andrea Valentinuzzi 2090451, Giovanni Brejc 2096046

import pyspark
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.streaming import StreamingContext
import random, os, statistics, time, argparse, threading
from collections import defaultdict

global timer, streamLength # global list to store the running time of each function
timer = []                 # initialize the list
streamLength = [0]         # stream length (an array to be passed by reference)
THRESHOLD = 10000000       # threshold for the stream length

def process_batch(batch, args, stopping_condition):
    """ Process a batch of data. If the stream length is greater than the threshold, set the stopping condition to True.

    Args:
        batch (RDD): Batch of data
        args (argparse.Namespace): Arguments
        stopping_condition (threading.Event): Stopping condition

    Returns:
        None

    Usage:
        >>> process_batch(batch, args, stopping_condition)
    """

    batch_size = batch.count()
    if not batch_size:
        return
    streamLength[0] += batch_size
    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()
        return
    
    batch = batch.map(lambda x: (int(x), 1) if int(x) >= args.left and int(x) <= args.right else None) \
            .filter(lambda x: x is not None) \
            .reduceByKey(lambda a, b: a + b) \
            .collect()                                                                # batch = [(element, n), ...]

    for element, n in batch:
        frequencyMap[element] += n                                                    # true frequency
        for j in range(args.D):       
            countSketch[j][hash_functions[j](element)] += g_functions[j](element) * n # count sketch

def main():
    # argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('D', help = 'number of rows of the count sketch', type = int)
    parser.add_argument('W', help = 'number of columns of the count sketch', type = int)
    parser.add_argument('left', help = 'left endpoint of the interval of interest', type = int)
    parser.add_argument('right', help = 'right endpoint of the interval of interest', type = int)
    parser.add_argument('K', help = 'number of top frequent items of interest', type = int)
    parser.add_argument('portExp', help = 'port number', type = int)
    args = parser.parse_args()
    
    # spark setup
    conf = SparkConf().setAppName('G070HW3')
    conf.setMaster("local[*]")
    conf.set("spark.locality.wait", "0s")
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1s
    ssc.sparkContext.setLogLevel("ERROR")
    stopping_condition = threading.Event()

    # stream
    portExp = int(args.portExp)
    print("Receiving data from port =", portExp)
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)

    # global variables
    p = 8191                                               # as defined in the assignment
    a = [random.randint(1, p - 1) for _ in range(args.D)]  # random numbers in [1, p - 1]
    b = [random.randint(0, p - 1) for _ in range(args.D)]  # random numbers in [0, p - 1]
    a_ = [random.randint(1, p - 1) for _ in range(args.D)] # random numbers in [1, p - 1]
    b_ = [random.randint(0, p - 1) for _ in range(args.D)] # random numbers in [0, p - 1]
    global frequencyMap                                    # frequency map
    global countSketch                                     # count sketch
    global hash_functions                                  # hash functions
    global g_functions                                     # g functions
    frequencyMap = defaultdict(int)
    countSketch = [[0 for _ in range(args.W)] for _ in range(args.D)]
    hash_functions = [lambda u, a = a[i], b = b[i] : ((a * u + b) % p) % args.W for i in range(args.D)]
    g_functions = [lambda u, a = a_[i], b = b_[i] : 2 * (((a * u + b) % p) % 2) - 1 for i in range(args.D)]
    
    # considered g_functions
    #g_functions = lambda u, j: ((2 * (hash_functions[j](u) % 2) - 1) * (2 * (u % 2) - 1))
    #g_functions = lambda u, j: 2 * ((u % (j + hash_functions[j](u) + 1)) % 2) - 1
    #g_functions = lambda u, j: 2 * ((u + hash_functions[j](u)) % 2) - 1

    # random element for each row and column (ideal, but high memory usage)
    #g_ = [[2 * random.randint(0, 1) - 1 for _ in range(args.W)] for _ in range(args.right - args.left + 1)]
    #g_functions = lambda u, j: g_[u - args.left][hash_functions[j](u)]

    # process batch
    stream.foreachRDD(lambda batch: process_batch(batch, args, stopping_condition))
    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, True)

    # true statistics
    f = frequencyMap                                             # true frequency
    interval = sum(f.values())                                   # interval length
    f2 = sum([f[element] ** 2 for element in f]) / interval ** 2 # true second moment

    f_approx = defaultdict(int)
    for element in f:
        temp = []
        for j in range(args.D):
            temp.append(countSketch[j][hash_functions[j](element)] * g_functions[j](element))
        f_approx[element] = statistics.median(temp)

    f2_approx = statistics.median([sum([countSketch[j][k] ** 2 for k in range(args.W)]) for j in range(args.D)]) / interval ** 2 # approximate second moment

    # average relative error
    k_freq = defaultdict(int, sorted(f.items(), key = lambda x: x[1], reverse = True)[:args.K])
    err = defaultdict(int)
    for element in k_freq:
        err[element] = abs(f[element] - f_approx[element]) / f[element]
    err = sum(err.values()) / len(err)

    # print
    print(f"D = {args.D} W = {args.W} [left,right] = [{args.left},{args.right}] K = {args.K} Port = {args.portExp}")
    print(f"Total number of items = {streamLength[0]}")
    print(f"Total number of items in = [{args.left},{args.right}] = {interval}")
    print(f"Number of distinct elements in [{args.left},{args.right}] = {len(f)}")
    if args.K <= 20:
        for element in k_freq:
            print(f"Item {element} Freq = {f[element]} Est. Freq = {f_approx[element]}")
    print(f"Avg err for top {args.K} = {err}")
    print(f"F2 {f2} F2 Estimate {f2_approx}")

if __name__ == "__main__":
    main()