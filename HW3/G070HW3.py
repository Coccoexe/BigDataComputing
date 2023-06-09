# Homework 3 - Streaming context
# Group 70 - Alessio Cocco 2087635, Andrea Valentinuzzi 2090451, Giovanni Brejc 2096046

import pyspark
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.streaming import StreamingContext
import random, os, statistics, time, argparse, threading
from collections import defaultdict

global timer, streamLength # global list to store the running time of each function
timer = []   # initialize the list
THRESHOLD = 10000000
streamLength = [0] # Stream length (an array to be passed by reference)


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
    
    # process batch
    batch = batch.map(lambda x: (int(x), 1) if int(x) >= args.left and int(x) <= args.right else None) \
            .filter(lambda x: x is not None) \
            .reduceByKey(lambda a, b: a + b) \
            .collect()
    for element, n in batch:
        frequencyMap[element] += n                                                         # true frequency
        for j in range(args.D):                                                            # for 1 <= j <= args.D
            #countSketch[j][hash_functions[j](element)] += g_functions[j] * n               # update count sketch
            countSketch[j][hash_functions[j](element)] += g_functions(element,j) * n 

# main function
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
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    stopping_condition = threading.Event()

    # stream
    portExp = int(args.portExp)
    print("Receiving data from port =", portExp)
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)

    # global variables
    p = 8191
    a = [random.randint(1, p - 1) for _ in range(args.D)]
    b = [random.randint(0, p - 1) for _ in range(args.D)]
    global countSketch
    global hash_functions
    global g_functions
    global frequencyMap
    countSketch = [[0 for _ in range(args.W)] for _ in range(args.D)]
    hash_functions = [lambda u: ((a[i] * u + b[i]) % p) % args.W for i in range(args.D)]
    g_functions = lambda u,j: (2*(hash_functions[j](u)%2)-1) * (2*(u%2)-1)

    frequencyMap = defaultdict(int)

    # process batch
    stream.foreachRDD(lambda batch: process_batch(batch, args, stopping_condition))
    
    # info
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    ssc.stop(False, True)
    print("Streaming engine stopped")
    print("END OF STREAMING\n")

    # true statistics
    f = frequencyMap
    interval = sum(f.values())
    f2 = sum([f[element] ** 2 for element in f]) / interval ** 2

    # approximate statistics
    f_approx = defaultdict(int)
    for element in f:
        temp = []
        for j in range(args.D):
            #temp.append(countSketch[j][hash_functions[j](element)] * g_functions[j])
            temp.append(countSketch[j][hash_functions[j](element)] * g_functions(element,j))
        f_approx[element] = statistics.median(temp)
    f2_approx = [sum([countSketch[j][k] ** 2 for k in range(args.W)]) / interval ** 2 for j in range(args.D)]

    # average relative error
    k_freq = defaultdict(int,sorted(f.items(), key = lambda x: x[1], reverse = True)[:args.K])
    err = defaultdict(int)
    for element in k_freq:
        print(element, f[element], f_approx[element])
        err[element] = abs(f[element] - f_approx[element]) / f[element]
    err = sum(err.values()) / len(err)

    print(k_freq)

    # print
    print("Interval size =", interval)
    print("Number of distinct elements =", len(f))
    print("Average relative error =", err)
    if args.K <= 20:
        f_top = sorted(f.values(), reverse = True)[:args.K]
        f_approx_top = sorted(f_approx, reverse = True)[:args.K]
        print("True frequencies of top-K items =", f_top)
        print("Estimated frequencies of top-K items =", f_approx_top)

if __name__ == "__main__":
    main()