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
    if streamLength[0] <= args.left and streamLength[0] >= args.right:
        return

    # process batch
    batch = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda a, b: a + b).collect()
    for element, n in batch:
        frequencyMap[element] += n # true frequency
        for i in range(args.D):    # count sketch
            countSketch[i][hash_functions[i](element)] += n * (2 * random.randint(0, 1) - 1)
    interval[0] += batch_size         # interval size

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
    global countSketch
    global hash_functions
    global interval
    global frequencyMap
    countSketch = [[0 for _ in range(args.W)] for _ in range(args.D)]
    hash_functions = [lambda u: ((random.randint(1, p - 1) * u + random.randint(0, p - 1)) % p) % args.W for _ in range(args.D)]
    interval = [0]
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
    f2 = sum([f[element] ** 2 for element in f]) / interval[0] ** 2

    # approximate statistics
    f_approx = [(2 * random.randint(0, 1) - 1) * sum([countSketch[i][hash_functions[i](element)] for i in range(args.D)]) for element in f]
    f2_approx = [sum([countSketch[i][j] ** 2 for j in range(args.W)]) / interval[0] ** 2 for i in range(args.D)]

    # average relative error
    err = [f_approx[i] for i in range(len(f_approx)) if f[i] >= sorted(f.values(), reverse = True)[args.K - 1]]
    err = sum([abs(err[i] - f[i]) / f[i] for i in range(len(err))]) / len(err)

    # print
    print("Interval size =", interval[0])
    print("Number of distinct elements =", len(f))
    print("Average relative error =", err)
    if args.K <= 20:
        f_top = sorted(f.values(), reverse = True)[:args.K]
        f_approx_top = sorted(f_approx, reverse = True)[:args.K]
        print("True frequencies of top-K items =", f_top)
        print("Estimated frequencies of top-K items =", f_approx_top)

if __name__ == "__main__":
    main()