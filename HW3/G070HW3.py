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
    batch_size = batch.count()
    streamLength[0] += batch_size

    # threshold
    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()
        return

    # [left, right]
    if streamLength[0] <= args.left and streamLength[0] >= args.right:
        return
    
    

    # empty args.D x args.W matrix
    countSketch = [[0 for _ in range(args.W)] for _ in range(args.D)]
    for element in batch.collect():
        element = int(element)
        for i in range(args.D):
                p = 8191
                a = random.randint(1, p - 1)
                b = random.randint(0, p - 1)
                C = 20 # ???
                h = lambda u: ((a * u + b) % p) % C
                # add random +- 1
                countSketch[i][h(element)] += (2 * random.randint(0, 1) - 1)

    #FINO A QUI SEMBRA GIUSTO
    #MEDIANA SBAGLIATA GUARDARE SLIDE (per ogni elemento...) e poi mi sembra usi le stesse hash, quindi da capire dove generare i numeri random

    # compute f = (2 * random.randint(0, 1) - 1) * countSketch[i][h(element)]
    f = []
    for i in range(args.D):
        p = 8191
        a = random.randint(1, p - 1)
        b = random.randint(0, p - 1)
        C = 20 # ???
        h = lambda u: ((a * u + b) % p) % C
        f.append((2 * random.randint(0, 1) - 1) * countSketch[i][h(element)])

    # compute the median of f
    print(statistics.median(f))

    # f2
    f2 = []
    for i in range(args.D):
        f2.append(sum([countSketch[i][j] ** 2 for j in range(args.W)]))
    
    # compute the median of f2
    print(statistics.median(f2))

















    

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

    # info
    portExp = int(args.portExp)
    print("Receiving data from port =", portExp)

    

    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda batch: process_batch(batch, args, stopping_condition))

    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    print("END OF STREAMING")

if __name__ == "__main__":
    main()