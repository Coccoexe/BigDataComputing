# Homework 3 - Streaming context
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
    
    # RDD setup
    docs = sc.textFile(args.file).map(lambda x: tuple(map(int, x.split(",")))).repartition(32).cache().distinct()
    
    # info
    
 

# main function
if __name__ == "__main__":
    main()
