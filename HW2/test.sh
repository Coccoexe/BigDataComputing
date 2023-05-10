#!/bin/bash

clear && echo a1_2
spark-submit --conf spark.pyspark.python=python3 --num-executors 2 G070HW2.py 8 3 1 /data/BDC2223/orkut4M.txt > output/a1_2.txt
clear && echo a1_4
spark-submit --conf spark.pyspark.python=python3 --num-executors 4 G070HW2.py 8 3 1 /data/BDC2223/orkut4M.txt > output/a1_4.txt
clear && echo a1_8
spark-submit --conf spark.pyspark.python=python3 --num-executors 8 G070HW2.py 8 3 1 /data/BDC2223/orkut4M.txt > output/a1_8.txt
clear && echo a1_16
spark-submit --conf spark.pyspark.python=python3 --num-executors 16 G070HW2.py 8 3 1 /data/BDC2223/orkut4M.txt > output/a1_16.txt

clear && echo a0_2
spark-submit --conf spark.pyspark.python=python3 --num-executors 2 G070HW2.py 16 3 0 /data/BDC2223/orkut4M.txt > output/a0_2.txt
clear && echo a0_4
spark-submit --conf spark.pyspark.python=python3 --num-executors 4 G070HW2.py 16 3 0 /data/BDC2223/orkut4M.txt > output/a0_4.txt
clear && echo a0_8
spark-submit --conf spark.pyspark.python=python3 --num-executors 8 G070HW2.py 16 3 0 /data/BDC2223/orkut4M.txt > output/a0_8.txt
clear && echo a0_16
spark-submit --conf spark.pyspark.python=python3 --num-executors 16 G070HW2.py 16 3 0 /data/BDC2223/orkut4M.txt > output/a0_16.txt

clear && echo a0_1M
spark-submit --conf spark.pyspark.python=python3 --num-executors 8 G070HW2.py 8 3 0 /data/BDC2223/orkut1M.txt > output/a0_1M.txt
clear && echo a0_4M
spark-submit --conf spark.pyspark.python=python3 --num-executors 8 G070HW2.py 8 3 0 /data/BDC2223/orkut4M.txt > output/a0_4M.txt
clear && echo a0_16M
spark-submit --conf spark.pyspark.python=python3 --num-executors 8 G070HW2.py 8 3 0 /data/BDC2223/orkut16M.txt > output/a0_16M.txt
clear && echo a0_64M
spark-submit --conf spark.pyspark.python=python3 --num-executors 8 G070HW2.py 8 3 0 /data/BDC2223/orkut64M.txt > output/a0_64M.txt
clear && echo a0_117M
spark-submit --conf spark.pyspark.python=python3 --num-executors 8 G070HW2.py 8 3 0 /data/BDC2223/orkut117M.txt > output/a0_117M.txt
