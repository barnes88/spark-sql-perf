#!/usr/bin/bash

from subprocess import run
import os

queries = [*range(1, 23)]
path = "./nvprof_TPCH/"
if not os.path.isdir(path):
    os.makedirs(path)
for query in queries:
    print("Running Query q" + str(query))
    filename = path + str(query) + ".txt"
    f = open(filename, "w")
    try:
        output = run(["nvprof --profile-child-processes --metrics ipc --csv --log-file Gpu_Tpch_%p_q"+str(query)+".csv --profile-from-start off ./sbt_launch.sh Gpu_Tpch "+ str(query)+" "+str(query)], stdout=f, shell=True, timeout=24*60*60)
    except:
        print("QUERY " + str(query) +" failed")
    finally:
        f.close()
