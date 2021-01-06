#!/usr/bin/bash

from subprocess import run
import os
from datetime import datetime

queries = [*range(1, 23)]
path = "./nvprof_TPCH/"
if not os.path.isdir(path):
    os.makedirs(path)
for query in queries:
    print("Running Query q" + str(query))
    print("Started at " + datetime.today().strftime('%Y-%m-%d-%H:%M:%S'))
    filename = path + str(query) + ".txt"
    f = open(filename, "w")
    try:
        output = run(["nvprof --profile-child-processes --events elapsed_cycles_sm --metrics ipc --csv --log-file Gpu_Tpch_q"+str(query)+"_%p.csv --profile-from-start off ./java_launch.sh Gpu_Tpch "+ str(query)+" "+str(query)], stdout=f, shell=True, timeout=24*60*60)
    except:
        print("QUERY " + str(query) +" failed")
    finally:
        f.close()
