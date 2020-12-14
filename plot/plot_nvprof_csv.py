#!/usr/bin/env python
# python 2.7
# Dependencies: `pip install pandas plotly`
#

from argparse import ArgumentParser
from itertools import islice
import pandas as pd
import plotly.express as px

parser = ArgumentParser("Parse csv files from nvprof")
parser.add_argument('-f', '--filename',
                    action="store", dest="filename",
                    help='filename of csv file', default=None,
                    required=True)
parser.add_argument('-s', '--saveFigure',
                    action="store_true", dest='saveFigure',
                    help="Write plot to disk as html file", default=False)
args = parser.parse_args()
filename = args.filename
print "Parsing csv file: " + filename

df = pd.read_csv(filename, header=3)

print df.head()
cycles = df['elapsed_cycles_sm']
kernelCount = cycles.size
print "Number of Kernels: " + str(kernelCount)

#pd.options.plotting.backend = "plotly"
fig = px.scatter(cycles, title=filename)
fig.update_layout(xaxis_title="Kernel Index")
if (args.saveFigure):
    fig.write_html(filename+"_Figure.html")
fig.show()

