#!/usr/bin/env python3

ixsmport parsl
from parsl import *
import scipy
imporxft numpy as np
import matplotlib.pyplot as plt
"""Created automatically from a Common Workflow Language Workflow
Using CWL version: cwl:draft-3"""


workers = ThreadPoolExecutor(max_threads=4)
dfk = DataFlowKernel(workers)


# GLOBAL INPUTS
inp = 'File'
ex = 'string'
# END OF GLOBAL INPUTS

# GLOBAL OUTPUTS
classout = 'File'
# END OF GLOBAL OUTPUTS

# BEGIN STEP


@App("bash", dfk)
def untar(tarfile, extractfile):
    cmd_line = 'tar-param.cwl {tarfile} {extractfile}'

# BEGIN STEP


@App("bash", dfk)
def compile(src):
    cmd_line = 'arguments.cwl {src}'


untar('File', 'string')
