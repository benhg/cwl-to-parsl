#!/usr/bin/env python3

import parsl
from parsl import *
import scipy
import numpy as np
import matplotlib.pyplot as plt
"""Created automatically from a Common Workflow Language Workflow
Using CWL version: v1.0"""


workers = ThreadPoolExecutor(max_threads=4)
dfk = DataFlowKernel(workers)


'''None'''
#GLOBAL INPUTS
infile = 'infile'
outfile_name = 'outfile_name'
#END OF GLOBAL INPUTS

#GLOBAL OUTPUTS
revcomp_dnafile = 'revcomp_dnafile'
#END OF GLOBAL OUTPUTS

# BEGIN STEP
@App("bash", dfk)
def reverse(dnafile):
    cmd_line = 'reverse.cwl {dnafile}'

# BEGIN STEP
@App("bash", dfk)
def complement(dnafile):
    cmd_line = 'complement.cwl {dnafile}'

# BEGIN STEP
@App("bash", dfk)
def rename(infile, outfile_name):
    cmd_line = '{'class': 'ExpressionTool', 'inputs': {'infile': {'type': 'File'}, 'outfile_name': {'type': 'string?'}}, 'outputs': {'outfile': 'File'}, 'expression': '${ var outfile = inputs.infile; if (inputs.outfile_name) {\n  outfile.basename = inputs.outfile_name;\n} return { "outfile": outfile }; }\n'} {infile} {outfile_name}'

reverse('infile', 'outfile_name')