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


'''creates custom genome from reference genome and two phased VCF files SNPs and Indels'''
# GLOBAL INPUTS
reference = 'File'
phasedsnps = 'File'
phasedindels = 'File'
strain = 'string'
filename = 'string'
# END OF GLOBAL INPUTS

# GLOBAL OUTPUTS
outfile = 'File'
# END OF GLOBAL OUTPUTS

# BEGIN STEP


@App("bash", dfk)
def applysnps(reference, phased, strain, output_filename):
    cmd_line = '../../tools/alea-insilico.cwl {reference} {phased} {strain} {output_filename}'

# BEGIN STEP


@App("bash", dfk)
def applyindels(reference, phased, strain, output_filename):
    cmd_line = '../../tools/alea-insilico.cwl {reference} {phased} {strain} {output_filename}'


applysnps('File', 'File', 'File', 'string', 'string')
