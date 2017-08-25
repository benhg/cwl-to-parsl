import sys
import string
import yaml


class ParslTranslator:

    def __init__(self, file, tab="    "):
        self.level = 0
        self.code = []
        self.inputs = {}
        self.tab = tab
        self.level = 0
        self.workflow = self.load_workflow(file)

    def load_workflow(self, file):
        with open(file) as fh:
            text = fh.read()
            return yaml.load(text)

    def end(self):
        return "\n".join(self.code)

    def write(self, string):
        self.code.append(self.tab * self.level + string)

    def start_parsl_app(self, name, inputs,  apptype='bash', dfkname='dfk'):
        self.write('@App("{}", {})'.format(apptype, dfkname))
        self.write("def {}({}):".format(
            name, ", ".join([val for val in inputs])))
        self.indent()

    def set_environment(self, dfkname='dfk', workersname='workers', executor='ThreadPoolExecutor', executor_options=['max_threads=4'], imports=[]):
        self.code.extend(
            ["#!/usr/bin/env python3\n", "import parsl", "from parsl import *"])
        self.code.extend(["import " + i for i in imports])
        self.add_creation_msg()
        self.code.append("\n")
        self.code.append("{} = {}({})".format(
            workersname, executor, ", ".join([opt for opt in executor_options])))
        self.code.append(
            "{} = DataFlowKernel({})".format(dfkname, workersname))
        self.code.append("\n")

    def indent(self):
        self.level = self.level + 1

    def dedent(self):
        if self.level == 0:
            raise SyntaxError
        self.level = self.level - 1

    def declare_variable(self, name, value=""):
        self.write("{} = '{}'".format(name, value))

    def add_creation_msg(self):
        self.write('''"""Created automatically from a Common Workflow Language {}
Using CWL version: {}"""'''.format(
            self.workflow['class'], self.workflow['cwlVersion']))

    def set_global_inputs(self):
        self.write("#GLOBAL INPUTS")
        for item in self.workflow['inputs']:
            self.declare_variable(item['id'], value=item['type'])
            self.inputs[item['id']] = item['type']
        self.write("#END OF GLOBAL INPUTS\n")

    def set_global_outputs(self):
        self.write("#GLOBAL OUTPUTS")
        for item in self.workflow['outputs']:
            self.declare_variable(item['id'], value=item['type'])
        self.write("#END OF GLOBAL OUTPUTS\n")

    def create_app_from_exec_step(self, step):
        self.start_parsl_app(step['id'], [i['id'] for i in step['inputs']])
        base_cmd = "cmd_line = '{} ".format(step['run'])
        inputs = ['{{{}}}'.format(j['id']) for j in step['inputs']]
        self.write(base_cmd + " ".join(inputs) + "'\n")
        self.dedent()

    def create_all_apps(self):
        for i in self.workflow['steps']:
            self.write("# BEGIN STEP")
            self.create_app_from_exec_step(i)

    def call_step_1(self):
        func = self.workflow['steps'][0]['id']
        call = "{}({})".format(func, ", ".join(
            ["{}='{}'".format(i, self.inputs[i]) for i in self.inputs.keys()]))
        self.write(call)

    def translate_workflow(self, imports=[]):
        gen.set_environment(imports=imports)
        gen.set_global_inputs()
        gen.set_global_outputs()
        gen.create_all_apps()
        gen.call_step_1()

    def dump_parsl_to_file(self, file):
        with open(file, 'w') as fh:
            fh.write(self.end())


if __name__ == '__main__':
    gen = ParslTranslator("testflow.cwl")
    gen.translate_workflow(
        ["scipy", "numpy as np", "matplotlib.pyplot as plt"])
    gen.dump_parsl_to_file("cwl.log")
