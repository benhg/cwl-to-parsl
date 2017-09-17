import argparse
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
        """Load CWL file into dictionaries and store as instance var"""
        with open(file) as fh:
            text = fh.read()
            return yaml.load(text)

    def end(self):
        """Return multiline string representing complete code"""
        return "\n".join(self.code)

    def write(self, string):
        """Write a line of code at the correct indentaiton level"""
        self.code.append(self.tab * self.level + string)

    def start_parsl_app(self, name, inputs,  apptype='bash', dfkname='dfk'):
        """Write the decorator and function definition of a parsl app"""
        self.write('@App("{}", {})'.format(apptype, dfkname))
        self.write("def {}({}):".format(
            name, ", ".join([val for val in inputs])))
        self.indent()

    def set_environment(self, dfkname='dfk', workersname='workers',
                        executor='ThreadPoolExecutor',
                        executor_options=['max_threads=4'], imports=[]):
        """Set necessary file header details, such as
        "import parsl" and setting up a dfk"""
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
        """Set indentation level 1 higher"""
        self.level = self.level + 1

    def dedent(self):
        """Set indentation level 1 lower"""
        if self.level == 0:
            raise SyntaxError
        self.level = self.level - 1

    def declare_variable(self, name, value=""):
        """Declare a variable with name and value"""
        self.write("{} = '{}'".format(name, value))

    def add_creation_msg(self):
        """Add note to code stating what created the workflow"""
        self.write('''"""Created automatically from a Common Workflow Language {}
Using CWL version: {}"""'''.format(
            self.workflow['class'], self.workflow['cwlVersion']))

    def set_global_inputs(self):
        """Declare CWL global input as python variable"""
        self.write("#GLOBAL INPUTS")
        for item in self.workflow['inputs']:
            self.declare_variable(item['id'], value=item['type'])
            self.inputs[item['id']] = item['type']
        self.write("#END OF GLOBAL INPUTS\n")

    def set_global_outputs(self):
        """Declare CWL global output as python variable"""
        self.write("#GLOBAL OUTPUTS")
        for item in self.workflow['outputs']:
            self.declare_variable(item['id'], value=item['type'])
        self.write("#END OF GLOBAL OUTPUTS\n")

    def create_app_from_exec_step(self, step):
        """Convert execution step from CWL to Parsl bash app"""
        self.start_parsl_app(step['id'], [i['id'] for i in step['inputs']])
        base_cmd = "cmd_line = '{} ".format(step['run'])
        inputs = ['{{{}}}'.format(j['id']) for j in step['inputs']]
        self.write(base_cmd + " ".join(inputs) + "'\n")
        self.dedent()

    def create_all_apps(self):
        """Create a parsl bash app for each step in execution steps"""
        for i in self.workflow['steps']:
            self.write("# BEGIN STEP")
            self.create_app_from_exec_step(i)

    def call_step_1(self):
        """Automatically call first function with global inputs as input values.
        This expects the first task to call the rest of the tasks."""
        func = self.workflow['steps'][0]['id']
        call = "{}({})".format(func, ", ".join(
            ["'{}'".format(self.inputs[i]) for i in self.inputs.keys()]))
        self.write(call)

    def translate_workflow(self, imports=[]):
        """Helper method to translate an entire workflow with one call"""
        self.set_environment(imports=imports)
        self.set_global_inputs()
        self.set_global_outputs()
        self.create_all_apps()
        self.call_step_1()

    def dump_parsl_to_file(self, file):
        """Dump parsl code to file."""
        with open(file, 'w') as fh:
            fh.write(self.end())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-in", action="store",
                        dest="input_file", default='testflow.cwl')
    parser.add_argument("-out", action="store",
                        dest="output_file", default='cwl.log')
    args = parser.parse_args()

    gen = ParslTranslator(args.input_file)
    gen.translate_workflow(
        ["scipy", "numpy as np", "matplotlib.pyplot as plt"])
    gen.dump_parsl_to_file(args.output_file)
