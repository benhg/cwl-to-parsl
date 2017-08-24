import sys
import string
import yaml


class ParslTranslator:

    def __init__(self, file, tab="    "):
        self.level = 0
        self.code = []
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
            ["#!/usr/bin/env python3", "\n", "import parsl", "from parsl import *"])
        self.code.extend(["import " + i for i in imports])
        self.code.append("\n")
        self.code.append("{} = {}({})".format(
            workersname, executor, ", ".join([opt for opt in executor_options])))
        self.code.append(
            "{} = DataFlowKernel({})".format(dfkname, workersname))
        self.add_creation_msg()
        self.code.append("\n")

    def indent(self):
        self.level = self.level + 1

    def dedent(self):
        if self.level == 0:
            raise SyntaxError
        self.level = self.level - 1

    def declare_variable(self, name, value=""):
        self.write("{} = {}".format(name, value))

    def add_creation_msg(self):
        self.write('''"""Created from a Common Workflow Language {}
Using CWL version: {}"""'''.format(
            self.workflow['class'], self.workflow['cwlVersion']))

    def set_global_inputs(self):
        for item in self.workflow['inputs']:
            gen.declare_variable(item['id'], value=item['type'])


if __name__ == '__main__':
    gen = ParslTranslator("testflow.cwl")
    gen.set_environment(imports=['numpy as np', 'scipy'])
    gen.set_global_inputs()
    gen.start_parsl_app("test_app", ["input1", "input2"])
    print(gen.end())
