#!/usr/bin/env python
"""
Description goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"
__date__ = "04/05/2018"

from yaml import load as yaml_load
from ingest.utils import doctict
from ingest.utils.doctict import DotDict

class TabParser:
    def __init__(self, file=None):
        self.dic = DotDict()
        self.key_label = {}
        if file:
            self.dic = self.load_template(file)

    def load_template(self, tabs_yaml):
        stream = open(tabs_yaml, 'r').read()
        yaml = yaml_load(stream)
        dic = DotDict(yaml)

        if "tabs" in dic:
            for key, value in dic["tabs"].items():
                dn = value["display_name"]
                self.key_label[dn.lower()] = key
                self.key_label[key] = key
        else:
            print ("warning")
        return dic

    def lookup(self, key):
        return doctict.get(self.dic, key)

    def get_key_for_label(self, label):
        return self.key_label[label.lower()]

class Tab:
    def __init__(self):
        self.display_name = ""
        self.columns = []

