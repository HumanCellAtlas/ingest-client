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

class TabConfig:
    def __init__(self):
        self._dic = DotDict()
        self._key_label = {}

    def load(self, input):
        stream = open(input, 'r').read()
        yaml = yaml_load(stream)
        self._dic = DotDict(yaml)
        if "tabs" in self._dic:
            for key, value in self._dic["tabs"].items():
                dn = value["display_name"]
                self._key_label[dn.lower()] = key
                self._key_label[key] = key
        else:
            print ("warning")
        return self

    def lookup(self, key):
        return doctict.get(self._dic, key)

    def get_key_for_label(self, label):
        return self._key_label[label.lower()]

class Tab:
    def __init__(self):
        self.display_name = ""
        self.columns = []

