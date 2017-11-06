import json
import collections
import numbers
import sys

# flattens json
#
# thing -> the thing to flatten, either an object(i.e dict), list or atomic type (i.e string, bool, int)
# keyPrefix -> the key prefix to append to when flattening
# flatAgg -> the flattened object so far
def flatten(thing, keyPrefix, flatAgg):
    if isAtomicType(thing): # we don't have to flatten further if it's atomic
        flatAgg[keyPrefix] = thing
    elif type(thing) == list:
        flattenList(thing, keyPrefix, flatAgg)
    elif type(thing) == dict:
        flattenObj(thing, keyPrefix, flatAgg)

def flattenObj(obj, keyPrefix, flatAgg):
    for (k, v) in obj.items():
        newKeyPrefix = k if keyPrefix == "" else keyPrefix + "." + k
        flatten(v, newKeyPrefix, flatAgg)

def flattenList(lst, keyPrefix, flatAgg):
    for index, elm in enumerate(lst):
        newKeyPrefix = keyPrefix + "[" + str(index) + "]"
        flatten(elm, newKeyPrefix, flatAgg)
        
def isAtomicType(thing):     
    return isinstance(thing, basestring) or isinstance(thing, numbers.Number) or type(thing) == bool

def run(objectToFlatten):
    flattenedObject = collections.OrderedDict(); # maintains insertion order, which looks neater
    flatten(objectToFlatten, "", flattenedObject) # initialize the flatten() with an empty string key prefix
    return flattenedObject
