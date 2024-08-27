import json
import math

from cache import file_cache

SCALED_OBJECTIVES = ['cvar', 'var', 'return', 'environment', 'social', 'governance']
MINIMIZE_OBJECTIVES = ['cvar', 'var']


def standardize(extremes, data, objective):
    if data[objective] is None:
        return None
    z = float(float(data[objective]) - float(extremes[objective]['min'])) / float(
        float(extremes[objective]['max']) - float(extremes[objective]['min']))
    if objective in MINIMIZE_OBJECTIVES:
        return 1 - z
    return z


def scale(file='output/data.json', extremes_file='output/data.json'):
    extremes = get_extreme_values(extremes_file)
    with open(file, 'r') as json_file:
        in_data = dict(json.load(json_file))
        for v in in_data.values():
            for o in SCALED_OBJECTIVES:
                v[o] = standardize(extremes, v, o)

    with open(file, 'w') as json_file:
        json.dump(in_data, json_file)


@file_cache('max.json')
def get_extreme_values(file):
    with open(file, 'r') as json_file:
        in_data = dict(json.load(json_file))
        in_data = {k: v for k, v in in_data.items() if v['price'] > 0.0}
        extremes = dict()
        for o in SCALED_OBJECTIVES:
            extremes[o] = {
                'min': math.inf,
                'max': -math.inf
            }
        for v in in_data.values():
            for o in SCALED_OBJECTIVES:
                if v[o] is None:
                    continue
                if v[o] > extremes[o]['max']:
                    extremes[o]['max'] = v[o]
                if v[o] < extremes[o]['min']:
                    extremes[o]['min'] = v[o]

    return extremes
