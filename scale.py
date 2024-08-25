import json
import math

from cache import file_cache


def scale(in_file='output/data.json', out_file='output/data.json'):
    extremes = get_extreme_values(in_file)
    with open(out_file, 'r') as json_file:
        out_data = dict(json.load(json_file))
        for k in out_data:
            if out_data[k]['cvar']:
                out_data[k]['cvar'] = (out_data[k]['cvar'] - float(extremes['cvar']['min'])) / (
                            float(extremes['cvar']['max']) - float(extremes['cvar']['min']))
            if out_data[k]['var']:
                out_data[k]['var'] = (out_data[k]['var'] - float(extremes['var']['min'])) / (
                            float(extremes['var']['max']) - float(extremes['var']['min']))
            if out_data[k]['return']:
                out_data[k]['return'] = (out_data[k]['return'] - float(extremes['return']['min'])) / (
                            float(extremes['return']['max']) - float(extremes['return']['min']))
            if out_data[k]['environment']:
                out_data[k]['environment'] = (out_data[k]['environment'] - float(extremes['environment']['min'])) / (
                            float(extremes['environment']['max']) - float(extremes['environment']['min']))
            if out_data[k]['social']:
                out_data[k]['social'] = (out_data[k]['social'] - float(extremes['social']['min'])) / (
                            float(extremes['social']['max']) - float(extremes['social']['min']))
            if out_data[k]['governance']:
                out_data[k]['governance'] = (out_data[k]['governance'] - float(extremes['governance']['min'])) / (
                            float(extremes['governance']['max']) - float(extremes['governance']['min']))

    with open(out_file, 'w') as json_file:
        json.dump(out_data, json_file)


@file_cache('max.json')
def get_extreme_values(in_file):
    with open(in_file, 'r') as json_file:
        in_data = dict(json.load(json_file))
        in_data = {k: v for k, v in in_data.items() if v['price'] > 0.0}
        extremes = {
            'cvar': {
                'min': math.inf,
                'max': -math.inf
            },
            'var': {
                'min': math.inf,
                'max': -math.inf
            },
            'return': {
                'min': math.inf,
                'max': -math.inf
            },
            'environment': {
                'min': math.inf,
                'max': -math.inf
            },
            'social': {
                'min': math.inf,
                'max': -math.inf
            },
            'governance': {
                'min': math.inf,
                'max': -math.inf
            },
        }
        for v in in_data.values():
            if v['cvar'] and v['cvar'] > extremes['cvar']['max']:
                extremes['cvar']['max'] = v['cvar']
            if v['var'] and v['var'] > extremes['var']['max']:
                extremes['var']['max'] = v['var']
            if v['return'] and v['return'] > extremes['return']['max']:
                extremes['return']['max'] = v['return']
            if v['environment'] and v['environment'] > extremes['environment']['max']:
                extremes['environment']['max'] = v['environment']
            if v['social'] and v['social'] > extremes['social']['max']:
                extremes['social']['max'] = v['social']
            if v['governance'] and v['governance'] > extremes['governance']['max']:
                extremes['governance']['max'] = v['governance']
            if v['cvar'] and v['cvar'] < extremes['cvar']['min']:
                extremes['cvar']['min'] = v['cvar']
            if v['var'] and v['var'] < extremes['var']['min']:
                extremes['var']['min'] = v['var']
            if v['return'] and v['return'] < extremes['return']['min']:
                extremes['return']['min'] = v['return']
            if v['environment'] and v['environment'] < extremes['environment']['min']:
                extremes['environment']['min'] = v['environment']
            if v['social'] and v['social'] < extremes['social']['min']:
                extremes['social']['min'] = v['social']
            if v['governance'] and v['governance'] < extremes['governance']['min']:
                extremes['governance']['min'] = v['governance']
    return extremes