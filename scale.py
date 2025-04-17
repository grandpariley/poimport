import math

import db

SCALED_OBJECTIVES = ['cvar', 'var', 'return', 'environment', 'social', 'governance']
MINIMIZE_OBJECTIVES = ['cvar', 'var']


def standardize(extremes, data, objective):
    if data[objective] is None:
        return None
    return float(float(data[objective]) - float(extremes[objective]['min'])) / float(
        float(extremes[objective]['max']) - float(extremes[objective]['min']))


async def scale():
    in_data = await db.fetch_data()
    extremes = get_extreme_values(in_data)
    for k, v in in_data.items():
        for o in SCALED_OBJECTIVES:
            v[o] = standardize(extremes, v, o)
            await db.update_data(k, v)


def get_extreme_values(in_data):
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
