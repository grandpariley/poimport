import json

from scale import SCALED_OBJECTIVES


def validate_max():
    with open('max.json', 'r') as max_file, open('output/raw/data.json', 'r') as data_file:
        extremes = json.load(max_file)
        data = dict(json.load(data_file))
        for v in data.values():
            for objective in SCALED_OBJECTIVES:
                if v[objective] and v[objective] > extremes[objective]['max']:
                    raise ValueError(objective + ' max incorrect: ' + v['ticker'] + ' has value ' + str(v[objective]) + ' > ' + str(extremes[objective]['max']))
                if v[objective] and v[objective] < extremes[objective]['min']:
                    raise ValueError(objective + ' min incorrect: ' + v['ticker'] + ' has value ' + str(v[objective]) + ' < ' + str(extremes[objective]['min']))


def validate_scale():
    with open('output/data.json', 'r') as data_file:
        data = dict(json.load(data_file))
        objective_counts = {
            'cvar': {'one': 0, 'zero': 0},
            'var': {'one': 0, 'zero': 0},
            'return': {'one': 0, 'zero': 0},
            'environment': {'one': 0, 'zero': 0},
            'social': {'one': 0, 'zero': 0},
            'governance': {'one': 0, 'zero': 0}
        }
        for v in data.values():
            for objective in SCALED_OBJECTIVES:
                if v[objective] is None:
                    continue
                if v[objective] == 1:
                    objective_counts[objective]['one'] += 1
                elif v[objective] == 0:
                    objective_counts[objective]['zero'] += 1
                elif v[objective] > 1:
                    raise ValueError(objective + ' for ' + v['ticker'] + ' too large: ' + str(v[objective]))
                elif v[objective] < 0:
                    raise ValueError(objective + ' for ' + v['ticker'] + ' too small: ' + str(v[objective]))





def validate():
    validate_max()
    validate_scale()
