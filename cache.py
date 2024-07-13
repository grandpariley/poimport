import os, json


def file_cache(filename='cache.json'):
    def decorator(function):
        def wrapper(*args, **kwargs):
            if os.path.exists(filename):
                with open(filename, 'r') as file:
                    print("Hit cache for " + filename)
                    return json.load(file)
            r = function(*args, **kwargs)
            with open(filename, 'w') as file:
                json.dump(r, file)
            return r

        return wrapper

    return decorator