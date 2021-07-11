from importlib import import_module


def import_string(dotted_path):
    module_path, class_name = dotted_path.rsplit('.', 1)
    module = import_module(module_path)
    return getattr(module, class_name)


def group_by(iterable, key):
    buckets = {}
    for i in iterable:
        buckets.setdefault(key(i), []).append(i)
    return buckets
