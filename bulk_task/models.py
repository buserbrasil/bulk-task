from .utils import import_string


class Args:
    def __init__(self, model, args, kwargs=None):
        self.model = model
        self.args = tuple(args)
        if kwargs is None:
            kwargs = {}
        self.kwargs = kwargs

    @classmethod
    def deserialize(cls, data):
        model_cls, params = import_string(data[0]), data[1]
        return cls(model_cls, params['args'], params['kwargs'])

    def serialize(self):
        params = {'args': self.args, 'kwargs': self.kwargs}
        return f'{self.model.__module__}.{self.model.__name__}', params

    def as_model(self):
        return self.model(*self.args, **self.kwargs)


class Job:
    def __init__(self, func, args):
        self.func = func
        self.args = args

    @classmethod
    def deserialize(cls, data):
        func, args = import_string(data[0]), Args.deserialize(data[1])
        return cls(func, args)

    def serialize(self):
        return (
            f'{self.func.__module__}.{self.func.__name__}',
            self.args.serialize(),
        )
