from . import base


class Metro(base.Neo4jNode):
    required = 'name',

    def __init__(self, name=None, **kwargs):
        super().__init__(name=name, **kwargs)