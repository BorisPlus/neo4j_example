from neobolt.exceptions import ConstraintError


class Neo4j:
    DEBUG = False

class Neo4jNode(Neo4j):
    kwargs = dict()
    required = ()

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def save(self, driver):
        for _ in filter(lambda x: self.kwargs.get(x) is None, self.required):
            return self, None
        with driver.session() as session:
            return self, session.write_transaction(self._save, **self.kwargs)

    @classmethod
    def _save(cls, tx, **kwargs):
        setter = ', '.join('obj.%(k)s = $%(k)s ' % dict(k=k) for k in kwargs)
        statement = (
            "CREATE (obj:{obj}) "
            "SET {setter} "
            "RETURN id(obj)"
        ).format(
            setter=setter,
            obj=cls.__name__.lower()
        )
        if Neo4jNode.DEBUG:
            print('\t', statement)
        result = tx.run(statement, **kwargs)
        was_saved = False
        try:
            was_saved = result.single()
        except ConstraintError:
            return was_saved

        return False if was_saved is None else True


def f(o):
    if isinstance(o, (int, float)):
        return o
    if isinstance(o, dict):
        return '{ %s }' % ', '.join(['%s: %s' % (k, f(o[k])) for k in o])
    if o == '':
        return '\'\''
    if o is None:
        return ''
    return '\'%s\'' % str(o)


class Neo4jRelation(Neo4j):
    kw = dict()

    @staticmethod
    def _save(tx, obj: Neo4jNode, related_obj: Neo4jNode, relation_dict, is_directed=None, **kwargs):
        if is_directed is None:
            is_directed = True
        obj_description = '( %(obj_class)s:%(obj_class)s %(obj_data)s )' % dict(
            obj_class=obj.__class__.__name__.lower(),
            obj_data=f(obj.kwargs),
        )
        related_obj_description = '( %(obj_class)s:%(obj_class)s %(obj_data)s )' % dict(
            obj_class=related_obj.__class__.__name__.lower(),
            obj_data=f(related_obj.kwargs),
        )
        relation_marker_template = ':{name}{attrs}'
        was_saved = None
        for relation_name in relation_dict:
            attrs_str = f(relation_dict.get(relation_name))
            relation_marker = relation_marker_template.format(
                name=relation_name,
                attrs=' %s' % attrs_str if attrs_str else ''
            )
            statement = (
                "MATCH {obj_description} "
                "MATCH {related_obj_description} "
                "CREATE UNIQUE ({obj_marker})-[{relation_marker}]-{is_directed}({related_obj_marker}) "
                "RETURN id({obj_marker}), id({related_obj_marker})"
            ).format(
                is_directed='>' if is_directed else '',
                obj_description=obj_description,
                related_obj_description=related_obj_description,
                relation_marker=relation_marker,
                obj_marker=obj.__class__.__name__.lower(),
                related_obj_marker=related_obj.__class__.__name__.lower()
            )

            if Neo4jRelation.DEBUG:
                print('\t', statement)
            result = tx.run(statement)
            try:
                if result.single() and was_saved is None:
                    was_saved = True
                else:
                    was_saved = False
            except ConstraintError:
                was_saved = False
        return was_saved

    def save(self, driver, obj: Neo4jNode, related_obj: Neo4jNode, relation_dict, is_directed=None, **kwargs):
        with driver.session() as session:
            return session.write_transaction(
                self._save, obj=obj, related_obj=related_obj,
                relation_dict=relation_dict,
                is_directed=is_directed
            )


class IntValued(Neo4jNode):
    required = 'value',

    def __init__(self, value=None, **kwargs):
        if isinstance(value, str):
            value = value.split('.', -1)[0]
            value = int(value)
        super().__init__(value=value, **kwargs)
