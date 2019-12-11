from neo4j import GraphDatabase
import csv
import time
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from node_models import (
    Id,
    Weight,
    Height,
    Size,
    Metro,
    Price,
    Boobs,
    Neo4jRelation,
    Neo4j
)

# Just PyCharm underline bump
if Neo4j:
    pass


# Read file and get dict data
def get_dict_row(file):
    with open(file, newline='\r\n') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';', quotechar='"')
        for _dict_row in reader:
            # print(dict_row)
            yield _dict_row


if __name__ == '__main__':

    # If you want to see all CQL-statements
    # Neo4j.DEBUG = True

    # Connect with Neo4j
    _driver = GraphDatabase.driver('bolt://127.0.0.1:7687', auth=('neo4j', 'm@tr1x'))

    # I wanna have some unique constraints of nodes
    pre_defined_statements = (
        'MATCH (all) DETACH DELETE all;',
        'CREATE CONSTRAINT ON (obj:id)      ASSERT obj.value IS UNIQUE;',
        'CREATE CONSTRAINT ON (obj:height)  ASSERT obj.value IS UNIQUE;',
        'CREATE CONSTRAINT ON (obj:weight)  ASSERT obj.value IS UNIQUE;',
        'CREATE CONSTRAINT ON (obj:size)    ASSERT obj.value IS UNIQUE;',
        'CREATE CONSTRAINT ON (obj:boobs)   ASSERT obj.value IS UNIQUE;',
        'CREATE CONSTRAINT ON (obj:price)   ASSERT obj.value IS UNIQUE;',
        'CREATE CONSTRAINT ON (obj:metro)   ASSERT obj.name IS UNIQUE;'
    )

    # clear and prepare database
    with _driver.session() as session:
        for constraint in session.run("CALL db.constraints"):
            session.run("DROP " + constraint[0])
        for statement in pre_defined_statements:
            session.run(statement)
    # exit()

    # Timer for demonstration
    start_time = time.time()

    root = os.path.dirname(os.path.dirname(__file__))
    file_path = os.path.join(root, 'app', 'dataset.csv')

    with _driver.session() as _session:
        step = 1
        last_time = start_time
        _session.begin_transaction()

        for dict_row in get_dict_row(file_path):

            # print(dict_row)

            # list of id-related nodes, as star-graph of node "Id"
            related_nodes = []

            # Node named "Id"
            # save return node object and marker 'node_created' (False - exists, True - new node, None - same exception)
            id_node, id_node_created = Id(value=dict_row.get('ID')).save(_session)

            # Node named Metro
            metro, metro_node_created = Metro(name=dict_row.get('Metro')).save(_session)
            related_nodes.append((metro, metro_node_created))

            # Node named Weight
            weight, weight_node_created = Weight(value=dict_row.get('Weight')).save(_session)
            related_nodes.append((weight, weight_node_created))

            # Node named Height
            height, height_node_created = Height(value=dict_row.get('Height')).save(_session)
            related_nodes.append((height, height_node_created))

            # Node named Boobs
            boobs, boobs_node_created = Boobs(value=dict_row.get('Boobs')).save(_session)
            related_nodes.append((boobs, boobs_node_created))

            # Node named Size
            size, size_node_created = Size(value=dict_row.get('Size')).save(_session)
            related_nodes.append((size, size_node_created))

            # Node named Price
            price, price_node_created = Price(value=dict_row.get('Price')).save(_session)
            related_nodes.append((price, price_node_created))

            # get all related nodes of "Id" if related node is not None saved
            for related_node in filter(lambda x: x[1] is not None, related_nodes):
                # Some marker description of relation
                relation_description = dict(has=None)
                if isinstance(related_node[0], Metro):
                    relation_description = dict(placed=None)

                # Create relation of nodes
                Neo4jRelation().save(
                    session=_session,
                    obj=id_node,
                    related_obj=related_node[0],
                    relation_dict=relation_description,
                    is_directed=True
                )

            # TIMER
            temp_time = time.time()
            print('STEP', step,
                  ', STEP TIMER', '%.2f sec' % (temp_time - last_time),
                  ', GLOBAL TIMER', '%.2f sec' % (temp_time - start_time))
            last_time = temp_time
            step += 1
            if step % 1000:
                _session.commit_transaction()
                _session.begin_transaction()

        _session.commit_transaction()
