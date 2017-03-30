import sqlalchemy.sql.expression as ex
from sqlalchemy.ext.compiler import compiles


def create_connection(db):
    """
    Return a new connection to a new engine, given a function that generates
    an engine
    """
    if callable(db):
        return db().connect()
    else:
        return db.connect()

def connect_and_execute(create_engine, sql):
    """
    Given an engine creator, execute a given sql statement in a separate
    connection
    """
    engine = create_engine()
    # transaction
    with engine.begin() as conn:
        conn.execute(sql)

    engine.dispose()
    return True


def make_sql_clause(s, constructor):
    if not isinstance(s, ex.ClauseElement):
        return constructor(s)
    else:
        return s


class CreateTableAs(ex.Executable, ex.ClauseElement):

    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(CreateTableAs)
def _create_table_as(element, compiler, **kw):
    return "CREATE TABLE %s AS %s" % (
        element.name,
        compiler.process(element.query)
    )


class InsertFromSelect(ex.Executable, ex.ClauseElement):

    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(InsertFromSelect)
def _insert_from_select(element, compiler, **kw):
    return "INSERT INTO %s (%s)" % (
        element.name,
        compiler.process(element.query)
    )


def to_sql_name(name):
    return name.replace('"', '')
