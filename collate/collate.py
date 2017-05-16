# -*- coding: utf-8 -*-
from .sql import execute_insert
from numbers import Number
from itertools import product, chain
import sqlalchemy.sql.expression as ex
import re

from joblib import Parallel, delayed

from .sql import make_sql_clause, to_sql_name, CreateTableAs, InsertFromSelect


def make_list(a):
    return [a] if not isinstance(a, list) else a


def make_tuple(a):
    return (a,) if not isinstance(a, tuple) else a


DISTINCT_REGEX = re.compile(r"distinct[ (]")


def split_distinct(quantity):
    # Only support distinct clauses with one-argument quantities
    if len(quantity) != 1:
        return ('', quantity)
    q = quantity[0]
    if DISTINCT_REGEX.match(q):
        return "distinct ", (q[8:].lstrip(" "),)
    else:
        return "", (q,)


class AggregateExpression(object):
    def __init__(self, aggregate1, aggregate2, operator,
                 cast=None, operator_str=None, expression_template=None):
        """
        Args:
            aggregate1: first aggregate
            aggregate2: second aggregate
            operator: string of SQL operator, e.g. "+"
            cast: optional string to put after aggregate1, e.g. "*1.0", "::decimal"
            operator_str: optional name of operator to use, defaults to operator
            expression_template: optional formatting template with the following keywords:
                name1, operator, name2
        """
        self.aggregate1 = aggregate1
        self.aggregate2 = aggregate2
        self.operator = operator
        self.cast = cast if cast else ""
        self.operator_str = operator if operator_str else operator
        self.expression_template = expression_template \
            if expression_template else "{name1}{operator}{name2}"

    def alias(self, expression_template):
        """
        Set the expression template used for naming columns of an AggregateExpression
        Returns: self, for chaining
        """
        self.expression_template = expression_template
        return self

    def get_columns(self, when=None, prefix=None, format_kwargs=None):
        if prefix is None:
            prefix = ""
        if format_kwargs is None:
            format_kwargs = {}

        columns1 = self.aggregate1.get_columns(when)
        columns2 = self.aggregate2.get_columns(when)

        for c1, c2 in product(columns1, columns2):
            c = ex.literal_column("({}{} {} {})".format(
                    c1, self.cast, self.operator, c2))
            yield c.label(prefix + self.expression_template.format(
                    name1=c1.name, operator=self.operator_str, name2=c2.name,
                    **format_kwargs))

    def __add__(self, other):
        return AggregateExpression(self, other, "+")

    def __sub__(self, other):
        return AggregateExpression(self, other, "-")

    def __mul__(self, other):
        return AggregateExpression(self, other, "*")

    def __div__(self, other):
        return AggregateExpression(self, other, "/", "*1.0")

    def __truediv__(self, other):
        return AggregateExpression(self, other, "/", "*1.0")

    def __lt__(self, other):
        return AggregateExpression(self, other, "<")

    def __le__(self, other):
        return AggregateExpression(self, other, "<=")

    def __eq__(self, other):
        return AggregateExpression(self, other, "=")

    def __ne__(self, other):
        return AggregateExpression(self, other, "!=")

    def __gt__(self, other):
        return AggregateExpression(self, other, ">")

    def __ge__(self, other):
        return AggregateExpression(self, other, ">=")

    def __or__(self, other):
        return AggregateExpression(self, other, "or", operator_str="|")

    def __and__(self, other):
        return AggregateExpression(self, other, "and", operator_str="&")


class Aggregate(AggregateExpression):
    """
    An object representing one or more SQL aggregate columns in a groupby
    """

    def __init__(self, quantity, function, order=None):
        """
        Args:
            quantity: SQL for the quantity to aggregate
            function: SQL aggregate function
            order: SQL for order by clause in an ordered set aggregate

        Notes:
            quantity, function, and order can also be lists of the above,
            in which case the cross product of those is used. If quantity is a
            collection than name should also be a collection of the same length.

            quantity can be a tuple of SQL quantities for aggregate functions
            that take multiple arguments, e.g. corr, regr_slope

            quantity can be a dictionary in which case the keys are names
            for the expressions and values are expressions.
        """
        if isinstance(quantity, dict):
            # make quantity values tuples
            self.quantities = {k: make_tuple(q) for k, q in quantity.items()}
        else:
            # first convert to list of tuples
            quantities = [make_tuple(q) for q in make_list(quantity)]
            # then dict with name keys
            self.quantities = {to_sql_name(str.join("_", q)): q for q in quantities}

        self.functions = make_list(function)
        self.orders = make_list(order)

    def get_columns(self, when=None, prefix=None, format_kwargs=None):
        """
        Args:
            when: used in a case statement to filter the rows going into the
                aggregation function
            prefix: prefix for column names
            format_kwargs: kwargs to pass to format the aggregate quantity
        Returns:
            collection of SQLAlchemy columns
        """
        if prefix is None:
            prefix = ""
        if format_kwargs is None:
            format_kwargs = {}

        name_template = "{prefix}{quantity_name}_{function}"
        column_template = "{function}({distinct}{args}){order_clause}{filter}"
        arg_template = "{quantity}"
        order_template = ""
        filter_template = ""

        if self.orders != [None]:
            order_template += " WITHIN GROUP (ORDER BY {order})"
        if when:
            filter_template = " FILTER (WHERE {when})"

        for function, (quantity_name, quantity), order in product(
                self.functions, self.quantities.items(), self.orders):
            distinct, quantity = split_distinct(quantity)
            args = str.join(", ", (arg_template.format(quantity=q)
                                   for q in quantity))
            order_clause = order_template.format(order=order)
            filter = filter_template.format(when=when)

            if order is not None:
                if len(quantity_name) > 0:
                    quantity_name += '_'
                quantity_name += to_sql_name(order)

            kwargs = dict(function=function, args=args, prefix=prefix,
                          distinct=distinct, order_clause=order_clause,
                          quantity_name=quantity_name, filter=filter, **format_kwargs)

            column = column_template.format(**kwargs).format(**format_kwargs)
            name = name_template.format(**kwargs)

            yield ex.literal_column(column).label(to_sql_name(name))


def maybequote(elt, quote_override=None):
    "Quote for passing to SQL if necessary, based upon the python type"
    def quote_string(string):
        return "'{}'".format(string)

    if quote_override is None:
        if isinstance(elt, Number):
            return elt
        else:
            return quote_string(elt)
    elif quote_override:
        return quote_string(elt)
    else:
        return elt


class Compare(Aggregate):
    """
    A simple shorthand to automatically create many comparisons against one column
    """
    def __init__(self, col, op, choices, function,
                 order=None, include_null=False, maxlen=None, op_in_name=True,
                 quote_choices=None):
        """
        Args:
            col: the column name (or equivalent SQL expression)
            op: the SQL operation (e.g., '=' or '~' or 'LIKE')
            choices: A list or dictionary of values. When a dictionary is
                passed, the keys are a short name for the value.
            function: (from Aggregate)
            order: (from Aggregate)
            include_null: Add an extra `{col} is NULL` if True (default False).
                 May also be non-boolean, in which case its truthiness determines
                 the behavior and the value is used as the value short name.
            maxlen: The maximum length of aggregate quantity names, if specified.
                Names longer than this will be truncated.
            op_in_name: Include the operator in aggregate names (default False)
            quote_choices: Override smart quoting if present (default None)

        A simple helper method to easily create many comparison columns from
        one source column by comparing it against many values. It effectively
        creates many quantities of the form "({col} {op} {elt})::INT" for elt
        in choices. It automatically quotes strings appropriately and leaves
        numbers unquoted. The type of the comparison is converted to an
        integer so it can easily be used with 'sum' (for total count) and
        'avg' (for relative fraction) aggregate functions.

        By default, the aggregates are named "{col}_{op}_{elt}", but the
        operator may be ommitted if `op_in_name=False`. This name can become
        long and exceed the maximum column name length. If ``maxlen`` is
        specified then any aggregate name longer than ``maxlen`` gets
        truncated with a number appended to ensure that they remain unique and
        identifiable (but note that sequntial ordering is not preserved).
        """
        if type(choices) is not dict:
            choices = {k: k for k in choices}
        opname = '_{}_'.format(op) if op_in_name else '_'
        d = {'{}{}{}'.format(col, opname, nickname):
             "({} {} {})::INT".format(col, op, maybequote(choice, quote_choices))
             for nickname, choice in choices.items()}
        if include_null is True:
            include_null = '_NULL'
        if include_null:
            d['{}_{}'.format(col, include_null)] = '({} is NULL)::INT'.format(col)
        if maxlen is not None and any(len(k) > maxlen for k in d.keys()):
            for i, k in enumerate(list(d.keys())):
                d['%s_%02d' % (k[:maxlen-3], i)] = d.pop(k)

        Aggregate.__init__(self, d, function, order)


class Categorical(Compare):
    """
    A simple shorthand to automatically create many equality comparisons against one column
    """
    def __init__(self, col, choices, function, order=None, op_in_name=False, **kwargs):
        """
        Create a Compare object with an equality operator, ommitting the `=`
        from the generated aggregation names. See Compare for more details.

        As a special extension, Compare's 'include_null' keyword option may be
        enabled by including the value `None` in the choices list. Multiple
        None values are ignored.
        """
        if None in choices:
            kwargs['include_null'] = True
            choices.remove(None)
        elif type(choices) is dict and None in choices.values():
            ks = [k for k, v in choices.items() if v is None]
            for k in ks:
                choices.pop(k)
                kwargs['include_null'] = str(k)
        Compare.__init__(self, col, '=', choices, function, order, op_in_name=op_in_name, **kwargs)


class Aggregation(object):
    def __init__(self, aggregates, groups, from_obj, prefix=None, suffix=None, schema=None):
        """
        Args:
            aggregates: collection of Aggregate objects.
            from_obj: defines the from clause, e.g. the name of the table. can use
            groups: a list of expressions to group by in the aggregation or a dictionary
                pairs group: expr pairs where group is the alias (used in column names)
            prefix: prefix for aggregation tables and column names, defaults to from_obj
            suffix: suffix for aggregation table, defaults to "aggregation"
            schema: schema for aggregation tables

        The from_obj and group expressions are passed directly to the
            SQLAlchemy Select object so could be anything supported there.
            For details see:
            http://docs.sqlalchemy.org/en/latest/core/selectable.html

        Aggregates will have {collate_date} in their quantities substituted with the date
        of aggregation.
        """
        self.aggregates = aggregates
        self.from_obj = make_sql_clause(from_obj, ex.text)
        self.groups = groups if isinstance(groups, dict) else {str(g): g for g in groups}
        self.prefix = prefix if prefix else str(from_obj)
        self.suffix = suffix if suffix else "aggregation"
        self.schema = schema

    def _get_aggregates_sql(self, group):
        """
        Helper for getting aggregates sql
        Args:
            group: group clause, for naming columns
        Returns: collection of aggregate column SQL strings
        """
        prefix = "{prefix}_{group}_".format(
            prefix=self.prefix, group=group)

        return chain(*[a.get_columns(prefix=prefix)
                       for a in self.aggregates])

    def get_selects(self):
        """
        Constructs select queries for this aggregation

        Returns: a dictionary of group : queries pairs where
            group are the same keys as groups
            queries is a list of Select queries, one for each date in dates
        """
        queries = {}

        for group, groupby in self.groups.items():
            columns = [groupby]
            columns += self._get_aggregates_sql(group)

            gb_clause = make_sql_clause(groupby, ex.literal_column)
            query = ex.select(columns=columns, from_obj=self.from_obj) \
                .group_by(gb_clause)

            queries[group] = [query]

        return queries

    def get_table_name(self, group=None):
        """
        Returns name for table for the given group
        """
        if group is None:
            name = '"%s_%s"' % (self.prefix, self.suffix)
        else:
            name = '"%s"' % to_sql_name("%s_%s" % (self.prefix, group))
        schema = '"%s".' % self.schema if self.schema else ''
        return "%s%s" % (schema, name)

    def get_creates(self):
        """
        Construct create queries for this aggregation
        Args:
            selects: the dictionary of select queries to use
                if None, use self.get_selects()
                this allows you to customize select queries before creation

        Returns:
            a dictionary of group : create pairs where
                group are the same keys as groups
                create is a CreateTableAs object
        """
        return {group: CreateTableAs(self.get_table_name(group),
                                     next(iter(sels)).limit(0))
                for group, sels in self.get_selects().items()}

    def get_inserts(self):
        """
        Construct insert queries from this aggregation
        Args:
            selects: the dictionary of select queries to use
                if None, use self.get_selects()
                this allows you to customize select queries before creation

        Returns:
            a dictionary of group : inserts pairs where
                group are the same keys as groups
                inserts is a list of InsertFromSelect objects
        """
        return {group: [InsertFromSelect(self.get_table_name(group), sel) for sel in sels]
                for group, sels in self.get_selects().items()}

    def get_drops(self):
        """
        Generate drop queries for this aggregation

        Returns: a dictionary of group : drop pairs where
            group are the same keys as groups
            drop is a raw drop table query for the corresponding table
        """
        return {group: "DROP TABLE IF EXISTS %s;" % self.get_table_name(group)
                for group in self.groups}

    def get_indexes(self):
        """
        Generate create index queries for this aggregation

        Returns: a dictionary of group : index pairs where
            group are the same keys as groups
            index is a raw create index query for the corresponding table
        """
        return {group: "CREATE INDEX ON %s (%s);" %
                       (self.get_table_name(group), groupby)
                for group, groupby in self.groups.items()}

    def get_join_table(self):
        """
        Generate a query for a join table
        """
        return ex.Select(columns=self.groups.values(), from_obj=self.from_obj) \
            .group_by(*self.groups.values())

    def get_create(self, join_table=None):
        """
        Generate a single aggregation table creation query by joining
            together the results of get_creates()
        Returns: a CREATE TABLE AS query
        """
        if not join_table:
            join_table = '(%s) t1' % self.get_join_table()

        query = "SELECT * FROM %s\n" % join_table
        for group, groupby in self.groups.items():
            query += "LEFT JOIN %s USING (%s)" % (
                self.get_table_name(group), groupby)

        return "CREATE TABLE %s AS (%s);" % (self.get_table_name(), query)

    def get_drop(self):
        """
        Generate a drop table statement for the aggregation table
        Returns: string sql query
        """
        return "DROP TABLE IF EXISTS %s" % self.get_table_name()

    def get_create_schema(self):
        """
        Generate a create schema statement
        """
        if self.schema is not None:
            return "CREATE SCHEMA IF NOT EXISTS %s" % self.schema

    def execute(self, conn, join_table=None):
        """
        Execute all SQL statements to create final aggregation table.
        Args:
            conn: the SQLAlchemy connection on which to execute
        """
        self.validate(conn)
        create_schema = self.get_create_schema()
        creates = self.get_creates()
        drops = self.get_drops()
        indexes = self.get_indexes()
        inserts = self.get_inserts()
        drop = self.get_drop()
        create = self.get_create(join_table=join_table)

        trans = conn.begin()
        if create_schema is not None:
            conn.execute(create_schema)

        for group in self.groups:
            conn.execute(drops[group])
            conn.execute(creates[group])
            for insert in inserts[group]:
                conn.execute(insert)
            conn.execute(indexes[group])

        conn.execute(drop)
        conn.execute(create)
        trans.commit()

    def execute_par(self, conn_func, n_jobs=14):
        """
        Execute all SQL statements to create final aggregation table.
        Args:
            conn_func:  a function that returns ae SQLAlchemy engine
        """

        engine = conn_func()

        creates = self.get_creates()
        drops = self.get_drops()
        indexes = self.get_indexes()
        inserts = self.get_inserts()

        if self.schema is not None:
            # transaction
            with engine.begin() as conn:
                conn.execute(self.get_create_schema())

        for group in self.groups:
            # transaction
            with engine.begin() as conn:
                conn.execute(drops[group])
                conn.execute(creates[group])

            insert_list = [insert for insert in inserts[group]]

            out = Parallel(n_jobs=n_jobs, verbose=51)(delayed(execute_insert)(conn_func, insert)
                                                      for insert in insert_list)
            # transaction
            with engine.begin() as conn:
                conn.execute(indexes[group])

        # transaction
        with engine.begin() as conn:
            conn.execute(self.get_drop())
            conn.execute(self.get_create())

        engine.dispose()

    def validate(self, conn):
        """
        Validate the Aggregation to ensure that it will perform as expected.
        This is done against an active SQL connection in order to enable
        validation of the SQL itself.
        """
        pass


