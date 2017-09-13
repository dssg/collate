# -*- coding: utf-8 -*-
from itertools import chain
import sqlalchemy.sql.expression as ex

from .sql import make_sql_clause
from .collate import Aggregation


class SpacetimeAggregation(Aggregation):
    def __init__(self, aggregates, groups, intervals, from_obj, dates, 
                 state_table, state_group=None,
                 prefix=None, suffix=None, schema=None, date_column=None,
                 output_date_column=None, input_min_date=None):
        """
        Args:
            intervals: the intervals to aggregate over. either a list of
                datetime intervals, e.g. ["1 month", "1 year"], or
                a dictionary of group : intervals pairs where
                group is a group in groups and intervals is a collection
                of datetime intervals, e.g. {"address_id": ["1 month", "1 year]}
            dates: list of PostgreSQL date strings,
                e.g. ["2012-01-01", "2013-01-01"]
            state_table: schema.table to query for valid state_group/date combinations
            state_group: the group level found in the state table (e.g., "entity_id")
            date_column: name of date column in from_obj, defaults to "date"
            output_date_column: name of date column in aggregated output, defaults to "date"
            input_min_date: minimum date for which rows shall be included, defaults
                to no absolute time restrictions on the minimum date of included rows

        For all other arguments see collate.Aggregation
        """
        Aggregation.__init__(self,
                             aggregates=aggregates,
                             from_obj=from_obj,
                             groups=groups,
                             prefix=prefix,
                             suffix=suffix,
                             schema=schema)

        if isinstance(intervals, dict):
            self.intervals = intervals
        else:
            self.intervals = {g: intervals for g in self.groups}
        self.dates = dates
        self.state_table = state_table
        self.state_group = state_group if state_group else "entity_id"
        self.date_column = date_column if date_column else "date"
        self.output_date_column = output_date_column if output_date_column else "date"
        self.input_min_date = input_min_date

    def _get_aggregates_sql(self, interval, date, group):
        """
        Helper for getting aggregates sql
        Args:
            interval: SQL time interval string, or "all"
            date: SQL date string
            group: group clause, for naming columns
        Returns: collection of aggregate column SQL strings
        """
        if interval != 'all':
            when = "{date_column} >= '{date}'::date - interval '{interval}'".format(
                    interval=interval, date=date, date_column=self.date_column)
        else:
            when = None

        prefix = "{prefix}_{group}_{interval}_".format(
                prefix=self.prefix, interval=interval,
                group=group)

        return chain(*[a.get_columns(when, prefix, format_kwargs={"collate_date": date,
                                                                  "collate_interval": interval})
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
            intervals = self.intervals[group]
            queries[group] = []
            for date in self.dates:
                columns = [groupby,
                           ex.literal_column("'%s'::date"
                                             % date).label(self.output_date_column)]
                columns += list(chain(*[self._get_aggregates_sql(
                        i, date, group) for i in intervals]))

                gb_clause = make_sql_clause(groupby, ex.literal_column)
                query = ex.select(columns=columns, from_obj=self.from_obj)\
                          .group_by(gb_clause)
                query = query.where(self.where(date, intervals))

                queries[group].append(query)

        return queries

    def get_imputation_rules(self):
        """
        Constructs a dictionary to lookup an imputation rule from an associated
        column name.

        Returns: a dictionary of column : imputation_rule pairs
        """
        imprules = {}
        for group, groupby in self.groups.items():
            for interval in self.intervals:
                prefix = "{prefix}_{group}_{interval}_".format(
                        prefix=self.prefix, interval=interval,
                        group=group)
                for a in self.aggregates:
                    imprules.update(a.column_imputation_lookup(prefix=prefix))
        return imprules

    def where(self, date, intervals):
        """
        Generates a WHERE clause
        Args:
            date: the end date
            intervals: intervals

        Returns: a clause for filtering the from_obj to be between the date and
            the greatest interval
        """
        # upper bound
        w = "{date_column} < '{date}'".format(
                            date_column=self.date_column, date=date)

        # lower bound (if possible)
        if 'all' not in intervals:
            greatest = "greatest(%s)" % str.join(
                    ",", ["interval '%s'" % i for i in intervals])
            min_date = "'{date}'::date - {greatest}".format(date=date, greatest=greatest)
            w += "AND {date_column} >= {min_date}".format(
                    date_column=self.date_column, min_date=min_date)
        if self.input_min_date is not None:
            w += "AND {date_column} >= '{bot}'::date".format(
                    date_column=self.date_column, bot=self.input_min_date)
        return ex.text(w)

    def get_indexes(self):
        """
        Generate create index queries for this aggregation

        Returns: a dictionary of group : index pairs where
            group are the same keys as groups
            index is a raw create index query for the corresponding table
        """
        return {group: "CREATE INDEX ON %s (%s, %s);" %
                (self.get_table_name(group), groupby, self.output_date_column)
                for group, groupby in self.groups.items()}

    def get_join_table(self):
        """
        Generates a join table, consisting of an entry for each combination of
        groups and dates in the from_obj
        """
        groups = list(self.groups.values())
        intervals = list(set(chain(*self.intervals.values())))

        queries = []
        for date in self.dates:
            columns = groups + [ex.literal_column("'%s'::date" % date).label(
                    self.output_date_column)]
            queries.append(ex.select(columns, from_obj=self.from_obj)
                             .where(self.where(date, intervals))
                             .group_by(*groups))

        return str.join("\nUNION ALL\n", map(str, queries))

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
            query += " LEFT JOIN %s USING (%s, %s)" % (
                    self.get_table_name(group), groupby, self.output_date_column)

        return "CREATE TABLE %s AS (%s);" % (self.get_table_name(), query)

    def validate(self, conn):
        """
        SpacetimeAggregations ensure that no intervals extend beyond the absolute
        minimum time.
        """
        if self.input_min_date is not None:
            all_intervals = set(*self.intervals.values())
            for date in self.dates:
                for interval in all_intervals:
                    if interval == "all":
                        continue
                    # This could be done more efficiently all at once, but doing
                    # it this way allows for nicer error messages.
                    r = conn.execute("select ('%s'::date - '%s'::interval) < '%s'::date" %
                                     (date, interval, self.input_min_date))
                    if r.fetchone()[0]:
                        raise ValueError(
                            "date '%s' - '%s' is before input_min_date ('%s')" %
                            (date, interval, self.input_min_date))

    def find_nulls(self):
        """
        Generate query to count number of nulls in each column in the aggregation table
        
        Returns: a SQL SELECT statement
        """
        query_template = """
            SELECT {cols} 
            FROM {state_tbl} t1 
            LEFT JOIN {aggs_tbl} t2 USING({group}, {date_col})
            """
        cols_sql = ',\n'.join([
            "SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}".format(col=column)
            for column in self.get_imputation_rules().keys()
            ])

        return query_template.format(
                cols=cols_sql, state_tbl=self.state_table, aggs_tbl=self.get_table_name(),
                group=self.state_group, date_col=self.output_date_column
            )

    def get_impute_create(self, impute_cols, nonimpute_cols):
        """
        Generates the CREATE TABLE query for the aggregation table with imputation.

        Args:
            impute_cols: a list of column names with null values
            nonimpute_cols: a list of column names without null values

        Returns: a CREATE TABLE AS query
        """
        imprules = self.get_imputation_rules()

        # key columns and date column
        query = "SELECT %s, %s" % (', '.join(self.groups.values()), self.output_date_column)

        # just pass through columns that don't require imputation (no nulls found)
        for col in nonimpute_cols:
            query += "\n,%s" % col

        # for columns that do require imputation, include SQL to do the imputation work
        # and a flag for whether the value was imputed
        for col in impute_cols:
            query += "\n,%s" % self._impute_sql(col, imprules[col])
            if imprules[col]['coltype'] not in ['categorical', 'array_categorical']:
                # Add an imputation flag for non-categorical columns (this is handeled
                # for categorical columns with a separate NULL category)
                query += "\n,CASE WHEN %s IS NULL THEN 1 ELSE 0 END AS %s_imp" % (col, col)

        # imputation starts from the state table and left joins into the aggregation table
        query += "\nFROM %s t1" % self.state_table
        query += "\nLEFT JOIN %s t2 USING(%s, %s)" % (
            self.get_table_name()
            self.state_group, 
            self.output_date_column
            )

        return "CREATE TABLE %s AS (%s)" % (self.get_table_name(imputed=True), query)

    def _impute_sql(self, column, impute_rule):
        """
        Generate a SQL snippet for coalescing an imputed value to fill in missing values.
        Currently available imputation types include:
            mean: mean-value imputation (within-date)
            constant: constant-value imputation (value must be specified)
            null_category: flag nulls for categorical variables
            error: raise an exception if null values are encountered

        Args:
            column: column name for imputation
            impute_rule: dict with keys: type, coltype, value (optional)

        Returns: a COALESCE statement
        """
        sql = "COALESCE({col}, {{imp}}) AS {col}".format(col=column)
        catcol = impute_rule['coltype'] in ['categorical', 'array_categorical']

        # mean imputation for non-categorical columns
        # note that we'll fall back to 0 if the column is entirely NULL for a given
        # date (hence the mean is NULL), rather than passing NULLs through
        if impute_rule['type'] == 'mean' and not catcol:
            return sql.format(
                imp="AVG(%s) OVER (PARTITION BY %s), 0" % (column, self.output_date_column)
            )

        # mean imputation for categorical columns:
        # flag the NULL category column with a 1 and other columns with the mean
        # note that we'll fall back to 0 if the column is entirely NULL for a given
        # date (hence the mean is NULL), rather than passing NULLs through
        elif impute_rule['type'] == 'mean' and catcol:
            if '_NULL' in column:
                return sql.format(imp=1)
            else:
                return sql.format(
                    imp="AVG(%s) OVER (PARTITION BY %s), 0" % (column, self.output_date_column)
                )

        # constant value imputation for non-categorical columns
        elif impute_rule['type'] == 'constant' and not catcol:
            return sql.format(
                imp=impute_rule['value']
            )

        # constant value imputation for categorical columns:
        # fill the appropriate category and null columns with 1, others with 0
        elif impute_rule['type'] == 'constant' and catcol:
            return sql.format(
                imp = 1 if impute_rule['value'] in column or '_NULL' in column else 0
            )

        # provide a convenience rule type to do zero-filling
        # (but for categoricals, still fill the NULL column with a 1)
        elif impute_rule['type'] == 'zero':
            return sql.format(
                imp = 1 if catcol and '_NULL' in column else 0
            )

        # just rely on the null category for a categorical column
        elif impute_rule['type'] == 'null_category' and catcol:
            return sql.format(
                imp = 1 if '_NULL' in column else 0
            )

        # can specify an "error" imputation type that will simply raise an exception
        # if any null values have been found in the column
        elif impute_rule['type'] == 'error':
            raise ValueError('NULL values found in column %s' % column)

        # a valid imputation type is required for every column, so error out if we don't
        # have one
        else:
            raise ValueError('Invalid imputation type %s for column %s' % (impute_rule['type'], column))
