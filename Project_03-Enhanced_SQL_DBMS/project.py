"""
Name: Ben Traines
Time To Completion: 3 hours
Comments: This program acts as if it is sqlite3, without actually
implemnting any sqlit3.

Sources:
"""
import string
from operator import itemgetter

_ALL_DATABASES = {}

class Database:
    """
    Database is a class that's created given the database
    name and contains all tables for the database.
    """
    def __init__(self, db_name):
        self._db_name = db_name
        self._tables = {}

    def get_name(self):
        return self._db_name

    def get_table(self, table_name):
        return self._tables[table_name]

    def add_table(self, name, table):
        self._tables[name] = table

class Table:
    """
    Table creates a table given the name of the table.
    The class also contains a list of dicts representing a column,
    with the column name as the key.
    """
    def __init__(self, table_name):
        self._table_name = table_name
        self._table = {}
        self._column_names = []

    def get_name(self):
        return self._table_name

    def get_table(self):
        return self._table

    def get_column(self, column_name):
        if ('.' in column_name) and (column_name not in self._column_names):
            table = column_name[:column_name.index('.')]
            return self._table[column_name[column_name.index('.')+1:]]

        return self._table[column_name]

    def get_column_names(self):
        return self._column_names

    def delete(self, where):
        print("Where DELETE: ", where)
        column_names = self.get_column_names()
        for name in column_names:
            column = self.get_column(name)
            for i in range(len(where)):
                print("Col ", column._column, " I: ", where[i])
                del column._column[where[i] - i]
        return []

    def add_column(self, name, column):
        self._column_names.append(name)
        self._table[name] = column


    def find_indexs(self, order_by, selected):

        col_names = self.get_column_names()

        print("ORDER ", order_by)
        print("selected: ", selected)
        print("col_names ", col_names)

        order_indexs = []
        for column_name in order_by:
            if (type(selected) is list) and (len(selected) > 1):
                if column_name not in selected:
                    if '.' in column_name:
                        column_name = column_name[column_name.index('.')+1:]
                    else:
                        column_name = self.get_name() + "." + column_name
                order_indexs.append(selected.index(column_name))
            else:
                if column_name not in col_names:
                    if '.' in column_name:
                        column_name = column_name[column_name.index('.')+1:]
                    else:
                        column_name = self.get_name() + "." + column_name
                order_indexs.append(col_names.index(column_name))


            print("column Name: ", column_name)
            print("selected: ", selected)

        return order_indexs

class Column:
    """
    Column is a class represinting a column in a table.
    The class contains the name of the column, type, and values
    """
    def __init__(self, column_name, column_type):
        self._column_name = column_name
        self._column_type = column_type
        self._column = []

    def get_name(self):
        return self._column_name

    def get_type(self):
        return self._column_type

    def get_values(self):
        return self._column

    def get_values_at(self, index_list):
        rtrn = []
        for index in index_list:
            rtrn.append(self._column[index])
        return rtrn

    def add_value(self, value):
        if (type(value) is not str) and (value is not None):
            for v in value:
                self._column.append(validate(v, self.get_type()))
        else:
            self._column.append(validate(value, self.get_type()))

class Tuplify:
    def __init__(self, selected, order_by, select_from, column_names):
        self._selected_values = self.tuplify(selected)
        self._select_from = select_from
        if type(select_from) is list():
            self._select_from = self.remove_star(select_from, column_names)
        self._order_by = order_by

    def get_selected_values(self):
        return self._selected_values

    def get_order_by(self):
        return self._order_by

    def get_select_from(self):
        return self._select_from


    def tuplify(self, values):
        rtrn = []
        for i in range(len(values[0])):
            rtrn.append(tuple(item[i] for item in values))
        return rtrn

    def remove_star(self, select_from, column_names):
        if '*' in select_from:
            for i in range(len(select_from)):
                if select_from[i] == '*':
                    select_from[i] = column_names
        print("\n\nRemove Star: ", select_from)
        return select_from

    def order_by(self, og, table):
        print("\n<-----Order By----->")
        print("Selected Values:\n\t", self.get_selected_values())
        print("Order By: ", self.get_order_by())
        print("Select From: ", self.get_select_from())
        print("All Column Names: ", table.get_column_names())
        print("OG: ", og)
        print("Table: ", table, "\n")

        select_from = self.get_select_from()
        all_column_names = table.get_column_names()
        if ('*' in select_from) and (len(select_from) > 1) and (type(select_from) is list):
            for i in range(len(select_from)):
                if select_from[i] == '*':
                    select_from.remove('*')
                    for name in all_column_names:
                        select_from.insert(i, name)




        order = self.get_order_by()
        if not self._select_from:
            self._select_from = table.get_column_names()

        order_indexs = table.find_indexs(order, self._select_from)
        print("order--> ", order_indexs)

        og.reverse()
        if order_indexs == og:
            index = len(og) - 1
            self._selected_values.sort(key=itemgetter(index))
            return self.get_selected_values()

        elif len(order_indexs) == 2:
            self._selected_values.sort(key=itemgetter(order_indexs[0], order_indexs[1]))
            return self.get_selected_values()

        elif len(order_indexs) == 1:
            self._selected_values.sort(key=itemgetter(order_indexs[0]))
            return self.get_selected_values()

        elif len(order_indexs) == 3:
            self._selected_values.sort(key=itemgetter(order_indexs[0], order_indexs[1], order_indexs[2]))
            return self.get_selected_values()



        return self.get_selected_values()





class Tokenize:

    def __init__(self, statement):
        self._values = []
        self._order_by = []
        self._tokens = []
        self._columns = []
        self._where = []
        self._keyword = ""
        self._table_name = ""
        self.tokenize(statement)

    def get_where(self):
        return self._where

    def get_columns(self):
        return self._columns

    def get_keyword(self):
        return self._keyword

    def get_tokens(self):
        return self._tokens

    def get_table_name(self):
        return self._table_name

    def get_values(self):
        return self._values

    def get_order_by(self):
        return self._order_by

    def tokenize(self, query):
        while query:
            old_qry = query

            if query[0] in string.whitespace:
                query = self.remove_leading_ws(query)
                continue

            if (query[0] in (string.ascii_letters + "_")) or (query[0] in "."):
                query = self.remove_word(query)
                continue

            if query[0] in "{},;()*=>!":
                self._tokens.append(query[0])
                query = query[1:]
                continue

            if query[0] in "'":
                query = self.remove_text(query)
                continue

            if query[0] in string.digits:
                query = self.remove_number(query)
                continue

        return self.assign()

    def assign(self):
        self.remove_comma()
        self._keyword = self._tokens[0]
        self._table_name = self._tokens[2]

        if self._keyword == "CREATE":
            self._values = self._tokens[4:-2]

        elif self._keyword == "INSERT":
            val_index = self._tokens.index("VALUES")
            index = self._tokens[val_index:].index(')') + 1 + val_index
            self._columns = self._tokens[4:self._tokens.index("VALUES")-1]

            if self._tokens[index] != '(':
                self._values = self._tokens[self._tokens.index("VALUES")+2:-2]

            else:
                self._values = self._tokens[val_index+2:-1]
                self.validate_values()
        elif self._keyword == "SELECT":
            if "WHERE" in self._tokens:
                self._where = self._tokens[self._tokens.index("WHERE")+1:self._tokens.index("ORDER")]

            self._table_name = self._tokens[self._tokens.index("FROM")+1]
            self._values = self._tokens[1:self._tokens.index("FROM")]
            self._order_by = self._tokens[self._tokens.index("BY")+1:-1]

            for i in range(len(self._values)):
                if self._values[i][-1] is '.':
                    if self._values[i+1] is '*':
                        self._values = self._values[i] + self._values[i+1]
        elif self._keyword == "DELETE":
            self._where = self._tokens[self._tokens.index("WHERE")+1:-1]
        print("\n<-----ASSIGN----->")
        print("keyword: ", self._keyword)
        print("table name: ", self._table_name)
        print("Columns: ", self._columns)
        print("Values: ", self._values)
        print("WHERE: ", self._where)
        print("Order BY: ", self._order_by, '\n')


    def validate_values(self):
        values = self._values
        rtrn = []
        while ')' in values:
            index = values.index(')')
            rtrn.append(list(values[:index]))
            values = values[index+2:]
        self._values = rtrn

    def in_tokens(self, string):
        if string in self._tokens:
            return True
        return False

    def remove_leading_ws(self, query):
        whitespace = self.collect_characters(query, string.whitespace)
        return query[len(whitespace):]

    def collect_characters(self, query, allowed_chars):
        letters = []
        for letter in query:
            if letter not in allowed_chars:
                break
            letters.append(letter)
        return "".join(letters)

    def remove_word(self, query):
        word = self.collect_characters(query, string.ascii_letters + "_" + string.digits)
        if query[len(word)] == '.':
            word += self.collect_characters(query[len(word):], "._" + string.ascii_letters + string.digits)
        if word == "NULL":
            self._tokens.append(None)
        else:
            self._tokens.append(word)
        return query[len(word):]

    def remove_text(self, query):
        query = query[1:]
        end_quote_index = query.find("'")
        text = ""
        ##check if double single quote
        while query[end_quote_index+1] == "'":
            text += query[:end_quote_index+1]
            query = query[end_quote_index+2:]

            end_quote_index = query.find("'")
            text += query[:end_quote_index]
            query = query[end_quote_index :]
            if query[:2] != "''":
                self._tokens.append(text)
                return query[1:]
            end_quote_index = query.find("'")
        else:
            text = query[:end_quote_index]
            query = query[end_quote_index + 1:]
            self._tokens.append(text)

            return query

    def remove_integer(self, query):
        int_str = self.collect_characters(query, string.digits)
        self._tokens.append(int_str)
        return query[len(int_str):]

    def remove_number(self, query):
        query = self.remove_integer(query)
        if query[0] == ".":
            whole_str = self._tokens.pop()
            query = self.remove_integer(query[1:])
            frac_str = self._tokens.pop()
            float_str = whole_str + "." + frac_str
            self._tokens.append(str(float_str))
        else:
            int_str = self._tokens.pop()
            self._tokens.append(str(int_str))
        return query

    def remove_comma(self):
        while ',' in self._tokens:
            self._tokens.remove(',')
        return self._tokens

class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self._database = Database(filename)

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        tokens = Tokenize(statement)
        keyword = tokens.get_keyword()

        if keyword == "CREATE":
            return self.create(tokens)

        elif keyword == "INSERT":
            return self.insert(self._database.get_table(tokens.get_table_name()), tokens)

        elif keyword == "SELECT":
            return self.select(tokens)

        elif keyword == "UPDATE":
            return self.update(tokens)

        elif keyword == "DELETE":
            return self.delete(tokens)

    def delete(self, tokens):
        where = tokens.get_where()
        table = self._database.get_table(tokens.get_table_name())

        print("WHERE ", where)
        print("TABLE ", table)
        where = self.validate_where(where, table)
        print("WHERE ", where)
        return table.delete(where)


    def create(self, tokens):
        """
        Takes a list of tokens and creates a table
        """

        table = Table(tokens.get_table_name())

        for value in tokens.get_values():
            if value.isupper():
                new_column = Column(column_name, value)
                table.add_column(new_column.get_name(), new_column)
            else:
                column_name = value

        self._database.add_table(table.get_name(), table)
        return list()

    def insert(self, table, tokens):

        if (len(table.get_table()) == len(tokens.get_columns())) or (len(tokens.get_columns()) == 0):
            column_names = table.get_column_names()

            if type(tokens.get_values()[0]) is list:
                rtrn = []
                for i in range(len(tokens.get_values()[0])):
                    group = tokens.get_values()
                    rtrn.append(tuple(item[i] for item in tokens.get_values()))
                tokens._values = rtrn

            for i in range(len(table.get_table())):
                if tokens.get_columns():
                    column_names = tokens.get_columns()
                table.get_column(column_names[i]).add_value(tokens.get_values()[i])
        else:
            missing_name = list(set(table.get_column_names()) - set(tokens.get_columns()))
            missing = table.get_column_names().index(missing_name[0])
            column_names = []

            if type(tokens.get_values()[0]) is list:
                for val_tup in tokens.get_values():
                    val_tup.insert(missing, None)

                rtrn = []
                for i in range(len(tokens.get_values()[0])):
                    group = tokens.get_values()
                    rtrn.append(tuple(item[i] for item in tokens.get_values()))
                tokens._values = rtrn

                tokens.get_columns().insert(missing, missing_name[0])
                column_names = tokens.get_columns()
            else:
                tokens.get_values().insert(missing, None)
                tokens.get_columns().insert(missing, missing_name[0])
                column_names = tokens.get_columns()

            for i in range(len(table.get_table())):
                table.get_column(column_names[i]).add_value(tokens.get_values()[i])

        return list()

    def validate_where(self, where, table):
        where_crct = []
        column_name = where[0]
        val = where[-1]
        operation = where[1:-1]

        if len(operation) > 1:
            if operation[0].isalnum():
                operation = ' '.join(operation)
            else:
                operation = ''.join(operation)
        else:
            operation = operation[0]

        column = table.get_column(column_name).get_values()
        column_type = table.get_column(column_name).get_type()
        val = validate(val, column_type)

        for i in range(len(column)):
            if operation == ">":
                if (column[i] != None) and (column[i] > val):
                    where_crct.append(i)
            elif operation == "=":
                if column[i] == val:
                    where_crct.append(i)
            elif operation == "!=":
                if (column[i] != val) and (column[i] is not None):
                    where_crct.append(i)
            elif operation == "IS NOT":
                if (val is None) and (column[i] is not None):
                    where_crct.append(i)
            elif operation == "IS":
                if (val is None) and (column[i] is None):
                    where_crct.append(i)

        return where_crct


    def select(self, tokens):
        select = tokens.get_values()
        order_by = tokens.get_order_by()
        where = tokens.get_where()
        table_name = tokens.get_table_name()
        table = self._database.get_table(table_name)

        print("\n<----SELECT---->")
        print("\tSelect: ", select)
        print("\tOrder By: ", order_by)
        print("\tWhere: ", where)
        print("\tTable: ", table)

        if where:
            where = self.validate_where(where, table)

        print("\nWhere Again: ", where)
        print("Order By: ", order_by, "\n")

        rtrn = []
        i = 0
        if type(select) is list:
            for column_name in select:
                print("column_name: ", column_name)
                if column_name == '*':
                    select = table.get_column_names()
                    for column_name in select:
                        rtrn.append(self.break_off_table(column_name, table, where))

                elif column_name == (tokens.get_table_name() + '.'):
                    full_name = column_name + select[i+1]
                    rtrn.append(self.break_off_table(full_name, table, where))

                else:
                    rtrn.append(self.break_off_table(column_name, table, where))
                i += 1
            print("Tuplify(", rtrn, "", order_by, " ", tokens.get_values(), ")")
            rtrn = Tuplify(rtrn, order_by, tokens.get_values(), table.get_column_names())
            return rtrn.order_by(table.find_indexs(select, select), table)
        else:
            select = table.get_column_names()
            for column_name in select:
                rtrn.append(self.break_off_table(column_name, table, where))

            print("\nRtrn: ", rtrn)
            print("oder by: ", order_by)
            print("other-> ", tokens.get_values())
            rtrn = Tuplify(rtrn, order_by, tokens.get_values(), table.get_column_names())

            return rtrn.order_by(table.find_indexs(select, select), table)




    def break_off_table(self, name, table, where):
        if '.' in name:
            table = self._database.get_table(name[:name.index('.')])
            name = name[name.index('.')+1:]

        column = []
        if name == '*':

            for column_name in table.get_column_names():
                if where:
                    column.append(table.get_column(column_name).get_values_at(where))
                else:
                    column.append(table.get_column(column_name).get_values())
            return column

        if where:
            return table.get_column(name).get_values_at(where)

        return table.get_column(name).get_values()

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass

def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)

def validate(value, column_type):
    if value == None:
        return None
    elif column_type == "INTEGER":
        return int(value)
    elif column_type == "TEXT":
        return str(value)
    elif column_type == "REAL":
        if value == None:
            return None
        else:
            return float(value)
