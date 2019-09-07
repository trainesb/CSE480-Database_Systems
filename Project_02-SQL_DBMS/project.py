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


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self.database = Database(filename)
        #print("Database\n\tDB Name: ", self.database.db_name, "\n\tTables: ", self.database.tables)

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        tokens = tokenize(statement)
        keyword = tokens[0]
        tbl_name = tokens[2]

        if keyword == "CREATE":
            table = Table(tbl_name)
            column_val = tokens[4:-2]
            column_name = ""
            for col in column_val:
                if col != ",":
                    if col.isupper():
                        new_col = Column(column_name, col)
                        table.table.append({new_col.column_name : new_col})
                    else:
                        column_name = col
            self.database.tables.append({table.table_name : table})
            return list()

        if keyword == "INSERT":
            values = tokens[5:-2]
            table = self.get_table(tbl_name)
            table_len = len(table)

            if table_len == 1:
                for col in table[0].values():
                    if col.column_type == "INTEGER":
                        for value in values:
                            col.column.append(tuple([int(value)]))

            else:
                val_i = tokens.index("VALUES")
                values = tokens[val_i+2:-2]
                add_val = []

                for val in values:
                    if val != ',':

                        add_val.append(val)
                i = 0
                for col in table:
                    for col_list in col.values():
                        if col_list.column_type == "INTEGER":
                            col_list.column.append(int(add_val[i]))
                        elif col_list.column_type == "TEXT":
                            col_list.column.append(str(add_val[i]))
                        elif col_list.column_type == "REAL":
                            if add_val[i] == None:
                                col_list.column.append(None)
                            else:
                                col_list.column.append(float(add_val[i]))
                        i = i + 1
            return list()


        if keyword == "SELECT":
            frm = tokens.index("FROM")
            ordr = tokens.index("ORDER")
            select_columns = tokens[1:frm]
            select_table = tokens[frm+1:ordr]
            select_ordr_by = tokens[ordr+2:-1]
            flip = False

            rtrn = []
            col_ord = []
            for tbl in select_table:
                table = self.get_table(tbl)
                for column in table:
                    for col_name, col in column.items():
                        col_ord.append(col_name)
                        if select_columns[0] == "*":
                            rtrn.append(col.column)
                        else:
                            for name in select_columns:
                                if name == col_name:
                                    rtrn.append(col.column)
                if len(rtrn) == 1:
                    return rtrn[0]
                else:
                    new_rtrn = []
                    for col in rtrn:
                        for i in range(len(col)):

                            if len(new_rtrn) < len(col):
                                new_rtrn.append([col[i]])
                            else:
                                new_rtrn[i].append(col[i])

                    rtrn = []
                    for i in range(len(new_rtrn)):
                        rtrn.append(tuple(new_rtrn[i]))
                    new_rtrn = []
                    if (col_ord[0] != select_columns[0]) and (select_columns[0] != "*"):
                        for tup in rtrn:
                            new_rtrn.append(tup[::-1])
                            flip = True
                        rtrn = new_rtrn

                    col_order = []
                    for order in select_ordr_by:
                        if order != ",":
                            col_order.append(col_ord.index(order))
                    col_order = col_order[::-1]
                    for order in col_order:
                        if flip:
                            order = 0
                        rtrn = sorted(rtrn, key=itemgetter(order))
                    return rtrn


    def get_table(self, table_name):
        print("data: ", self.database.tables)
        for tbl in self.database.tables:
            print("TBL: ", tbl)
            for key, val in tbl.items():
                if key == table_name:
                    return val.table


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


class Database:
    """
    Database is a class that's created given the database
    name and contains all tables for the database.
    """
    def __init__(self, db_name):
        self.db_name = db_name
        self.tables = []

class Table:
    """
    Table creates a table given the name of the table.
    The class also contains a list of dicts representing a column,
    with the column name as the key.
    """
    def __init__(self, table_name):
        self.table_name = table_name
        self.table = []

class Column:
    """
    Column is a class represinting a column in a table.
    The class contains the name of the column, type, and values
    """
    def __init__(self, column_name, column_type):
        self.column_name = column_name
        self.column_type = column_type
        self.column = []

def collect_characters(query, allowed_chars):
    letters = []
    for letter in query:
        if letter not in allowed_chars:
            break
        letters.append(letter)
    return "".join(letters)

def remove_leading_ws(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]

def remove_word(query, tokens):
    word = collect_characters(query, string.ascii_letters+"_"+string.digits)

    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]

def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query

def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]

def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(str(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(str(int_str))
    return query

def tokenize(query):
    tokens = []
    while query:
        old_qry = query

        if query[0] in string.whitespace:
            query = remove_leading_ws(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "{},;()*":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

    return tokens
