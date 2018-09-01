import copy
import sqlite3
import os
import os.path



class Database(object):

    def __init__(self):
        self.__database_path = None
        self.__db = None
        self.__opened = False
        return

    def open(self, database_path=None, override=False):
        assert isinstance(database_path, str)
        try:
            if override and os.path.isfile(database_path):
                os.remove(database_path)

            self.__db = sqlite3.connect(database_path, check_same_thread=False)
            self.__opened = True
            self.__database_path = ''

            print('Database opened: ' + database_path)
            return True
        except Exception as e:
            print('Database Error during opening: ', e)
        return False

    def is_opened(self):
        if self.__db is not None:
            assert isinstance(self.__db, sqlite3.Connection)
        return self.__opened

    def get_database_path(self):
        return self.__database_path

    def connection(self):
        if self.__db is not None:
            assert isinstance(self.__db, sqlite3.Connection)
            return self.__db
        return None

    def create_tables(self, tables):
        assert isinstance(self.__db, sqlite3.Connection)
        try:
            for table in tables:
                sql_create = getattr(table(), "_SQL_CREATE")
                # print 'creating table: ', sql_create
                c = self.__db.cursor()
                c.execute(sql_create)
                setattr(table, '_db', self.__db)
            return True
        except Exception as e:
            print('Table ', table.__name__, ' Error: ', e)
        return False

    def cursor(self):
        return self.__db.cursor()


class EdgeModelException(Exception):

    def __init__(self, message, idx=None):
        self.message = message
        self.idx = idx




class FieldType(object):
    INTEGER = 1
    TEXT = 2
    REAL = 4
    FOREIGN = 8
    DATETIME = 16
    TIME = 32


class PT(object):  # property index
    FieldName = 0
    Description = 1
    NotNull = 2
    Size = 3
    Precision = 4
    DefaultValue = 5
    PrimaryKey = 6
    Unique = 7
    AutoIncrement = 8
    ForeignModel = 9
    ForeignTable = 10
    ForeignKey = 11
    Properties = 'Properties'
    Fields = 0
    Keys = 1


class PrimaryKey(object):
    pass


class Unique(object):
    pass


class NotNull(object):
    pass


class AutoIncrement(object):
    pass


class Field(object):

    __type = FieldType.TEXT
    _properties = None
    _object = None

    # TODO *kwargs NotNull AutoIncremnt Unique PrimaryKey EdgeModel
    def __init__(self, *vargs, **kwargs):
        self.__init_args(vargs, kwargs)
        self.__value = [self._properties[PT.DefaultValue]]

    def set_value(self, val):
        self.__value[0] = val

    def get_value(self):
        return self.__value[0]

    def get_value_as_ref(self):
        return self.__value

    def __init_args(self, vargs, kwargs):
        self._properties = dict()

        if any(v is NotNull for v in vargs) is True:
            self._properties[PT.NotNull] = True
        elif 'NotNull' in kwargs:
            self._properties[PT.NotNull] = kwargs['NotNull']
        else:
            self._properties[PT.NotNull] = False

        if any(v is PrimaryKey for v in vargs) is True:
            self._properties[PT.PrimaryKey] = True
        elif 'PrimaryKey' in kwargs:
            self._properties[PT.PrimaryKey] = kwargs['PrimaryKey']
        else:
            self._properties[PT.PrimaryKey] = False

        if any(v is Unique for v in vargs) is True:
            self._properties[PT.Unique] = True
        elif 'Unique' in kwargs:
            self._properties[PT.Unique] = kwargs['Unique']
        else:
            self._properties[PT.Unique] = False

        if any(v is AutoIncrement for v in vargs) is True:
            self._properties[PT.AutoIncrement] = True
        elif 'AutoIncrement' in kwargs:
            self._properties[PT.AutoIncrement] = kwargs['AutoIncrement']
        else:
            self._properties[PT.AutoIncrement] = False

        if 'FieldName' in kwargs:
            self._properties[PT.FieldName] = kwargs['FieldName']
        else:
            raise EdgeModelException('FieldName is not setted', 1)

        if 'Description' in kwargs:
            self._properties[PT.Description] = kwargs['Description']
        else:
            self._properties[PT.Description] = ''

        if 'Size' in kwargs:
            self._properties[PT.Size] = kwargs['Size']
        else:
            self._properties[PT.Size] = None

        if 'Precision' in kwargs:
            self._properties[PT.Precision] = kwargs['Precision']
        else:
            self._properties[PT.Precision] = None

        if 'DefaultValue' in kwargs:
            self._properties[PT.DefaultValue] = kwargs['DefaultValue']
        else:
            self._properties[PT.DefaultValue] = None

        if 'ForeignModel' in kwargs:
            self._properties[PT.ForeignModel] = kwargs['ForeignModel']

            if len(getattr(self._properties[PT.ForeignModel], '__KEY_NAMES')) == 0:
                raise EdgeModelException('Model does not has primary key', 1)

            if isinstance(self._properties[PT.ForeignModel], EdgeModel):
                raise EdgeModelException('Foreign Model is not an EdgeModel', 1)

            self._properties[PT.ForeignKey] = getattr(self._properties[PT.ForeignModel], '__KEY_NAMES')[0]
            self._properties[PT.ForeignTable] = getattr(self._properties[PT.ForeignModel], '__TABLE')
            if self._properties[PT.ForeignKey] is None or self._properties[PT.ForeignTable] is None:
                raise EdgeModelException('Data Field Error: could not be initialized, the ref class was not defined', 1)
        else:
            self._properties[PT.ForeignModel] = None

    def property(self, idx):
        return self._properties[idx]

    def fieldname(self):
        return self._properties[PT.FieldName]

    def is_foreign_key(self):
        return self._properties[PT.ForeignModel] is not None

    def get_object(self):
        return self._object


class IntegerField(Field):
    __type = FieldType.INTEGER

    def __init__(self, *vargs, **kwargs):
        Field.__init__(self, *vargs, **kwargs)

    def set_value(self, val):
        if (self._properties[PT.ForeignModel] is not None) and isinstance(val, self._properties[PT.ForeignModel]):
            keys = val.get_keys()
            if len(keys) > 0:
                # print('keys: ', keys[0].get_value())
                Field.set_value(self, keys[0].get_value())
            else:
                raise EdgeModelException('Foreign Model ' + str(val.__class__) + ' must have a primary key', 1)
            self._object = val
        else:
            assert isinstance(val, int)
            Field.set_value(self, val)


class TextField(Field):
    __type = FieldType.TEXT

    def __init__(self, *vargs, **kwargs):
        Field.__init__(self, *vargs, **kwargs)

    def set_value(self, val):
        assert isinstance(val, str)
        Field.set_value(self, val)


class RealField(Field):
    __type = FieldType.REAL

    def __init__(self, *vargs, **kwargs):
        Field.__init__(self, *vargs, **kwargs)

    def set_value(self, val):
        assert isinstance(val, float)
        Field.set_value(self, val)


# TODO CREATE A TIME
class DateTimeField(Field):
    __type = FieldType.DATETIME

    def __init__(self, *vargs, **kwargs):
        Field.__init__(self, *vargs, **kwargs)

    def set_value(self, val):
        assert isinstance(val, str)
        Field.set_value(self, val)


class TimeField(Field):
    __type = FieldType.TIME

    def __init__(self, *vargs, **kwargs):
        Field.__init__(self, *vargs, **kwargs)

    def set_value(self, val):
        # assert isinstance(val, long)
        Field.set_value(self, val)


#    TODO  Foreigner Class: People.set_poeple_type(PeopleType)


class EdgeModel(object):

    #    data = 'TODO: dict [FieldName:Valor] com referecia para o valor Field.__value  utilizado para executar sqls'
    #    STATIC_VARS = 'TODO: dict com todos os Fields do banco descritos staticamente'

    table_name = None
    _SQL_CREATE, _SQL_INSERT, _SQL_UPDATE, _SQL_DELETE, _SQL_IS_PERSISTED = None, None, None, None, None
    _SQL_SELECT_SINGLE, _SQL_SELECT, _SQL_SELECT_ROWID = None, None, None
    _db = None
    __keys = None
    __fields = None
    __all_field_names = None

    # Global vars
    _join = None
    _where = None

    def __init__(self, hash_data=None):
        self.__set_static_vars()
        self.__set_data_vars()

    '''Static -------------------------------------------------------------------------------------'''
    # fields = [getattr(self, name) for name in props if isinstance(getattr(self, name), Field)] #keys = [field for field in fields if field._properties[PT.PrimaryKey] is True]

    def __set_static_vars(self):

        if hasattr(self.__class__, '__STATIC_VARS'):
            return

        self.__define_model__()

        props = dir(self)
        fields, keys = [], []
        field_names = []
        key_names = []
        for name in props:
            f = getattr(self, name)
            if isinstance(f, Field):
                setattr(f, '_attrname', name)
                fields.append(f)
                if f.property(PT.PrimaryKey) is True:
                    keys.append(f)
                    key_names.append(f.property(PT.FieldName))
                else:
                    field_names.append(f.property(PT.FieldName))

        setattr(self.__class__, '__FIELDS', fields)
        setattr(self.__class__, '__KEYS', keys)
        setattr(self.__class__, '__TABLE', self.table_name)
        setattr(self.__class__, '__FIELD_NAMES', field_names)
        setattr(self.__class__, '__KEY_NAMES', key_names)
        self.__create_sqls()
        setattr(self.__class__, '__STATIC_VARS', True)

    def __create_sqls(self):
        self.__sql_create()
        self.__sql_delete()
        self.__sql_insert()
        self.__sql_update()
        self.__sql_is_persisted()
        self.__sql_select()
        self.__sql_select_rowid()
        self.__sql_select_single()

    def __sql_create(self):
        fields = getattr(self.__class__, '__FIELDS')

        s = 'CREATE TABLE IF NOT EXISTS ' + self.table_name + '('
        columns = []
        foreigns = []
        for o in fields:
            assert (isinstance(o, Field))

            c = o.property(PT.FieldName) + ' '
            if isinstance(o, IntegerField):
                c += 'INTEGER'
            elif isinstance(o, TextField):
                c += 'TEXT'
            elif isinstance(o, RealField):
                c += 'REAL'

            if o.property(PT.PrimaryKey) is True:
                c += ' PRIMARY KEY'
            if o.property(PT.AutoIncrement) is True:
                c += ' AUTOINCREMENT'
            if o.property(PT.NotNull) is True:
                c += ' NOT NULL'
            if o.property(PT.Unique) is True:
                c += ' UNIQUE'

            columns.append(c)

            if o.is_foreign_key():
                foreigns.append('FOREIGN KEY (' + o.property(PT.FieldName) +
                                ') REFERENCES ' + o.property(PT.ForeignTable) + '(' + o.property(PT.ForeignKey) + ')')

        s += ", ".join(c for c in columns)
        if len(foreigns) > 0:
            s += "," + ",".join(f for f in foreigns)
        s += ");"
        setattr(self.__class__, '_SQL_CREATE', s)

    def __sql_insert(self):
        fields = getattr(self, '__FIELD_NAMES') + getattr(self, '__KEY_NAMES')
        s = 'INSERT INTO ' + self.table_name + ' (' + ",".join(fields) + ') ' + 'VALUES (' + \
            ",".join(":" + p for p in fields) + ');'
        setattr(self.__class__, '_SQL_INSERT', s)

    def __sql_update(self):
        field_names = getattr(self, '__FIELD_NAMES')
        key_names = getattr(self, '__KEY_NAMES')
        s = 'UPDATE ' + self.table_name + ' SET ' + ", ".join(
            c + ' = :' + c for c in field_names) \
            + ' WHERE ' + " and ".join(c + ' = :' + c for c in key_names) + ";"
        setattr(self.__class__, '_SQL_UPDATE', s)

    def __sql_delete(self):
        key_names = getattr(self, '__KEY_NAMES')
        s = 'DELETE FROM ' + self.table_name + ' WHERE ' + " and ".join(c + ' = :' + c for c in key_names) + ";"
        setattr(self.__class__, '_SQL_DELETE', s)

    def __sql_is_persisted(self):
        key_names = getattr(self, '__KEY_NAMES')
        s = 'SELECT 1 FROM ' + self.table_name + ' WHERE ' + " and ".join(c + ' = :' + c for c in key_names) + ";"
        setattr(self.__class__, '_SQL_IS_PERSISTED', s)

    def __sql_select(self):
        fields = getattr(self, '__KEY_NAMES') + getattr(self, '__FIELD_NAMES')
        s = 'SELECT ' + ", ".join(fields) + ' FROM ' + self.table_name + ' '
        setattr(self.__class__, '_SQL_SELECT', s)

    def __sql_select_single(self):
        s = getattr(self.__class__, '_SQL_SELECT')
        keys = getattr(self, '__KEY_NAMES')
        s += ' WHERE ' + " and ".join(c + '= :' + c for c in keys)
        setattr(self.__class__, '_SQL_SELECT_SINGLE', s)

    def __sql_select_rowid(self):
        s = getattr(self, '_SQL_SELECT')
        s += ' WHERE rowid = :rowid;'
        setattr(self.__class__, '_SQL_SELECT_ROWID', s)

    def __define_model__(self):
        raise EdgeModelException('Must be defined in model class', 1)

    # Object

    def __set_data_vars(self):
        fields = getattr(self.__class__, '__FIELDS')
        self.__values = dict()
        self.__fields = []
        self.__keys = []

        for f in fields:
            # TODO TRY ONLY COPY INSTEAD DEEPCOPY
            newfield = copy.deepcopy(f)
            del newfield._attrname
            setattr(self, f._attrname, newfield)
            self.__fields.append(newfield)
            self.__values[newfield.fieldname()] = newfield.get_value_as_ref()
            if newfield.property(PT.PrimaryKey) is True:
                self.__keys.append(newfield)
        self.__all_field_names = getattr(self, '__KEY_NAMES') + getattr(self, '__FIELD_NAMES')

    def __execute_query(self, query, fields=None):
        params = dict()
        for f in fields:
            params[f.fieldname()] = f.get_value()

        if self._db is not None:
            c = self._db.cursor()
            c.execute(query, params)
            return c

        return None

    def __load_by_rowid(self, rowid):
        try:
            c = self._db.cursor()
            c.execute(self.__class__._SQL_SELECT_ROWID, {'rowid': rowid})
            row = c.fetchone()
            i = 0
            data = {}
            for v in self.__all_field_names:
                data[v] = row[i]
                i += 1
            self.load_from_array(data)
            return True
        except Exception as e:
            pass
            print(self.__class__.__name__, '__load_by_rowid -> Error -> ', e)
        return False

    def __load_by_keys(self):
        try:
            r = self.__execute_query(self.__class__._SQL_SELECT_SINGLE, self.__keys)
            self._db.commit()
            row = r.fetchone()
            if len(row):
                i = 0
                data = {}
                for v in self.__all_field_names:
                    data[v] = row[i]
                    i += 1
                self.load_from_array(data)
                return True
        except Exception as e:
            print(self.__class__.__name__, '__load_by_keys -> Error -> ', e)
        return False

    def __is_persisted(self):
        try:
            if self.__keys[0].get_value() is None:
                return False
            r = self.__execute_query(self.__class__._SQL_IS_PERSISTED, self.__keys)
            self._db.commit()
            data = r.fetchone()

            if data is not None:
                if len(data) > 0:
                    return True
        except Exception as e:
            print(self.__class__.__name__, '__is_persisted -> Error -> ', e)
        return False

    def __update(self):
        try:
            r = self.__execute_query(self.__class__._SQL_UPDATE, self.__fields)
            self._db.commit()
            if r.rowcount > 0:
                # TODO RELOAD
                return True
        except Exception as e:
            print(self.__class__.__name__, '__update -> Error -> ', e)
        return False

    def __insert(self):
        try:
            r = self.__execute_query(self.__class__._SQL_INSERT, self.__fields)
            self._db.commit()
            if r.lastrowid is not None and r.lastrowid > 0:
                return self.__load_by_rowid(r.lastrowid)
        except Exception as e:
            print(self.__class__.__name__, '__insert -> Error -> ', e)
        return False

    def __delete(self):
        try:
            r = self.__execute_query(self.__class__._SQL_DELETE, self.__keys)
            self._db.commit()
            if r.rowcount > 0:
                return True
        except Exception as e:
            print(self.__class__.__name__, '__insert -> Error -> ', e)
        return False

    def get_keys(self):
        return self.__keys

    def save(self):
        if self.__is_persisted():
            return self.__update()
        return self.__insert()

    def delete(self):
        if self.__is_persisted():
            return self.__delete()
        return False

    def load(self):
        if self.__is_persisted():
            return self.__load_by_keys()
        return False

    def load_from_array(self, data):
        for key, value in data.items():
            if key in self.__values:
                self.__values[key][0] = value

    def get_all(self):
        try:
            c = self.__execute_query(self.__class__._SQL_SELECT, self.__fields)
            if c is None:
                return None

            fetch_tuples = c.fetchall()
            ret_classes = list()

            for line in fetch_tuples:

                i = 0
                data = {}
                for v in self.__all_field_names:
                    data[v] = line[i]
                    i += 1
                self.load_from_array(data)
                new_self = copy.deepcopy(self)
                ret_classes.append(new_self)

            return ret_classes
        except Exception as e:
            print(self.__class__.__name__, '__insert -> Error -> ', e)
        return None

    def get_sql(self, sql):
        try:
            c = self.__execute_query(sql, self.__fields)
            if c is None:
                return None

            fetch_tuples = c.fetchall()
            ret_classes = list()

            for line in fetch_tuples:

                i = 0
                data = {}
                for v in self.__all_field_names:
                    data[v] = line[i]
                    i += 1
                self.load_from_array(data)
                new_self = copy.deepcopy(self)
                ret_classes.append(new_self)

            return ret_classes
        except Exception as e:
            print(self.__class__.__name__, '__insert -> Error -> ', e)
        return None

    def get_with_params(self):
        try:
            sql = self.__class__._SQL_SELECT
            if self._join is not None:
                sql += self._join
            if self._where is not None:
                sql += self._where
            c = self.__execute_query(sql, self.__fields)
            if c is not None:
                fetch_tuples = c.fetchall()
                ret_classes = list()

                for line in fetch_tuples:

                    i = 0
                    data = {}
                    for v in self.__all_field_names:
                        data[v] = line[i]
                        i += 1
                    self.load_from_array(data)
                    new_self = copy.deepcopy(self)
                    ret_classes.append(new_self)

                return ret_classes
            return None
        except Exception as e:
            print(self.__class__.__name__, '__get -> Error -> ', e)
        return None

    def get_by_id(self, id_search):
        try:
            sql = self.__class__._SQL_SELECT + " where " + self.__keys[0] + " = " + id_search
            c = self.__execute_query(sql, self.__fields)
            line = c.fetchone()

            i = 0
            data = {}
            for v in self.__all_field_names:
                data[v] = line[i]
                i += 1
            self.load_from_array(data)

            return self

        except Exception as e:
            print(self.__class__.__name__, '__insert -> Error -> ', e)
        return None

    def join(self, value):
        self._join = value
        return self

    def where(self, value):
        self._where = value
        return self
