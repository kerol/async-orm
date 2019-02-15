# coding: utf-8
import datetime

from django.db import models
from django.conf import settings


# all databases pool
dbs = {}


class Q:

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __or__(self, other):
        return [self.kwargs, other.kwargs]  # self or other


class AsyncBaseModel(models.Model):
    """ Async Base Model """
    class Meta:
        abstract = True

    def __init__(self, db_obj=False, *args, **kwargs):
        super(AsyncBaseModel, self).__init__(*args, **kwargs)
        self.db_obj = db_obj
        self.original = kwargs
        self.original['db_obj'] = db_obj

    async def save(self, echo=False, using='default', ts=None, *args, **kwargs):
        """ async save: insert or update """
        if not self.db_obj:
            return await self.__save(echo=echo, using=using, ts=ts)
        elif self.db_obj:
            return await self.__update(echo=echo, using=using, ts=ts)

    async def __save(self, echo=False, using='default', ts=None):
        """ private async insert """
        fields, values = [], []
        for field in self._meta.get_fields():
            value = getattr(self, field.attname)
            if not value:
                if field.primary_key:
                    continue
                if field.has_default():
                    value = field.get_default()
                elif isinstance(field, models.fields.DateTimeField):
                    if field.auto_now or field.auto_now_add:
                        value = datetime.datetime.now()
                elif isinstance(field, models.fields.DateField):
                    if field.auto_now or field.auto_now_add:
                        value = datetime.date.today()
                else:
                    continue
            fields.append(field.attname)
            values.append(SqlCompiler.value_format(value))
        result = await self.__insert(fields, values, echo=echo, using=using, ts=ts)
        # update primary key id if needed
        if result[0] == 1 and self._meta.pk.attname == 'id':
            self.id = result[1]
        return result

    @classmethod
    async def create(cls, echo=False, using='default', ts=None, **kwargs):
        obj = cls(**kwargs)
        ret = await obj.__save(echo=echo, using=using, ts=ts)
        print(ret)
        return obj

    @classmethod
    async def __insert(cls, fields, values, echo=False, using='default', ts=None):
        """ insert into sql """
        sql = 'INSERT INTO {} ({}) VALUES ({});'.format(cls._meta.db_table, SqlCompiler.fields_builder(fields),
                                                        ','.join(values))
        if echo:
            print('(transaction)' if ts is not None else '', sql)

        if ts is not None:
            return await cls.execute_ts(sql, 'commit', ts)
        else:
            return await cls.execute(sql, using, 'commit')

    @classmethod
    async def execute(cls, sql, using, action):
        """ execute sql """
        async with dbs[using].acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                if action == 'commit':
                    await conn.commit()
                    return cur.rowcount, cur.lastrowid
                else:
                    return await getattr(cur, action)()

    @classmethod
    async def execute_ts(cls, sql, action, ts):
        """ transaction execute """
        await ts.cursor.execute(sql)
        if action == 'commit':
            return ts.cursor.rowcount, ts.cursor.lastrowid
        else:
            return await getattr(ts.cursor, action)()

    @classmethod
    def model_fields(cls):
        return [field.attname for field in cls._meta.get_fields()]

    @classmethod
    async def select(cls, fields=None, where=None, order_by=None, limit=None, offset=0, for_update=False, count=None,
                     distinct=None, join_as=None, on=None, alias=None, using='default', echo=False, ts=None):
        """ async select """
        sql = 'SELECT '

        if count:
            assert len(count) == 1
            _sql = 'COUNT(`{}`) AS cnt' if not distinct else 'COUNT(DISTINCT(`{}`)) AS cnt'
            sql += _sql.format(count[0])
        else:
            # fields
            if not fields:
                fields = cls.model_fields()
            sql += SqlCompiler.fields_builder(fields)

        # table name
        sql += ' FROM {}'.format(cls._meta.db_table)
        if alias:
            sql += ' AS {}'.format(alias)
        if join_as and on:
            sql += ' JOIN {} AS {} ON {}'.format(join_as[0] if isinstance(join_as[0], str)
                        else join_as[0]._meta.db_table, join_as[1], on)

        # where
        if where:
            sql += SqlCompiler.where_builder(where)

        # order by
        if order_by:
            sql += SqlCompiler.order_by_builder(order_by)
        elif not count:
            if hasattr(cls._meta, 'ordering'):
                sql += SqlCompiler.order_by_builder(cls._meta.ordering)
            else:
                sql += ' ORDER BY `{}` ASC'.format(cls._meta.pk.attname)

        # limit offset
        if limit:
            sql += ' LIMIT {},{}'.format(offset, limit)
        if for_update:
            sql += ' FOR UPDATE'

        # final sql
        sql += ';'

        if echo:
            print('(transaction)' if ts is not None else '', sql)

        if ts is not None:
            ret = await cls.execute_ts(sql, 'fetchall', ts)
        else:
            ret = await cls.execute(sql, using, 'fetchall')

        # count
        if count:
            return ret[0][0] if ret else 0

        # return list or one object
        if ret:
            if not join_as and not on:
                ret = [cls(db_obj=True, **dict(zip(fields, item))) for item in ret]
            return ret[0] if limit == 1 else ret
        else:
            return

    async def __update(self, using='default', echo=False, ts=None):
        """ private async update """
        updated = []
        updated_fields = [k for k in self.original if self.original[k] != getattr(self, k)]
        if not updated_fields:
            return
        for field in updated_fields:
            value = getattr(self, field)
            updated.append('`{}`={}'.format(field, SqlCompiler.value_format(value)))
        for field in self._meta.get_fields():
            if hasattr(field, 'auto_now') and getattr(field, 'auto_now'):
                if isinstance(field, models.fields.DateTimeField):
                    updated.append('`{}`={}'.format(field.attname, SqlCompiler.value_format(datetime.datetime.now())))
                elif isinstance(field, models.fields.DateField):
                    updated.append('`{}`={}'.format(field.attname, SqlCompiler.value_format(datetime.date.today())))

        pk_name = self._meta.pk.attname
        sql = 'UPDATE {} SET {} WHERE `{}`={};'.format(self._meta.db_table, ','.join(updated), pk_name, self.pk)
        if echo:
            print('(transaction)' if ts is not None else '', sql)
        if ts is not None:
            return await self.execute_ts(sql, 'commit', ts)

        return await self.execute(sql, using, 'commit')

    async def delete(self, using='default', echo=False, ts=None):
        """ async delete """
        sql = 'DELETE FROM {} WHERE `{}`={};'.format(self._meta.db_table, self._meta.pk.attname, self.pk)
        if echo:
            print('(transaction)' if ts is not None else '', sql)
        if ts is not None:
            return await self.execute_ts(sql, 'commit', ts)
        return await self.execute(sql, using, 'commit')


class SqlCompiler:

    # TODO: more lookups to be implemented.
    lookups = {
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<=',
        'in': 'IN',
    }

    @classmethod
    def order_by_builder(cls, order_by: [str, list]):
        """ order by sql """
        if isinstance(order_by, str):
            return ' ORDER BY {}'.format(order_by)
        return ' ORDER BY ' + ','.join(['`{}` {}'.format(field.replace('-', ''),
                    'DESC' if field.startswith('-') else 'ASC') for field in order_by])

    @classmethod
    def fields_builder(cls, fields: list):
        """ fields format """
        return ','.join(['`{}`'.format(field) for field in fields])

    @classmethod
    def where_builder(cls, where: [Q, str, list]):
        """ where sql """
        if isinstance(where, str):
            return ' WHERE {}'.format(where)
        elif isinstance(where, Q):
            return ' WHERE {}'.format(cls._where_builder(where.kwargs, 'AND'))
        return ' WHERE ({}) OR ({})'.format(cls._where_builder(where[0], 'AND'), cls._where_builder(where[1], 'AND'))

    @classmethod
    def value_format(cls, v):
        """ value format """
        if isinstance(v, str) or isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
            return '"{}"'.format(v)
        return str(v)

    @classmethod
    def _where_builder(cls, dct, op):
        """ where subquery """
        conds = []
        for k, v in dct.items():
            _k = k.split('__')
            _op = cls.lookups[_k[-1]] if len(_k) > 1 else '='
            conds.append('`{}`{}{}'.format(_k[0], _op, cls.value_format(v)))

        return (' {} '.format(op)).join(conds)


def transaction(using='default'):
    """ return transaction context """
    return Atomic(using)


class Atomic:
    def __init__(self, using='default'):
        self.using = using
        self.conn = None
        self.cursor = None

    async def __aenter__(self):
        self.conn = await dbs[self.using].acquire()
        self.cursor = await self.conn.cursor()
        if not self.conn or not self.cursor:
            raise ValueError('Can not get db connection or cursor')
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        try:
            await self.conn.commit()
            pass
        except Exception as e:
            print(e)
            await self.conn.rollback()
        finally:
            await self.cursor.close()
            dbs[self.using].release(self.conn)
            self.conn = None
            self.cursor = None

    def __await__(self):
        return self


class DjBaseModel(models.Model):
    """ Django Base Model """
    class Meta:
        abstract = True


if not settings.ASYNC:
    BaseModel = DjBaseModel
else:
    BaseModel = AsyncBaseModel
print(settings.ASYNC, BaseModel)
