#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Reference：http://blog.csdn.net/gvfdbdf/article/details/49254701

__author__ = 'Jarod Zheng'

import logging; logging.basicConfig(level=logging.INFO)
import asyncio, os, json, time
import aiomysql

from datetime import datetime
from aiohttp import web

###############################################
# 创建连接池，封装SQL执行语句等公共方法
###############################################

# 创建连接池
async def create_pool(loop, **kw):
	
	logging.info('create database connection pool...')

	global __pool
	__pool = await aiomysql.create_pool(
		host=kw.get('host', 'localhost'), 
		port=kw.get('port', 3306), 
		user=kw['user'], 
		password=kw['password'], 
		db=kw['db'], 
		charset=kw.get('charset', 'utf8'), 
		autocommit=kw.get('autocommit', True), 
		maxsize=kw.get('maxsize', 10), 
		minsize=kw.get('minsize', 1), 
		loop=loop
		)

# 封装 SQL select 执行语句为函数，用于执行查找操作
async def select(sql, args, size=None):

	log(sql, args)

	global __pool
	with (await __pool) as conn:
		cur = await coon.cursor(aiomysql.DictCursor)
		await cur.execute(sql.replace('?', '%s'), args or ()) # 将语句中？替换成%s
		if size:
			rs = await cur.fetchmany(size)
		else:
			rs = await cur.fetchall()
		await cur.close()

		logging.info('rows returned: %s' % len(rs))
		return rs

# 封装 SQL execute 执行语句为函数，用于执行Insert， Update， Delete操作
async def execute(sql, args):

	log(sql, args)

	with (await __pool) as conn:
		try: 
			cur = await conn.cursor()
			await cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount
			await cur.close()
		except BaseException as e:
			raise
		return affected

# 将数字转换成相应个数'?'组合成的字符串
def create_args_string(num):
	L = []
	for n in range(num): # 将num个'?'装入列表L中
		L.append('?')
	return ','.join(L) # 将L中所有'?'用逗号分隔组成一个字符串


#############################################
# 创建数据库字段类型
#############################################

# 定义数据库表通用字段
class Field(object):
	# 字段名字，类型，主键，默认值
	def __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default

	# 打印数据库表时候字段类型时，输出：类名，字段类型：名字
	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.name, self.column_type, self.name)


# 定义字符串型数据库字段
class StrignField(Field):

	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name, ddl, primary_key, default)

# 定义整数型数据库字段
class IntegerField(Field):

	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name, 'bigint', primary_key, default)

# 定义布尔型数据库字段
class BooleanField(Field):

	def __init__(self, name=None, primary_key=False, default=None):
		super().__init__(name, 'boolean', primary_key, default)

# 定义浮点型数据库字段
class FloatField(Field):

	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name, 'real', primary_key, default)

# 定义文本型数据库字段
class TextField(Field):

	def __init__(self, name=None, primary_key=False, default=None):
		super().__init__(name, 'Text', primary_key, default)


################################################
# 创建Model类型，并在Model元类中相关的封装和方法
################################################


class ModelMetaclass(type):

	# __new__的执行在__init__之前
	# cls:代表要__init__的类，比如下面的User和Model
	# basees:代表继承父类的集合
	# attrs：类属性集合
	def __new__(cls, name, bases, attrs):
		
		# 创造类的时候排除对“Model”类的修改
		if name=='Model':
			return type.__new__(cls, name, bases, attrs)

		# 获取table名称
		tableName = attrs.get('__table__', None) or name
		logging.info('found model: %s (table: %s)' % (name, tableName))
		
		# 获取field和主键
		mappings = dict()
		fields = []
		primaryKey = None

		for k, v in attrs.items():
			# 如果attrs中元素是Field类型，捕获到mapping中
			if isinstance(v, Field):
				logging.info('found mapping: %s ==> %s' % (k, v))
				mapping[k] = v

				# 找到主键，存入primaryKey中
				if v.primary_key:
					if primaryKey: # 若类实例已经事先存在主键，则主键重复
						raise RuntimeError('Duplicate primary key for field: %s' % k)
					primaryKey = k
				else:
					fields.append(k) # fields存放所有非primary key的key值

		# 主键不存在
		if not primaryKey:
			raise RuntimeError('Primary key not found.')

		# 从类属性中删除Field属性，刚刚已经存到mappings里去了（防止实例属性遮盖类的同名属性）
		for k in mapping.keys():
			attrs.pop(k)

		# 将fields里的属性名强制转换成字符串，形成列表
		escaped_fields = list(map(lambda f: '%s' % f, fields))


		attrs['__mappings__'] = mappings # 保存属性和列的映射关系
		attrs['__table__'] = tableName # 保存表名
		attrs['__primary_key__'] = primaryKey # 保存主键名
		attrs['__fields__'] = fields # 保存主键外的属性名集合

		# 构造SQL语句
		# 检索数据库表，表tableName中包含主键primaryKey在内的所有Field字段全部列出
		attrs['__select__'] = "select '%s', %s from '%s'" % (primaryKey, ','.join(escaped_fields), tableName)
		# 插入数据库表，往表tableName中插入包含主键primaryKey在内的所有Field字段，出入值全为'?'占位符
		attrs['_insert__'] = "insert into '%s' (%s, %s) values (%s)" % (tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields)+1)) # 最后一个%s添加一系列'?'
		# 更改数据库表，表tableName中，所有非primaryKey字段 = ?, 当primaryKey = ?的时候
	##### mappings.get(f).name or f为什么不能直接f ??
		attrs['__update__'] = "update '%s' set %s where '%s'=?" % (tableName, ','.join(map(lambda f: "'%s'=?" % (mappings.get(f).name or f), fields)), primaryKey)
		# 删除数据库表记录，表tableName中，primaryKey = ?的记录
		attrs['__delete__'] = "delete from '%s' where '%s'=?" % (tableName, primaryKey)

		return type.__new__(cls, name, bases, attrs)


# 定义数据库表的基类：Model，Model的任意子类都可以对应一张数据库表
class Model(dict, metaclass=ModelMetaclass):
	
	def __init__(self, **kw):
		# 集成元类中给所有初始化信息，字典，SQL构造函数
		super(Model, self).__init__(**kw)

	# __getattr__和__setattr__实现动态类属性操作
	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)

	def __setattr__(self, key):
		self[key] = value

##### ???
	def getValue(self, key):
		# 调用内建函数getattr，若不存在属性key，则返回None
		return getattr(self,  key, None) #

##### ???
	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		# 如果属性key不存在，在__mappings__中查找属性值，并返回改属性值的default值
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default

				logging.debug('using default value for %s: %s' % (key, str(value)))
				setattr(self, key, value)
		return value


	# 数据库操作方法: find, find all, save, update, delete...
	@classmethod
	async def find(cls, pk):
		
		# find object by primary key
		rs = await select("%s where '%s'=?" % (cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs)==0:
			return None
		return cls(**rs[0])


	@classmethod
	# 查询数据库表所有记录，查询满足条件where的所有记录，查询结果排序orderBy
	async def findAll(cls, where=None, args=None, **kw):
		sql = [cls.__select__]
		
		# 如果传入匹配条件where
		if where:
			sql.append('where')
			sql.append(where)
		

		if args is None:
			args = []
		
		# 定义结果排序
		orderBy = kw.get('orderBy', None)
		if orderBy：
			sql.append('orderBy')
			sql.append(orderBy)

		# 定义限制参数limit
		limit = kw.get('limit', None)
		if limit is not None:
			sql.append('limit')
			# 如果limit是整数，SQL后面直接添加'?'
			if isinstance(limit, int):
				sql.append('?')
				args.append(limit)


		rs = await select("%s where '%s'=?" % (cls.__select__, cls.__primary_key__), )

	@classmethod
	async def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = await execute(slef._insert__, args)
		if rows != 1:
			logging.warn('failed to insert record: affected rows: %s' % rows)



###########################################
from orm import Model, StringField, IntegerField


class User(Model):
	__table__ = 'users'

	id = IntegerField(primary_key=True)
	name = StrignField()

'''
实例操作
user = User(id=123, name='Michael')
user.insert()
users = user.findAll()
'''
			
