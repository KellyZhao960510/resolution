#!/usr/bin/env python3
# coding:utf8
'''
用于封装mysql操作
'''


import pymysql
import re
import shutil
import os


class MysqlHandler():
    """docstring for MysqlHandler"""

    def __init__(self, host='localhost', port=3306, db='mysql', user='root', passwd='123456', charset='utf8'):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.passwd = passwd
        self.charset = charset
        self.open()

    def open(self):
        self.conn = pymysql.connect(host=self.host, port=self.port, db=self.db,
                                    user=self.user, passwd=self.passwd, charset=self.charset)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def show_databases(self):
        '''显示数据库列表'''
        sql = 'show databases;'
        self.cursor.execute(sql)
        self.databases = self.cursor.fetchall()
        for i in self.databases:
            yield i[0]

    def create_database(self, dbname):
        '''创建数据库'''
        sql = 'create database {} default charset={}'.format(
            dbname, self.charset)
        try:
            self.cursor.execute(sql)
        except Exception as e:
            raise
        else:
            print('{}库创建成功'.format(dbname))

    def use_database(self, db):
        '''跳转到某个数据库'''
        self.db = db
        self.open()

    def drop_database(self, db):
        '''删除数据库'''
        sql = 'drop database {}'.format(db)
        try:
            self.cursor.execute(sql)
        except:
            raise
        else:
            self.conn.commit()
            print('{}删除成功'.format(db))

    def create_table(self, tablename, *args):
        field = ''
        for i in args:
            field += '{} {},'.format(i[0], i[1])
        field = field[:-1]
        print(field)
        sql = 'create table {} ({}) default charset={}'.format(
            tablename, field, self.charset)
        print(sql)
        try:
            self.cursor.execute(sql)
        except Exception as e:
            raise

    def add_index(self, table, field, index_type, index_name=''):
        '''
            为表添加index, unique, 或者　primary key(默认不添加auto_increment)
            index_type: 索引类型　
            index_name: 索引名
        '''
        if index_type in ("index", "unique"):
            sql = "create {} {} on {}({})".format(index_type, index_name, table, field)
        elif index_type == 'primary key':
            sql = "alter table {} add primary key({})".format(table, field)
        else:
            print('请选择{}中的一种添加索引'.format('index | unique index | primary key'))
            return
        try:
            self.cursor.execute(sql)
        except:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            print('{}索引添加成功!'.format(index_type))

    def drop_index(self, table, index_type, index_name):
        '''
            为表添加index, unique, 或者　primary key(默认不添加auto_increment)
            index_type: 索引类型　
            index_name: 索引名
        '''
        if index_type in ('index', 'unique'):
            sql = "drop index {} on {}".format(index_name, table)
        elif index_type == 'primary key':
            sql = "alter table {} drop primary key".format(table)
        else:
            print('请选择{}中的一种删除索引'.format('index | unique index | primary key'))
            return
        try:
            self.cursor.execute(sql)
        except:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            print('{}索引删除成功!'.format(index_type))

    def add_foreign_key(self, table, field, refer_table, refer_field, FK_ID, link_control='cascade'):
        '''
            为表添加外键
            refer_table: 被参考表名
            refer_field: 被参考字段名
            FK_ID: 外键名
            link_control: 级联动作 默认为＂cascade＂
                1、cascade ：数据级联更新
                    当主表删除记录 或者 更新被参考字段的值时,从表会级联更新
                2、restrict 
                    1、当删除主表记录时,如果从表中有相关联记录则不允许主表删除
                    2、更新同理
                3、set null
                    1、当主表删除记录时,从表中相关联记录的参考字段值自动设置为NULL
                    2、更新同理
                4、no action
                    同 restrict,都是立即检查外键限制  

            外键使用规则
            1、两张表被参考字段和参考字段数据类型要一致
            2、被参考字段必须是 key 的一种,通常是 primary key
        '''
        sql = '''
                alter table {} add　constraint {} foreign key({})
                references {}({})
                on delete {}
                on update {}
            　　'''.format(table, FK_ID, field, refer_field, refer_field, link_control)
        try:
            self.cursor.execute(sql)
        except:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            print('外键{}添加成功!'.format(FK_ID))


    def drop_foreign_key(self, table, FK_ID):
        '''
            删除外键
            FK_ID: 外键名
        '''
        sql = "alter table {} drop foreign key {};".format(table, FK_ID)
        try:
            self.cursor.execute(sql)
        except:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            print('外键{}删除成功!'.format(FK_ID))


    def select(self, table, field='*', where='', group_by='', having='', order_by='', order='ASC', limit=''):
        '''
        查询表记录
    　　　 示例: MysqlHandler().select(table="sanguo", field="country,avg(gongji) as pjgj", group_by="country",having="pjgj > 105", order_by="pjgj", order='DESC', limit="2")
        参数详注:
            group_by: 按哪个字段分组
                1、group by之后的字段名必须要为select(参数field的值)之后的字段名
                2、如果select之后的字段没有在group by语句之后,则必须要对该字段进行聚合处理(聚合函数)
            having:　对查询结果进一步筛选,通常与gruop by联合使用，用于对聚合函数的判断
            order_by:　按哪个字段进行排序，也可以是聚合函数
            order:　默认＂ASC＂: 升序  "DESC": 降序
            limit:　限制查询记录个数
                1、limit="n" ：-->显示n条记录
                2、limit="m,n" ：-->从第(m+1)条开始,显示n条记录
                     limit="4,5 ：显示 第5名-第9名
                     ## m的值是从0开始计数的

        '''
        sql = 'select {} from {} where {} group by {} having {} order by {} {} limit {}'\
            .format(field, table, where, group_by, having, order_by, order, limit)

        rst = re.match(
            'select\s.+\sfrom\s.+\s(?P<where>where\s.*\s)(?P<group_by>group\sby\s.*\s)(?P<having>having\s.*\s)(?P<order_by>order\sby\s.*\s)(?P<limit>limit\s[0-9]?,?[1-9]?)', sql)
        
        if not where:
            sql = sql.replace('where ', '')
        if not group_by:
            sql = sql.replace(rst.group('group_by'), '')
        if not having:
            sql = sql.replace(rst.group('having'), '')
        if not order_by:
            sql = sql.replace(rst.group('order_by'), '')
        if not limit:
            sql = sql.replace(rst.group('limit'), '')

        self.cursor.execute(sql)
        for i in self.cursor.fetchall():
            yield i

    def insert(self, table, data, field=''):
        '''插入表记录
        　　　示例: MysqlHandler().insert(test, (id, name), ((1, kelly),(2, lucy)))
        '''
        sql = 'insert into {} ({}) values{}'.format(
            table, ', '.join(field), ', '.join(map(str, data)))
        try:
            self.cursor.execute(sql)
        except:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            print('记录插入成功!')

    def show_tables(self):
        '''显示某个数据库内表格列表'''
        sql = 'show tables;'
        self.cursor.execute(sql)
        self.databases = self.cursor.fetchall()
        for i in self.databases:
            yield i[0]

    def desc_table(self, tablename):
        '''查看表结构'''
        sql = 'desc {}'.format(tablename)
        self.cursor.execute(sql)
        for i in self.cursor.fetchall():
            yield i

    def drop_table(self, tablename):
        '''删除数据库内指定表'''
        sql = 'drop table {}'.format(tablename)
        self.cursor.execute(sql)
        print('{}.{}删除成功'.format(self.db, tablename))

    def add_field(self, table, field, datatype, postion=None):
        '''添加字段名
           table:表名
        　　　field:字段名
        　　　datatype:对应字段类型
        　　　position:添加后字段所处位置,默认为空，添加到最后　　(first, after field)
        '''
        sql = 'alter table {} add {} {} {}'.format(
            table, field, datatype, postion)
        self.cursor.execute(sql)
        print('字段{} {}添加成功'.format(field, datatype))

    def modify_field(self, table, field):
        '''
            修改字段类型
            table:表名
            field:字段名 字段类型
            eg:
                MysqlHandler().modify_field(table='test', field='id int')  修改id数据类型为int
                MysqlHandler().modify_field(table='test', field='number int default 0')  增加默认约束
                MysqlHandler().modify_field(table='test', field='id int　not null')  增加非空约束  

        '''
        sql = 'alter table {} modify {}'.format(table, field)
        self.cursor.execute(sql)
        print('字段{}修改成功'.format(field))

    def drop_field(self, table, field):
        '''
            删除字段
           table:表名
        　　　field:字段名
        '''
        sql = 'alter table {} drop {}'.format(table, field)
        self.cursor.execute(sql)
        print('字段{}删除成功'.format(field))

    def rename_field(self, table, old_field, new_field, datatype):
        '''重命名字段名
           table:表名
        　　　old_field:旧字段名
        　　　new_field:新字段名
        　　　datatype:对应字段类型
        '''
        sql = 'alter table {} change {} {} {}'.format(
            table, old_field, new_field, datatype)
        self.cursor.execute(sql)
        print('{} 字段重命名为: {}'.format(old_field, new_field))

    def rename_table(self, old_table, new_table):
        '''重命名字段名
        　　　old_table:旧表名
        　　　new_table:新表名
        '''
        sql = 'alter table {} rename {}'.format(old_table, new_table)
        self.cursor.execute(sql)
        print('{} 表重命名为: {}'.format(old_table, new_table))

    def drop_record(self, table, where=''):
        '''删除表记录,where不传参,默认删除全部记录'''
        sql = 'delete from {} where {}'.format(table, where)
        try:
            self.cursor.execute(sql)
        except:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            print('记录删除成功!')

    def update_record(self, table, set_record, where):
        '''更新表记录
        　　　eg: MysqlHandler().update_record('test', set_record="id=2,sex='m'", where="name='lucy'")
        '''
        sql = 'update {} set {} where {}'.format(table, set_record, where)
        try:
            self.cursor.execute(sql)
        except:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            print('记录修改成功!')



    def view_db_path(self):
        sql = 'show variables like "secure_file_priv"'
        self.cursor.execute(sql)
        return self.cursor.fetchall()[0][1]



    def __enter__(self):
        return self

    def __exit__(self, exec_type, exec_value, exec_tb):
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    # mysql = MysqlHandler()
    # mysql.create_database('test1')
    # rst = mysql.show_databases()
    # for i in rst:
    #     print(i)
    # mysql.close()
    # print(help('query'))
    with MysqlHandler() as mysql:
        mysql.use_database('MOSHOU')
        # mysql.add_index(table="sanguo", field="id", index_type="primary key")
        mysql.modify_field(table="sanguo", field="id int auto_increment")
        for i in mysql.desc_table("sanguo"):
            print(i)


#         print(mysql.view_db_path())
    #     # mysql.add_field('userinfo', 'birthday', 'int', 'first')
    #     # mysql.drop_field('userinfo', 'birthday')
    #     # mysql.modify_field('userinfo', 'birthday', 'timestamp')
    #     # mysql.create_table('dict', (('id', 'int')))
    #     # mysql.rename_table('userinfo', 'user')
    #     # for i in mysql.desc_table('userinfo'):
    #     #     print(i)
    #     # mysql.insert('user', ((6, 'kelly', 234354),))
    #     # mysql.drop_record('user', where="username='kelly'")
    #     # mysql.update_record(
    #     #     'user', set_record="id=3, username='kelly'", where="id=2")
    #     for i in mysql.select(table="sanguo", field="country", group_by="country"):
    #         print(i)
    #     # mysql.select(table="sanguo", field="country,avg(gongji) as pjgj", group_by="country",\
    #             having="pjgj > 105", order_by="pjgj", order='DESC', limit="2")
    # for i in mysql.show_tables():
    #     print(i)
    # s = 'select country,avg(gongji) as pjgj from sanguo where  group by country having pjgj > 105 order by pjgj DESC limit 2'.replace('where ', '')
    # print(s)
