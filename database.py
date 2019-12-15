#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql


# table create: attention, we have set the primary key as auto_increment, and we have introduced queue to avoid
# conflict, so we don't need to use lock to avoid the confusion of primary keys
time_quintuple_table = '''
    CREATE TABLE time_quintuple_tbl(
        time_quintuple_id INT NOT NULL AUTO_INCREMENT,
        decimal_time DECIMAL(16,6) NOT NULL,
        src_mac VARCHAR(17) NOT NULL,
        dst_mac VARCHAR(17) NOT NULL,
        src_ip VARCHAR(15) NOT NULL,
        dst_ip VARCHAR(15) NOT NULL,
        PRIMARY KEY (time_quintuple_id)
    );'''

time_quintuple_insert = '''INSERT INTO time_quintuple_tbl(decimal_time, src_mac, dst_mac, src_ip, dst_ip) VALUES 
    (%f, '%s', '%s', '%s', '%s');'''


class DbCon(object):

    # create mysql connection and cursor
    def __init__(self):
        self._connection = pymysql.Connect(
            host='192.168.190.8',
            user='admin',
            password='admin',
            db='time_quintuple',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        self._cursor = self._connection.cursor()

    # after using mysql, close cursor and connection
    def db_close(self):
        try:
            self._cursor.close()
            self._connection.close()
        except Exception as e:
            print(e)

    # receives table create sql, create tables
    def db_table_create(self, table_create_sql):
        try:
            self._cursor.execute(table_create_sql)
        except Exception as e:
            print(e)

    # drop useless table
    def db_table_drop(self, table_name):
        try:
            self._cursor.execute('DROP TABLE IF EXISTS %s' % table_name)
        except Exception as e:
            print(e)

    # insert rows into tables
    def db_table_insert(self, table_column, one_row_value):
        try:
            time, mac_src, mac_dst, ip_src, ip_dst = \
                one_row_value[0], one_row_value[1], one_row_value[2], one_row_value[3], one_row_value[4]
            self._cursor.execute(table_column % (time, mac_src, mac_dst, ip_src, ip_dst))
            self._connection.commit()

        except Exception as e:
            print(e)
