import time
import traceback

import MySQLdb
import mysql.connector
from Config import *
import logging

class DatabaseHelper:
    connection = None
    cursor = None
    db = None

    @staticmethod
    def buildConnection():
        connection_config_dict = {
            'user': DB_USER,
            'password': DB_PASSWORD,
            'host': 'localhost',
            'database': DB_NAME,
            'raise_on_warnings': True,
            'autocommit': True,
            'pool_name': 'mysqlpool',
            'pool_size': 20,
            'connection_timeout': 1000,
            'charset': 'utf8mb4'
        }
        try:
            mysql.connector.connect(**connection_config_dict)
        except Exception as e:
            logging.error(e)
            print(e)
            time.sleep(10)
            DatabaseHelper.buildConnection()

    def __init__(self, autoCommit: bool = True):
        self.db = mysql.connector.connect(pool_name='mysqlpool')
        self.db.autocommit = autoCommit

    def changeAutoCommit(self, autoCommit: bool):
        self.db.autocommit = autoCommit

    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.db.is_connected():
            self.db.close()

    def getCursor(self):
        self.cursor = self.db.cursor(buffered=True)
        # self.cursor.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle
        # self.cursor.execute("SET CHARACTER SET utf8mb4;")  # same as above
        # self.cursor.execute("SET character_set_connection=utf8mb4;")  # same as above
        # self.cursor.execute('SET GLOBAL connect_timeout=28800')
        # self.cursor.execute('SET GLOBAL interactive_timeout=28800')
        # self.cursor.execute('SET GLOBAL wait_timeout=28800')
        # self.cursor.execute('set GLOBAL max_allowed_packet=67108864')
        return self.cursor

    def getOne(self, sql, param=None, cache=False):
        results = None
        cursor = None
        try:
            if not self.db.is_connected():
                time.sleep(1)
                self.db.reconnect()
            cursor = self.getCursor()
            cursor.execute(sql, param)
            if cursor.rowcount == 0:
                return None
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            if len(rows) == 0:
                return None
            results = rows[0]
        except MySQLdb.Error as e:
            print(e)
            logging.error(sql, e)
            traceback.print_exc()
            results = None
        except Exception as e:
            logging.error(sql, e)
            print(e)
            traceback.print_exc()
            results = None
        finally:
            if cursor is not None:
                cursor.close()
        return results

    def getCount(self, sql):
        return self.getOne(f"select count(*) as cnt from({sql}) as x")['cnt']

    def getAll(self, sql, param=None):
        results = None
        cursor = None
        try:
            if not self.db.is_connected():
                time.sleep(1)
                self.db.reconnect()
            cursor = self.getCursor()
            cursor.execute(sql, param)
            if cursor.rowcount == 0:
                results = []
            else:
                columns = [col[0] for col in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                results = rows
        except MySQLdb.Error as e:
            print(e)
            logging.error(sql, e)
            traceback.print_exc()
            results = None
        except Exception as e:
            print(e)
            logging.error(sql, e)
            traceback.print_exc()
            results = None
        finally:
            if cursor is not None:
                cursor.close()
        return results

    def justExecute(self, sql, params=None, multi=False):
        results = None
        cursor = None
        try:
            if not self.db.is_connected():
                time.sleep(1)
                self.db.reconnect()
            cursor = self.getCursor()
            if params is None:
                cursor.execute(sql, multi=multi)
            else:
                cursor.execute(sql, params, multi=multi)
            results = True
        except MySQLdb.Error as e:
            print(sql, e)
            logging.error(sql, e)
            traceback.print_exc()
            results = False
        except Exception as e:
            print(sql, e)
            logging.error(sql, e)
            traceback.print_exc()
            results = False
        finally:
            if cursor is not None:
                cursor.close()
        return results

    def executeWithId(self, sql, params):
        results = None
        cursor = None
        try:
            if not self.db.is_connected():
                time.sleep(1)
                self.db.reconnect()
            cursor = self.getCursor()
            cursor.execute(sql, params)
            return cursor.lastrowid
        except MySQLdb.Error as e:
            print(e)
            logging.error(sql, e)
            traceback.print_exc()
            self.db.ping(True)
            results = False
        except Exception as e:
            print(e)
            logging.error(sql, e)
            traceback.print_exc()
            results = False
        finally:
            if cursor is not None:
                cursor.close()
        print(results)
        return results
