import sys
import os
import sqlite3 as sqlite

def database_connection_factory(database_name):
    try:
        connection = sqlite.connect('Data/' + database_name)
        return connection
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)


def database_exists(database_name):
    return os.path.isfile('Data/' + database_name)


def execute_sql(database_connection, script_path):
    try:
        cursor = database_connection.cursor()
        script = open(script_path, 'r').read()
        cursor.executescript(script)
        database_connection.commit()
    except IOError:
        print('Error: ' + script_path + ' file not found...')
        sys.exit(1)
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)


def table_has_data(database_connection, table_name):
    try:
        cursor = database_connection.cursor()
        sql_query = r'SELECT * from ' + table_name + ' LIMIT 1'
        cursor.execute(sql_query)
        data = cursor.fetchone()
        if data:
            return True
        else:
            return False
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)


def main():

    db_name = r'test.db'
    db_source = r'Data/DataSource.sql'
    tb_name = r'tb_customer_account'
    tb_source = r'Data/TableSource.sql'

    connection = None

    try:
        print('Conectando a base de dados...')
        connection = database_connection_factory(db_name)
        print('Checando tabela de clientes...')
        execute_sql(connection, tb_source)
        if table_has_data(connection, tb_name) is False:
            print('Populando tabela de clientes...')
            execute_sql(connection, db_source)
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)
    finally:
        if connection:
            connection.close()
            sys.exit(0)


if __name__ == '__main__':
    main()
