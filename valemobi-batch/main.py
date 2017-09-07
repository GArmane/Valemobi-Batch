import os
import sqlite3 as sqlite
import sys


def database_connection_factory(database_name):
    try:
        connection = sqlite.connect('Data/' + database_name)
        return connection
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)


def database_exists(database_name):
    return os.path.isfile('Data/' + database_name)


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
    finally:
        if cursor is True:
            cursor.close()


def execute_sql(database_connection, script_path):
    try:
        cursor = database_connection.cursor()
        script = open(script_path, 'r')
        sql = script.read()
        result = cursor.executescript(sql)
        database_connection.commit()
        return result
    except IOError:
        print('Error: ' + script_path + ' file not found...')
        sys.exit(1)
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)
    finally:
        if cursor is True:
            cursor.close()
        if script is True:
            script.close()


def get_customers(database_connection):
    try:
        cursor = database_connection.cursor()
        cursor.execute(
            '''
            select * from tb_customer_account
            where id_customer between 1500 and 2700
            and vl_total > 560
            order by vl_total desc;
            '''
        )
        return cursor.fetchall()
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)
    finally:
        if cursor is True:
            cursor.close()


def get_average(database_connection):
    try:
        cursor = database_connection.cursor()
        cursor.execute(
            '''
            select avg(vl_total) from tb_customer_account
            where id_customer between 1500 and 2700
            and vl_total > 560;
            '''
        )
        return cursor.fetchone()
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)
    finally:
        if cursor is True:
            cursor.close()


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

        print('Calculando média...')
        print('Média total: {:.2f}'.format(get_average(connection)[0]))

        print('Clientes usados no cálculo de média...')
        for customer in get_customers(connection):
            print('ID: {}, CPF/CNPJ: {}, Nome: {}, Ativo: {}, Saldo: {}'
                  .format(
                      customer[0],
                      customer[1],
                      customer[2],
                      'Sim' if customer[3] is 1 else 'Não',
                      customer[4]))

        sys.exit(0)
    except sqlite.Error as error:
        print('Error: %s' % error.args[0])
        sys.exit(1)
    finally:
        if connection:
            connection.close()
            sys.exit(0)


if __name__ == '__main__':
    main()
