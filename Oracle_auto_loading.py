#!/usr/bin/env python
# coding: utf-8

# ## 1. Auto Creating Staged Table and Importing data from Excel File to Oracle DB

# Program 1: Auto Creating Staged Table and Importing data from Excel File to Oracle DB
# Import modules
import cx_Oracle
import pandas as pd
import os
import csv
# Set up the connection and the target database/schema
# Note: type 'sudo hostname <hostname>' in terminal to match your host name with the name in /etc/hosts file temporarily if getting 'ORA-21561: OID generation failed' error.
# Close the excel files opened that are going to be used in this program when seeing: XLRDError: Unsupported format, or corrupt file: Expected BOF record; found b'\x08user168' 
username = 'MASY_LC4013'
password = 'MASY_LC4013'
server_ip = 'localhost:1522'
service_name = 'app12c'
DB = 'MASY_LC4013'
# Define a function to confirm drop
def confirm_bef_drop(prompt, complaint='yes or no, please'): #complaint:key argument, 'Yes or no, please!': positional argument
    while True:
        confirm = input(prompt)   # In Python 2, it is called raw_input()
        if confirm in ['y', 'ye', 'yes', 'YES']:  #same as: if ok == yes or ok == y or ok == ye
            try: 
                cur.execute('DROP TABLE ' + table)
            except:
                print('No table with the same name existed, no need to drop')
            return True
        if confirm in ['n', 'no', 'nop', 'nope', 'NO']:
            raise IOError('Please change your excel name')
        print(complaint)

# Create the connection and the cursor object
con = cx_Oracle.connect(username + '/' + password + '@' + server_ip + '/' + service_name)
print('The connection to DB: %s' % con)
cur = con.cursor()
print('Cursor object created: %s' % cur)
# Iterate through all the excel files in xlsx format under the current working directory to create staged table and import the data to the staged table
# Note: This will set up the name of your staged table as the UPPER CASE of the name of the excel file.
for excel in os.listdir(os.getcwd()):
    if excel.endswith('.xlsx'):
        table = excel.split('.')[0].upper()
        print('Excel File: %s' % excel)
        print('Staged Table: %s' % table)
        df = pd.read_excel(excel)
        # Confirm with user before drop the existed table with the same name that is going to be created.
        confirm_bef_drop('If you currently have a table named %s, ' % table + 'it will be dropped, type no if you don\'t want to do so, this program will breaks. You can re-run this after you changed your excel file name. Otherwise, type yes.')
        # Create a Staged table with the table name of 'table' and set the data sizes.
        sql_create_tbl = 'CREATE TABLE ' + table + ' ('
        varchar2_size = '50'
        number_size = '38'
        lis = []
        dtypes = list(zip(df.dtypes.index, df.dtypes.values))
        for elem in dtypes:
            if str(elem[1]).find('datetime') != -1:
                lis.append(str(elem[0] + ' DATE'))
            elif str(elem[1]) == 'float64' or str(elem[1]) == 'int64': 
                lis.append(str(elem[0] + ' NUMBER'+ '(' + number_size + ')'))
            elif str(elem[1]) == 'object': 
                lis.append(str(elem[0] + ' VARCHAR2'+ '(' + varchar2_size + ')'))
        cols_dtypes = ','.join(lis)
        sql_create_tbl = sql_create_tbl + cols_dtypes + ')'
        print('-'*100)
        print('CREATE command: %s' % sql_create_tbl)
        print('-'*100)
        df = df.where((pd.notnull(df)), None)
        cur.execute(sql_create_tbl)
#         for error in cur.getbatcherrors():
#             print("Error", error.message, "at row offset", error.offset)
        # Insert data from excel files end with xlsx to the staged table
        col_name = list(df.columns)
        sql_col_name = ','.join(col_name)
        sql_insert = 'INSERT INTO ' + table + ' (' + sql_col_name + ') VALUES ('
        sql_values = []
        for i in range(1, len(col_name) + 1):
            sql_values.append(':' + str(i))
        sql_values = ','.join(sql_values) + ')'
        sql_insert += sql_values
        print('INSERT command: %s' % sql_insert)
        print('-'*100)
        rows = [tuple(x) for x in df.values]
        print('Data Sample: %s' % rows[0:2])
        print('-'*100)
        cur.executemany(sql_insert, rows, batcherrors=True, arraydmlrowcounts=True)
        #Return rows affected: Oracle Client library needs to be version 12.1 or higher to run the the code below in comment
        #rowCounts = cur.getarraydmlrowcounts()
        #for count in rowCounts:
            #print("Inserted", count, "rows.")
        for error in cur.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
        con.commit()
        continue
    else:
        continue
cur.close()
con.close()


# ## 2. Insert Data from Staged table to Relational Tables
# - Functionalities not fully realized yet. Might cause mistakes in some circumstances. Will be imporved at the next update


# Program 2: 1. Auto Creating Staged Table and Importing data from Excel File to Oracle DB 2.Insert Data from Staged table to Relational Tables (Functionalities not fully realized yet. Might cause mistakes in some circumstances. Will be imporved at the next update)
# Import modules
import cx_Oracle
import pandas as pd
import os
import csv
# Set up the connection and the target database/schema
# Note: type 'sudo hostname <hostname>' in terminal to match your host name with the name in /etc/hosts file temporarily if getting 'ORA-21561: OID generation failed' error.
# Close the excel files opened that are going to be used in this program when seeing: XLRDError: Unsupported format, or corrupt file: Expected BOF record; found b'\x08user168' 
username = 'MASY_LC4013'
password = 'MASY_LC4013'
server_ip = 'localhost:1522'
service_name = 'app12c'
DB = 'MASY_LC4013'
#
staged_tables = ['HR_DATA']
target_tables = [['LC_JOB', 'LC_JOB_HISTORY', 'LC_LOCATION', 'LC_REGION', 'LC_EMPLOYEE', 'LC_DEPARTMENT', 'LC_COUNTRY']]
insert_dic = dict(list(zip(staged_tables, target_tables)))
# Define a function to confirm drop
def confirm_bef_drop(prompt, complaint='yes or no, please'): #complaint:key argument, 'Yes or no, please!': positional argument
    while True:
        confirm = input(prompt)   # In Python 2, it is called raw_input()
        if confirm in ['y', 'ye', 'yes', 'YES']:  #same as: if ok == yes or ok == y or ok == ye
            try: 
                cur.execute('DROP TABLE ' + table)
            except:
                print('No table with the same name existed, no need to drop')
            return True
        if confirm in ['n', 'no', 'nop', 'nope', 'NO']:
            raise IOError('Please change your excel name')
        print(complaint)

# Create the connection and the cursor object
con = cx_Oracle.connect(username + '/' + password + '@' + server_ip + '/' + service_name)
print('The connection to DB: %s' % con)
cur = con.cursor()
print('Cursor object created: %s' % cur)
# Iterate through all the excel files in xlsx format under the current working directory to create staged table and import the data to the staged table
# Note: This will set up the name of your staged table as the UPPER CASE of the name of the excel file.
for excel in os.listdir(os.getcwd()):
    if excel.endswith('.xlsx'):
        table = excel.split('.')[0].upper()
        print('Excel File: %s' % excel)
        print('Staged Table: %s' % table)
        df = pd.read_excel(excel)
        # Confirm with user before drop the existed table with the same name that is going to be created.
        confirm_bef_drop('If you currently have a table named %s, ' % table + 'it will be dropped, type no if you don\'t want to do so, this program will breaks. You can re-run this after you changed your excel file name. Otherwise, type yes.')
        # Create a Staged table with the table name of 'table' and set the data sizes.
        sql_create_tbl = 'CREATE TABLE ' + table + ' ('
        varchar2_size = '50'
        number_size = '38'
        lis = []
        dtypes = list(zip(df.dtypes.index, df.dtypes.values))
        for elem in dtypes:
            if str(elem[1]).find('datetime') != -1:
                lis.append(str(elem[0] + ' DATE'))
            elif str(elem[1]) == 'float64' or str(elem[1]) == 'int64': 
                lis.append(str(elem[0] + ' NUMBER'+ '(' + number_size + ')'))
            elif str(elem[1]) == 'object': 
                lis.append(str(elem[0] + ' VARCHAR2'+ '(' + varchar2_size + ')'))
        cols_dtypes = ','.join(lis)
        sql_create_tbl = sql_create_tbl + cols_dtypes + ')'
        print('-'*100)
        print('CREATE command: %s' % sql_create_tbl)
        print('-'*100)
        df = df.where((pd.notnull(df)), None)
        cur.execute(sql_create_tbl)
#         for error in cur.getbatcherrors():
#             print("Error", error.message, "at row offset", error.offset)
        # Insert data from excel files end with xlsx to the staged table
        col_name = list(df.columns)
        sql_col_name = ','.join(col_name)
        sql_insert = 'INSERT INTO ' + table + ' (' + sql_col_name + ') VALUES ('
        sql_values = []
        for i in range(1, len(col_name) + 1):
            sql_values.append(':' + str(i))
        sql_values = ','.join(sql_values) + ')'
        sql_insert += sql_values
        print('INSERT command: %s' % sql_insert)
        print('-'*100)
        rows = [tuple(x) for x in df.values]
        cur.executemany(sql_insert, rows, batcherrors=True, arraydmlrowcounts=True)
        #Return rows affected: Oracle Client library needs to be version 12.1 or higher to run the the code below in comment
        #rowCounts = cur.getarraydmlrowcounts()
        #for count in rowCounts:
            #print("Inserted", count, "rows.")
        for error in cur.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
            # Insert Data into the relational tables. The functionality is not perfect for the moment, some edge cases will fail. While it won't affect the staged table building above.
        try:
            lis = []
            for row in cur.execute("SELECT column_name FROM USER_TAB_COLUMNS WHERE table_name = 'HR_DATA'"):
                lis.append(row[0])
            cols = ','.join(lis)
            print(cols)
            print('-'*100)
            fail = 'table not existed in dict, no target tables to insert'
            targets = insert_dic.get(table, fail)
            print(targets)
            print('-'*100)
            if targets == fail:
                con.commit()
                continue
            else:
                dic = {}
                for tbl in targets:
                    sql = "SELECT column_name FROM USER_TAB_COLUMNS WHERE table_name = " + "'" + tbl + "'"
                    lis = []
                    cur.execute(sql)
                    rows = cur.fetchall()
                    for i in rows:
                        lis.append(','.join(i))
                    dic[tbl] = lis
                print(dic)
                print('-'*100)
                for key in dic:
                    print(key)
                    print('-'*100)
                    lis = []
                    table_4pk = "'" + key + "'"
                    query_pk = "SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = " + table_4pk + " AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position"
                    for row in cur.execute(query_pk):
                        lis.append(','.join(row))
                    where = ' AND '.join([i + ' IS NOT NULL' for i in lis])
                    columns = ','.join(lis)
                    sql_select_stg = "SELECT DISTINCT " + ','.join(dic[key])+ " FROM " + table + " WHERE " + where
                    sql_insert_tbl = "INSERT INTO " + key + '(' + ','.join(dic[key]) + ')' + ' ' + sql_select_stg
                    print(sql_insert_tbl)
                    cur.execute(sql_insert_tbl)
        except Exception as e:
            print(type(e),e)
        con.commit()
        continue
    else:
        continue
cur.close()
con.close()




