# Auto_OracleDB_Data_Load_Insert__excel
A Python script that automates the process of creating staged table and loading data into it from excel files and inserting data into relational tables/schemas in Oracle DB using cx_Oracle.
There are two programs in total in one file, use them seperately in terms of your own cases.

For program 1, it iterates through all the excel files in xlsx format under the current working directory to create staged table and import the data to the corresponding staged table.

For program 2, it does the same thing as program 1 plus inserting the data in the staged to target relational tables. Warning: the functionalities of this is not very completed with several bugs and wrong logics for some cases, improve it or modify it for your own usage before using.

There are several notes before running the script for both program 1 and program 2:

  - Ensure your hostname matches the hostname in the /etc/hosts file. Otherwise type 'sudo hostname <hostname>' in terminal to match them temporarily if getting 'ORA-21561: OID generation failed' error.
  - Close the excel files that are going to be used. Otherwise you will see a XLRDError: "Unsupported format, or corrupt file: Expected BOF record; found b'\x08user168'"
  - The staged tables are going to be names as the upper case of excel files so change the excel name if there is any duplicates with the existing table otherwise the current ones will be dropped which might cause unwanted result. For the sake of safety, you will be asked about this before it covers the tables.
  - Uncomment the following code if you have the Oracle Client library of version 12.1 or higher. It will return you the rows affected of inserting:
     #rowCounts = cur.getarraydmlrowcounts()
     #for count in rowCounts:
         #print("Inserted", count, "rows.")
  - Set up the following variables to enable connection between Python and Oracle DB:
      username = ''
      password = ''
      server_ip = ''
      service_name = ''
      DB = ''
  - Set up the following variables before inserting data into relational tables:
      staged_tables = ['staged_table_name']
      target_tables = [['table1', 'table2']]
  
  
