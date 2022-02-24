A simple tool to create ddls for snowflake database objects. Supported db object types are table, view, function,
procedure, pipe, sequence.

Rename config.example.ini to config.ini and enter the relevant information.

It is possible to configure multiple servers adding new section blocks in the ini file.

The app will create a dedicated folder in the default system temp folder location and then will organise the files as
shown below:

    [DEDICATED TEMP FOLDER]
        [INI SECTION NAME]
            [DATABASE NAME]
                [SCHEMA NAME]
                    [OBJECT TYPE]
                        object_name_dll.sql
                        ...
                    [OBJECT TYPE]
                        object_name_dll.sql
                        ...
                [SCHEMA NAME]
                    ...
            [DATABASE NAME]
                ...
        [INI SECTION NAME]
        ...

## INI CONFIGURATION

The value inside the `[]` is the ini section, it is possible to add multiple sections copying all the relevant keys

| INI KEY           | TYPE   |                                                                                                                |
|-------------------|--------|----------------------------------------------------------------------------------------------------------------|
| DBACCOUNT         | string | The snowflake account, usually the part before `.snowflakecomputing.com`                                       |
| DBUSER            | string | The username you want to use to connect                                                                        |
| DBPASSWORD        | string | The password associated to the username                                                                        |
| DBUSERROLE        | string | The role associated to the user to use during the operations                                                   |
| DBWAREHOUSE       | string | The default warehouse to use                                                                                   |
| DBNAME            | string | The default database (this is only used for connection purpose, it won't affect the output of the application) |
| DBSCHEMA          | string | The default schema (this is only used for connection purpose, it won't affect the output of the application)   |
| DDLSEPARATEFOLDER | bool   | If set to `1` it will create use an additional folder between the INI SECTION FOLDER and the DATABASE folder   |

## QUICK RUN

Once the config file is created and configured you can try to run the app using the command  

    go run .

## PLEASE NOTE
Only the database objects visible to `DBUSER`/`DBUSERROLE` will be processed.  
it is possible that objects not visible will be skipped or objects with wrong permission will cause error.  
It is suggested to create a dedicated user with proper permissions configured and use it.