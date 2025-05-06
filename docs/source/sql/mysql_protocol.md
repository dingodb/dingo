# MySQL Protocol

MySQL protocol is the protocol used for communication between MySQL database server and client. It is a binary protocol based on TCP/IP protocol, used for transferring data between client and server. The main purpose of MySQL protocol is to allow clients to connect to MySQL server and perform various operations.


1. Support data manipulation operations (CRUD).
2. View system variables:
   - View global variables. 
   - View all session variables. 
   - View some system variables. 
   - View some session variables. 
   - View specified system variables. 
   - View executor variables.
3. Modify system variables:
   - Modify global system variables. 
   - Modify session system variables. 
   - Modify executor variables. 
   - Set interactive_timeout variable. 
   - Set wait_timeout variable.
4. Support setting idle timeout for mysql-driver.
5. Support specifying database when connecting with JDBC.
6. Support server to obtain database status information.
7. Display table information:
    - View table structure. 
    - View specified table. 
    - View columns and data types in specified table. 
    - View Region information.
8. View creation statements:
    - View create table statements. 
    - View create database statements. 
    - View create user statements.
9. Support SQL prepared statements.
10. Support testing server availability and connection.
11. Support creating users with SSL.
12. Support processing alter user syntax.
