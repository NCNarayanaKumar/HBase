# HBase
HBase examples

The objective of this repository is to demonstrate the basic operations available in the HBase client API for Java.

It is assumed that the code is running in an environment which has access to a HBase table named EMP with the following structure:
Table Name: EMP
Column Family: EMP_DETAILS (EMP_NO, DEPT_NO, SALARY)
Column Family: PERSONAL_DETAILS (FNAME, LNAME, DATE_OF_BIRTH)

The example includes to following operations:
1. Populate 8 employee records in the table
2. Update salary for one of the employees
3. Delete one of the employees from the table
4. Use the GET operation to display the contents of the table
5. Use the SCAN operation to display the contents of the table
6. Calculate the sum of salaries for all departments

The following maven dependency is required to run the code: org.apache.hbase:hbase-client:1.2.0

To provide feedback or for questions, please send an email to: prateeksheel86@gmail.com
