package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by Prateek Sheel on 3/24/17.
 */
public class HBaseExample {

    /**
     * Name of the file to be read which contains input data
     */
    private static final String fileName = "data.txt";

    /**
     * Table name in which to insert the records
     */
    private static final String tableName = "EMP";

    /**
     * Column family name for employee details
     */
    private static final String empDetailsColFamily = "EMP_DETAILS";

    /**
     * Column family  name for personal details
     */
    private static final String personalDetailsColFamily = "PERSONAL_DETAILS";

    /**
     * Column name for salary column in employee details column family
     */
    private static final String salaryColumn = "SALARY";

    /**
     * Column name for Department Number column
     */
    private static final String deptNumColumn = "DEPT_NO";

    /**
     * Main method for the HBaseExample class
     * @param args
     */
    public static void main(String [] args){
        HBaseExample example = new HBaseExample();
        System.out.println("Adding employees to HBase table:");
        example.addEmployees();
        System.out.println();

        System.out.println("Updating salary of employee with rowKey:1 to 95000");
        example.updateSalary("1", "95000");
        System.out.println();

        System.out.println("Deleting employee with rowKey:8");
        example.deleteEmployee("8");
        System.out.println();

        System.out.println("Display all employees using GET function:");
        example.displayUsingGET();

        System.out.println("Display all employees using SCAN function:");
        example.displayUsingScan();
        System.out.println();

        System.out.println("Get sum of salaries in all departments:");
        example.getSumOfSalaryByDeptId();
    }

    /**
     * Add employees to a HBase table
     * Reads data from "data.txt" and adds records to a HBase
     * table named "EMP". For details about the data in the
     * input file please refer to "data.txt"
     */
    private void addEmployees(){
        HTable table = null;
        try{
            //Instantiate File and Scanner
            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(classLoader.getResource(fileName).getFile());
            Scanner inputScanner = new Scanner(file);

            //Create HBase Configuration Object and HTable Object
            Configuration conf = HBaseConfiguration.create();
            table = new HTable(conf, tableName);
            List<Put> puts = new ArrayList<>();

            while(inputScanner.hasNext()){
                //Read one line at a time from the file
                String line = inputScanner.nextLine();

                //Split the words in the line by comma
                StringTokenizer tokenizer = new StringTokenizer(line, ",");

                //If the line has less than 4 comma separated values, its not valid
                if(tokenizer.countTokens() < 4){
                    System.out.println("Invalid input: " + line);
                } else {
                    // First token in the input file is the rowId
                    Put put = new Put(Bytes.toBytes(tokenizer.nextToken()));
                    // Next three tokens are Column Family, Column Name, and Value respectively
                    put.add(Bytes.toBytes(tokenizer.nextToken()),
                            Bytes.toBytes(tokenizer.nextToken()),
                            Bytes.toBytes(tokenizer.nextToken()));
                    puts.add(put);
                }
            } // End of While Loop

            // Insert all records in the table
            table.put(puts);
            System.out.println("Employees have been added to the HBase table EMP");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if(null != table){
                try{
                    table.close();
                } catch (IOException ioEx){
                    ioEx.printStackTrace();
                }
            }
        }
    }

    /**
     * Update the salary of employee identified by rowKey
     * @param rowKey
     * @param salary
     */
    private void updateSalary(String rowKey, String salary){
        HTable table = null;
        try{
            //Create HBase Configuration Object and HTable Object
            Configuration conf = HBaseConfiguration.create();
            table = new HTable(conf, tableName);
            List<Put> puts = new ArrayList<>();

            Put put = new Put(Bytes.toBytes(rowKey));
            // Next three tokens are Column Family, Column Name, and Value respectively
            put.add(Bytes.toBytes(empDetailsColFamily),
                    Bytes.toBytes(salaryColumn),
                    Bytes.toBytes(salary));
            puts.add(put);
            table.put(puts);

            System.out.println("Salary updated for rowKey = " + rowKey + " to new value: " + salary);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if(null != table){
                try{
                    table.close();
                } catch (IOException ioEx){
                    ioEx.printStackTrace();
                }
            }
        }
    }

    /**
     * Delete the employee identified by rowKey from HBase table
     * @param rowKey
     */
    private void deleteEmployee(String rowKey){
        HTable table = null;
        try{
            //Create HBase Configuration Object and HTable Object
            Configuration conf = HBaseConfiguration.create();
            table = new HTable(conf, tableName);

            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);

            System.out.println("Employee identified by rowKey = " + rowKey + " has been deleted.");

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if(null != table){
                try{
                    table.close();
                } catch (IOException ioEx){
                    ioEx.printStackTrace();
                }
            }
        }
    }

    /**
     * Display all records in the table using GET operation
     */
    private void displayUsingGET(){
        HTable table = null;
        try{
            //Create HBase Configuration Object and HTable Object
            Configuration conf = HBaseConfiguration.create();
            table = new HTable(conf, tableName);

            // Start getting records from table from rowKey 1
            int rowKey = 1;
            boolean moreRecords = true;

            Get get = new Get(Bytes.toBytes(Integer.toString(rowKey)));
            Result result = table.get(get);

            if(result.isEmpty()){
                moreRecords = false;
            }

            while(moreRecords){
                // Create a StringBuffer to store the output
                StringBuffer outputBuffer = new StringBuffer();

                // Get Map returns the following
                // NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
                // The keys in the top level map are the column families
                // The keys in the intermediate map are the columns
                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = result.getMap();
                Iterator<byte[]> columnFamilyIterator = resultMap.keySet().iterator();

                outputBuffer.append("Employee: " + rowKey + "\n\t");

                while(columnFamilyIterator.hasNext()){
                    // Iterate through all the column families
                    byte[] columnFamily = columnFamilyIterator.next();
                    outputBuffer.append(Bytes.toString(columnFamily));

                    NavigableMap<byte[], NavigableMap<Long, byte[]>> innerMap = resultMap.get(columnFamily);
                    Iterator<byte[]> innerIterator = innerMap.keySet().iterator();

                    while(innerIterator.hasNext()){
                        // Iterate through all the columns
                        byte[] column = innerIterator.next();
                        outputBuffer.append("(");
                        outputBuffer.append(Bytes.toString(column));
                        outputBuffer.append("= ");

                        NavigableMap<Long, byte[]> valueMap = innerMap.get(column);
                        Iterator<Long> valueIterator = valueMap.keySet().iterator();

                        while(valueIterator.hasNext()){
                            // Iterate through all the values
                            Long timestamp = valueIterator.next();
                            String value = Bytes.toString(valueMap.get(timestamp));
                            outputBuffer.append(value);
                            outputBuffer.append(" ");
                            outputBuffer.append("Timestamp: ");
                            outputBuffer.append(timestamp);
                        }
                        outputBuffer.append(")");
                    }

                    outputBuffer.append("\n\t");//End of column family
                }
                outputBuffer.append("\n");//End of row
                System.out.println(outputBuffer.toString());

                rowKey++;
                get = new Get(Bytes.toBytes(Integer.toString(rowKey)));
                result = table.get(get);

                if(result.isEmpty()){
                    moreRecords = false;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if(null != table){
                try{
                    table.close();
                } catch (IOException ioEx){
                    ioEx.printStackTrace();
                }
            }
        }
    }

    /**
     * Display all the records in a HBase table using Scan
     */
    private void displayUsingScan(){
        HTable table = null;
        ResultScanner scanner = null;
        try{
            //Create HBase Configuration Object and HTable Object
            Configuration conf = HBaseConfiguration.create();
            table = new HTable(conf, tableName);

            Scan scan = new Scan();
            scanner = table.getScanner(scan);

            for(Result res: scanner){
                System.out.println(res);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if(null != table){
                try{
                    table.close();
                } catch (IOException ioEx){
                    ioEx.printStackTrace();
                }
            }

            if(null != scanner){
                try{
                    scanner.close();
                } catch (Exception ex){
                    ex.printStackTrace();
                }
            }
        }
    }

    /**
     * Display Sum of Salaries for all Departments
     * in the HBase table
     */
    private void getSumOfSalaryByDeptId(){
        HTable table = null;
        StringBuffer outputBuffer = new StringBuffer();
        Map<String, List<String>> departmentSalary = new HashMap<>();

        try{
            //Create HBase Configuration Object and HTable Object
            Configuration conf = HBaseConfiguration.create();
            table = new HTable(conf, tableName);

            //Getting a scanner for department number column in employee details family
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(empDetailsColFamily), Bytes.toBytes(deptNumColumn));
            scan.addColumn(Bytes.toBytes(empDetailsColFamily), Bytes.toBytes(salaryColumn));
            scan.setMaxVersions();
            ResultScanner deptSalaryScanner = table.getScanner(scan);

            // Create a SET of all distinct department numbers
            for(Result res: deptSalaryScanner){
//                System.out.println(res);
                String department = Bytes.toString(res.getValue(Bytes.toBytes(empDetailsColFamily),
                        Bytes.toBytes(deptNumColumn)));
                String salary = Bytes.toString((res.getValue(Bytes.toBytes(empDetailsColFamily),
                        Bytes.toBytes(salaryColumn))));

                if(!departmentSalary.containsKey(department)) {
                    // Encountering this department for the first time
                    // Add it to the Map, and create a new salary list
                    List<String> salaryList = new ArrayList<>();
                    salaryList.add(salary);
                    departmentSalary.put(department,salaryList);
                } else {
                    // Department already exists in the Map
                    // Add the salary to the list
                    List<String> list = departmentSalary.get(department);
                    list.add(salary);
                }
            }

            // Create the format for the output
            outputBuffer.append("DEPARTMENT NUMBER\t");
            outputBuffer.append("TOTAL SALARY\n");

            // Now, we have a map which is of the form <Department Number, List of Salaries>
            Iterator<String> deptSalaryIterator = departmentSalary.keySet().iterator();
            while(deptSalaryIterator.hasNext()){
                String departmentNumber = deptSalaryIterator.next();
                outputBuffer.append(departmentNumber);
                outputBuffer.append("\t\t\t\t\t");

                List<String> salaryList = departmentSalary.get(departmentNumber);
                int totalSalary = 0;
                for(String salary: salaryList){
                    try{
                        int intSalary = Integer.parseInt(salary);
                        totalSalary += intSalary;
                    } catch (Exception ex){
                        System.out.println("Invalid value: " + salary);
                    }
                }
                outputBuffer.append(totalSalary);
                outputBuffer.append("\n");
            }

            System.out.println(outputBuffer.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if(null != table){
                try{
                    table.close();
                } catch (IOException ioEx){
                    ioEx.printStackTrace();
                }
            }
        }
    }
}
