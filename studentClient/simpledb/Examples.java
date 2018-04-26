import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.*;
import java.util.Arrays;

import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;

public class Examples {
    public static void main(String[] args) {
        //=====================================CS4432-Project1=====================

        //set up a new connection to the database
        Connection conn = null;
        try {

            //using the drivers for simpleDB create a connection
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            //create a cars table with car id, car model, car make, and car year
            String s = "create table CARS(CId int, CModel varchar(10), CMake varchar(10), CYear int)";
            //execute the table create statement
            stmt.executeUpdate(s);
            System.out.println("Table CARS created.");

            //insert several rows into the cars table
            s = "insert into CARS(CId, CModel, CMake, CYear) values";
            String[] carVals = {"(1, '320i', 'BMW', 2014)",
                                "(2, 'MDX', 'Acura', 2014)",
                                "(3, 'Silverado', 'Chevrolet', 2014)",
                                "(4, 'C300', 'Mercedes-Benz', 2015)",
                                "(5, 'Corolla', 'Toyota', 2016)"};
            for (int i = 0; i < carVals.length; i++){
                stmt.executeUpdate(s + carVals[i]);
            }
            System.out.println("CARS records inserted");

            //check to see if the insertions did execute
            s = "select CModel from CARS";
            ResultSet rs = stmt.executeQuery(s);

            while(rs.next()){
                String model = rs.getString("CModel");
                System.out.println(model);
            }

            System.out.println("All models queried from CARS");

            //check to see if the insertions did execute and we can look up values based upon individual columns
            s = "select CMake from CARS where CYear=2014";
            rs = stmt.executeQuery(s);

            while(rs.next()){
                String make = rs.getString("CMake");
                System.out.println(make);
            }

            System.out.println("Makes from 2014 queried from CARS");

            //see if we can delete all rows from cars
            s = "delete from CARS";
            stmt.executeUpdate(s);
            System.out.println("All rows deleted from CARS");

            //create a books table
            s = "create table BOOKS(BId int, BName varchar(25), BAuthor varchar(25), BYear int)";
            stmt.executeUpdate(s);
            System.out.println("Table BOOKS created.");

            //insert 6 records into the books table
            s = "insert into BOOKS(BId, BName, BAuthor, BYear) values";
            String[] bookVals = {"(1, 'Brave New World', 'Aldous Huxley', 1932)",
                                 "(2, 'Where Angels Fear To Tread', 'E.M. Forster', 1905)",
                                 "(3, 'For Whom the Bell Tolls', 'Earnest Hemingway', 1940)",
                                 "(4, 'On the Road', 'Jack Kerouac', 1957)",
                                 "(5, 'Catch-22', 'Joseph Heller', 1961)",
                                 "(6, 'Island', 'Aldous Huxley', 1962)"};
            for (int i = 0; i < bookVals.length; i++){
                stmt.executeUpdate(s + bookVals[i]);
            }
            System.out.println("BOOKS records inserted.");

            //check if we can query from the books table
            s = "select BName from BOOKS where BYear=1962";
            rs = stmt.executeQuery(s);

            while(rs.next()){
                String title = rs.getString("BName");
                System.out.println(title);
            }

            System.out.println("All titles queried from the year 1962");

            //more queries
            s = "select BName, BYear from BOOKS where BAuthor='Aldous Huxley' and BName='Brave New World'";
            rs = stmt.executeQuery(s);

            while(rs.next()){
                int year = rs.getInt("BYear");
                String title = rs.getString("BName");
                System.out.println("{Name: " + title + ", Year: " + year + "}");
            }

            System.out.println("All titles and years queried from the select with BAuthor Aldous Huxley and BName Brave New World");

            //delete from books
            s = "delete from BOOKS";
            stmt.executeUpdate(s);
            System.out.println("All rows deleted from BOOKS.");

        } catch (SQLException e){
            e.printStackTrace();
        } finally {
            //close the connection to the database
            try {
                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e){
                e.printStackTrace();
            }
        }
    }
}
