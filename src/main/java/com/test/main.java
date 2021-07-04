package com.test;
import java.util.List;


public class main {
    public static void main(String[] args) {
        //Get Data
        JobDaoImp _JobDaoImp = new JobDaoImp();
       // List<Job> jobLst = _JobDaoImp.ReadFromCSV("Wuzzuf_Jobs.csv");
        //System.out.println("************ This CSV file contains top  = " + jobLst.size()+ " Jobs ***************");
        _JobDaoImp.readCsv("Wuzzuf_Jobs.csv");

    }
 
}
