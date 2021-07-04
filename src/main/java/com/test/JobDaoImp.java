package com.test;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import smile.data.DataFrame;
import smile.io.Read;

public class JobDaoImp {
   

    //Constructor
    public JobDaoImp( ) {
       
    }
       
    
    public List<Job> ReadFromCSV(String CSVFileName)
    {
    	List<Job> Jobs  = new ArrayList<Job>();
    	//Read Data from CSV file
       try {
            //read from File
            BufferedReader _BufferedReader=new BufferedReader(new FileReader(CSVFileName));
            String line = _BufferedReader.readLine();
            do {       
                line= _BufferedReader.readLine();
               if(line != null)
               {
                   String [] attributes = line.split(",",-1);
                        Job _job = new Job ();
                        _job.setTitle(attributes[0]);
                        _job.setCompany(attributes[1]);
                        _job.setLocation(attributes[2]);
                        _job.setType(attributes[3]);
                        _job.setLevel(attributes[4]);
                        _job.setYearsExp(attributes[5]);
                        _job.setCountry(attributes[6]);
                        _job.setSkills(attributes[7]);
                        Jobs.add(_job);
               }
           } while (line != null);
            _BufferedReader.close();
        } 
        catch (IOException ex) {
            System.err.println("Error" + ex.getMessage());
        }
       return  Jobs;
    }
    public DataFrame readCSV(String path) {
		CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader ().withDelimiter(',');
		DataFrame df = null;
		try {
			df =  Read.csv(path,format);
			
			System.out.println("before omiting nulls"+df.size());
			df = df.omitNullRows();
			
			df = df.factorize(new String[]{});
			System.out.println("after omitting nulss"+df.size());
			System.out.println("after omitting nulss"+df.getDouble(5, 5));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return df;
		}
   public Dataset<Row> readCsv(String filename){
	   
	   SparkSession spark = SparkSession
			    .builder()
			    .appName("Java Spark SQL Example")
			    .config("spark.master", "local")
			    .getOrCreate();
	   spark.sparkContext().setLogLevel("ERROR");
	   Logger.getLogger("org").setLevel(Level.OFF);
	   StructType schema = new StructType()
			    .add("Title", "string")
			    .add("Company", "string")
			    .add("Location", "string")
			    .add("Type", "string")	
			    .add("Level", "string")	
			    .add("YearsExp", "string")	
			    .add("Country", "string")
			    .add("Skills", "string");
	   Dataset<Row> df = spark.read()
		    .schema(schema)
		    .csv(filename);
	  
	   System.out.println("before omittttttttttttttttttttttttttttttttttttttttttttttttting duplicates"+df.count());
	   df  = df.distinct();
	   System.out.println("after omitting duplicates"+df.count());
	   return df;
   }
    
   
}
