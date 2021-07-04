package com.test;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;


public class main {
    public static void main(String[] args) throws IOException {
        //Get Data
        JobDaoImp _JobDaoImp = new JobDaoImp();

        //List<Job> jobLst = _JobDaoImp.ReadFromCSV("Wuzzuf_Jobs.csv");
        //System.out.println("************ This CSV file contains top  = " + jobLst.size()+ " Jobs ***************");
        
        //Clean the data (null, duplications)
      // _JobDaoImp.readCsv_OmittingDuplicates("Wuzzuf_Jobs.csv");
        
        //Display structure and summary of the data using Smile Dependency
        //_JobDaoImp.Getsummary ("Wuzzuf_Jobs.csv");
        
        //Finding out the most popular job titles & Display bar chart
        //_JobDaoImp.GetPopularJobTitle();
        
        //Finding out the most popular job Areas & Display bar chart
        //_JobDaoImp.GetPopularArea();
       

    }
 
   
}
