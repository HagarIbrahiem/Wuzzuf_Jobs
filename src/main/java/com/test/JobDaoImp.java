package com.test;
//import com.sun.rowset.internal.Row;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;
import smile.data.DataFrame;
//import smile.data.Dataset;
import smile.data.measure.NominalScale;
//import smile.data.type.StructType;
import smile.data.vector.IntVector;
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
    public DataFrame readCSV_OmittingNulls(String path) {
		CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader ().withDelimiter(',');
		DataFrame df = null;
		try {
			df =  Read.csv(path,format);
			
			System.out.println("before omiting nulls"+df.size());
			df = df.omitNullRows();
			
			df = df.factorize(new String[]{});
			System.out.println("after omitting nulls"+df.size());
			System.out.println("after omitting nulls"+df.getDouble(5, 5));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return df;
		}
    public Dataset<Row> readCsv_OmittingDuplicates(String filename){
	   
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
	  
	   System.out.println("before omitting duplicates"+df.count());
	   df  = df.distinct();
	   System.out.println("after omitting duplicates"+df.count());
	   return df;
    //      return null;
   }
    
    
    public DataFrame Getsummary (String path){
        //Display structure and summary of the data using Smile Dependency
         DataFrame df= null;
        try
        {
            CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
            df= Read.csv (path, format);
            System.out.println("======= Structure Before Encoding ============== \n  "  + df.structure());

            df= df.merge(IntVector.of("New-Title", encodeCategory(df, "Title")));
            df= df.merge(IntVector.of("New-Company", encodeCategory(df, "Title")));
            df= df.merge(IntVector.of("New-Location", encodeCategory(df, "Title")));
            df= df.merge(IntVector.of("New-Type", encodeCategory(df, "Title")));
            df= df.merge(IntVector.of("New-Level", encodeCategory(df, "Title")));
            df= df.merge(IntVector.of("New-YearsExp", encodeCategory(df, "Title")));
            df= df.merge(IntVector.of("New-Country", encodeCategory(df, "Title")));
            df= df.merge(IntVector.of("New-Skills", encodeCategory(df, "Title")));

            System.out.println ("======= Encoding Non Numeric Data ==============");
            System.out.println("======= Structure After Encoding ============== \n  "  + df.structure());
            System.out.println("======= Summery ==============  \n "  +df.summary());

        }
        catch (IOException ex) {
            System.err.println("Error" + ex.getMessage());
        }
        catch (URISyntaxException ex) {
	    // TODO Auto-generated catch block
	    System.err.println("Error" + ex.getMessage());
	}
        return df;
    }  
    private static int[] encodeCategory(DataFrame df, String columnName) {
        //Encoding columns to numerical values
        //The method gets an array of String with the column values and creates a corresponding array of integer of the nominal scale for the string values.
        String[] values = df.stringVector(columnName).distinct ().toArray(new String[]{});
        int[] pclassValues= df.stringVector(columnName).factorize (new NominalScale(values)).toIntArray();
        return pclassValues;
    }
   
    public  void GetPopularJobTitle() throws IOException {
        Logger.getLogger ("org").setLevel (Level.ERROR);
        // CREATE SPARK CONTEXT
        SparkConf conf = new SparkConf ().setAppName ("TitleCounts").setMaster ("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);
        // LOAD DATASETS
        JavaRDD<String> Jobs = sparkContext.textFile ("Wuzzuf_Jobs.csv");
        // TRANSFORMATIONS
        JavaRDD<String> titles = Jobs
                .map (JobDaoImp::extractJobTitle)
                .filter (StringUtils::isNotBlank);
       
        System.out.println("======= Titles ============== \n  " );
        // COUNTING
        Map<String, Long> TitleCounts = titles.countByValue ();
        List<Map.Entry> sorted = TitleCounts.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
        
        //Check if Numer is not repeated , while loop
        System.out.println("======= The Most Popular Title is ( "+ sorted.get(sorted.size()-1).getKey()+" ) With Repeated times of ( "+  sorted.get(sorted.size()-1).getValue()+" ) ============== \n " );
        
        DisplayJobTitles(TitleCounts);
    }
    private static String extractJobTitle(String TitleLine) {
        try {
            String COMMA_DELIMITER = ",";
            return TitleLine.split (COMMA_DELIMITER)[0];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
        
    }

    public  void GetPopularArea() throws IOException {
        Logger.getLogger ("org").setLevel (Level.ERROR);
        // CREATE SPARK CONTEXT
        SparkConf conf = new SparkConf ().setAppName ("AreaCounts").setMaster ("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);
        // LOAD DATASETS
        JavaRDD<String> Jobs = sparkContext.textFile ("Wuzzuf_Jobs.csv");
        // TRANSFORMATIONS
        JavaRDD<String> Areas = Jobs
                .map (JobDaoImp::extractJobArea)
                .filter (StringUtils::isNotBlank);
       
        System.out.println("======= Areas ============== \n  " );
        // COUNTING
        Map<String, Long> TitleCounts = Areas.countByValue ();
        List<Map.Entry> sorted = TitleCounts.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
        
        //Check if Numer is not repeated , while loop
         System.out.println("======= The Most Popular Area is ( "+ sorted.get(sorted.size()-1).getKey()+" ) With Repeated times of ( "+  sorted.get(sorted.size()-1).getValue()+" ) ============== \n " );
        
        DisplayJobAreas(sorted);
    }
    private static String extractJobArea(String AreaLine) {
        try {
            String COMMA_DELIMITER = ",";
            return AreaLine.split (COMMA_DELIMITER)[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    
    public static void DisplayJobTitles (Map<String, Long> lst){
        System.out.println("///////////");
        List<String> Titles = new ArrayList<>();
        List<Long> Counts = new ArrayList<>();
        int i = 0;
        for (String entry : lst.keySet()) {
            if(i>5)
                break;
            Titles.add(entry);
            Counts.add(lst.get(entry));
            i++;
        }



        CategoryChart chart = new CategoryChartBuilder().width(500).height(500).title("Passengers Names & Ages").
                              xAxisTitle("Title").yAxisTitle("Counts").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);

        chart.addSeries("Series Name : Jobs titles & counts", Titles, Counts);

        new SwingWrapper(chart).displayChart();
    }
    
    public static void DisplayJobAreas (List<Map.Entry> lst){
        List<String> Titles = new ArrayList<>();
        List<Integer> Counts = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : lst) {
            Titles.add(entry.getKey());
            Counts.add(entry.getValue());
        }
        CategoryChart chart = new CategoryChartBuilder().width(500).height(500).title("Passengers Names & Ages").
                              xAxisTitle("Area").yAxisTitle("Counts").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);

        chart.addSeries("Series Name : Jobs Areas & counts", Titles, Counts);

        new SwingWrapper(chart).displayChart();
    }
}
