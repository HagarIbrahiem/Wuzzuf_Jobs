package com.test;
import java.awt.Color;

//import com.sun.rowset.internal.Row;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.Styler.LegendPosition;

import smile.data.DataFrame;
//import smile.data.Dataset;
import smile.data.measure.NominalScale;
//import smile.data.type.StructType;
import smile.data.vector.IntVector;
import smile.io.Read;
import java.util.regex.Pattern;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
public class JobDaoImp {
   

    //Constructor
    public JobDaoImp( ) {
       
    }
    public Map<String,Long> jobsdemand( Dataset<Row> ds){
   	 JavaRDD<String> companys = ds.select("Company").toJavaRDD().map(m->m.getString(0))
                .filter (StringUtils::isNotBlank);
   	 
   	Map<String, Long> jobCount = companys.countByValue();
   	
   	List<Map.Entry> jobsdemands = jobCount.entrySet ().stream ()
   			.sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
   	for(Map.Entry<String, Long> company: jobsdemands) {
   		System.out.println(company.getKey()+": "+company.getValue());
   	}
   	
   	return jobCount;
   }
   public Map<String,Long> skillsdemand(Dataset<Row> ds){
   	JavaRDD<String> skills =   ds.select("Skills").toJavaRDD().map(m->m.getString(0))
               .filter (StringUtils::isNotBlank);
   			

   	JavaRDD<String> skillsset = skills.flatMap (title -> Arrays.asList (title
   			.toLowerCase ()
   			.trim ()
   			.split (",")).iterator());
   	Map<String, Long> skillscount = skillsset.countByValue();
   	
   	List<Map.Entry> skillsdemand = skillscount.entrySet ().stream ()
   			.sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
    	System.out.println("sizeeeeeeeeeeeeeeeeeeeeeeelllllllllll" +skills.count());
    	System.out.println("sizeeeeeeeeeeeeeeeeeeeeeee" +skillsset.count());
   	for(Map.Entry<String, Long> skill: skillsdemand ) {
   		System.out.println(skill.getKey()+": "+skill.getValue());
   	}
   	return skillscount;
   }
   public void DisplayskillsDemands(Map<String,Long> skillsdemands) {
   	List<Double> jobscount = new ArrayList<Double>();
   	List<String> skills = new ArrayList<String>();
    	
   	for(String skill: skillsdemands.keySet()) {
   		skills.add(skill);
   		jobscount.add(Double.valueOf(skillsdemands.get(skill)));
   	}
   	CategoryChart chart = new CategoryChartBuilder ().width (1024).height (768).title ("skills Histogram").xAxisTitle ("skills").yAxisTitle
   			("job count").build ();
   			// 2.Customize Chart
   			chart.getStyler ().setLegendPosition(Styler.LegendPosition.InsideNW);
   			chart.getStyler ().setHasAnnotations (true);
   			chart.getStyler ().setStacked (true);
   			// 3.Series
   			chart.addSeries ("skills's count",skills, jobscount);
   			// 4.Show it
   			new SwingWrapper (chart).displayChart ();
   }
   public  void displayJobsdemands(Map<String,Long> companysjobs) {
	    // Create Chart
	    PieChart chart = new PieChartBuilder ().width (800).height (600).title (getClass ().getSimpleName ()).build ();
	    // Customize Chart
	    Color[] sliceColors = new Color[companysjobs.size()];
	    
	   
	    // Series
	    int i = 0;
	    for(String company: companysjobs.keySet()) {
	    	chart.addSeries (company, companysjobs.get(company));
	    	sliceColors[i] = new Color(i%250,i*2%250,50);
	    	i++;		
	    	}
	    
	    chart.getStyler ().setSeriesColors (sliceColors);
	    // Show it
	    new SwingWrapper (chart).displayChart ();
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
	  
	   System.out.println("before omittttttttttttttttttttttttttttttttttttttttttttttttting duplicates"+df.count());
	   df  = df.distinct();
	   System.out.println("after omitting duplicates"+df.count());
	   return df;
   }
    
    
    public String[][] Getsummary (String path){
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
        return df.summary().toStrings(df.nrows());
    }  
    private static int[] encodeCategory(DataFrame df, String columnName) {
        //Encoding columns to numerical values
        //The method gets an array of String with the column values and creates a corresponding array of integer of the nominal scale for the string values.
        String[] values = df.stringVector(columnName).distinct ().toArray(new String[]{});
        int[] pclassValues= df.stringVector(columnName).factorize (new NominalScale(values)).toIntArray();
        return pclassValues;
    }
   
    public  Map<String, Long> GetPopularJobTitle(Dataset<Row> ds) {
        // TRANSFORMATIONS
        JavaRDD<String> titles = ds.select("Title").javaRDD().map(m->m.getString(0))
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
        
      //  DisplayJobTitles(TitleCounts);
        return TitleCounts;
    }
    private static String extractJobTitle(String TitleLine) {
        try {
            String COMMA_DELIMITER = ",";
            return TitleLine.split (COMMA_DELIMITER)[0];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
        
    }

    public   Map<String, Long> GetPopularArea(Dataset<Row> ds)  {
        // TRANSFORMATIONS
        JavaRDD<String> Areas = ds.select("Location").javaRDD().map(m->m.getString(0))
                .filter (StringUtils::isNotBlank);
       
        System.out.println("======= Areas ============== \n  " );
        // COUNTING
        Map<String, Long> AreaCounts = Areas.countByValue ();
        List<Map.Entry> sorted = AreaCounts.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
        
        //Check if Numer is not repeated , while loop
         System.out.println("======= The Most Popular Area is ( "+ sorted.get(sorted.size()-1).getKey()+" ) With Repeated times of ( "+  sorted.get(sorted.size()-1).getValue()+" ) ============== \n " );
        
      //  DisplayJobAreas(AreaCounts);
       return AreaCounts;
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
        List<String> Titles = new ArrayList<>();
        List<Long> Counts = new ArrayList<>();
        for (String entry : lst.keySet()) {
            Titles.add(entry);
            Counts.add(lst.get(entry));
        }

        CategoryChart chart = new CategoryChartBuilder().width(1000).height(1000).title("Passengers Names & Ages").
                              xAxisTitle("Title").yAxisTitle("Counts").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);

        chart.addSeries("Series Name : Jobs titles & counts", Titles, Counts);

        new SwingWrapper(chart).displayChart();
    }
    
    public static void DisplayJobAreas (Map<String, Long> lst){
        List<String> Titles = new ArrayList<>();
        List<Long> Counts = new ArrayList<>();

        for (String entry : lst.keySet()) {
            Titles.add(entry);
            Counts.add(lst.get(entry));
        }

        CategoryChart chart = new CategoryChartBuilder().width(1000).height(1000).title("Passengers Names & Ages").
                              xAxisTitle("Area").yAxisTitle("Counts").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);

        chart.addSeries("Series Name : Jobs Areas & counts", Titles, Counts);

        new SwingWrapper(chart).displayChart();
    }
    public void  Kmean( Dataset<Row> ds) {
    	StringIndexerModel labelIndexer = new StringIndexer()
    		       .setInputCol("Company")
    		       .setOutputCol("new-Company")
    			  // .setOutputCol("New-Title").setInputCol("Title")
    			   .fit(ds);
    	 ds =  labelIndexer.transform(ds);
    	 StringIndexerModel labelIndexer2 = new StringIndexer()
    			.setOutputCol("New-Title").setInputCol("Title")
  			   .fit(ds);
  	     ds =  labelIndexer2.transform(ds);
    	 //System.out.println("after factorizzzzzzzzzzzzzzzzzing"+ds.count());
    	// System.out.println("after factorizzzzzzzzzzzzzzzzzing");
    	//ds.foreach(m-> System.out.println("comapny"+ m.getDouble(8) + "title" + m.getDouble(9) ));
    	// System.out.println("after factorizzzzzzzzzzzzzzzzzing"+ds.describe("New-Title"));
    	JavaRDD<Vector> parsedData = ds.select("New-Title","new-Company").javaRDD().
    			map(m->Vectors.dense(new double[] { m.getDouble(0),m.getDouble(1)} ));
//    	    .map(new Function<String, Vector>() {
//				public Vector call(String v1) throws Exception {
//					String svalue[] = v1.split(",");
//		    		double[] values = new double[svalue.length];
//		    		for (int i = 0; i < values.length; i++) {
//		    			values[i] = Double.parseDouble(svalue[i]);
//		    		
//		    			}
//		    		return Vectors.dense(values);
//			   }
//    		});
//    		 for(Vector  v: parsedData.top(20)) {
//    			 System.out.println("max"+v.argmax()); 
//    		 }
    		
    		parsedData.cache();
    		int numClusters = 10;
    		int numIterations = 50;
    		KMeansModel clusters = KMeans.train (parsedData.rdd(), numClusters, numIterations);
    		System.out.println("Cluster centers:");
    		for (Vector center : clusters.clusterCenters()) {
    		System.out.println("" + center);
    		  
    		  
      }
    		   XYChart chart = new XYChartBuilder().width(800).height(600).build();
    		   
    		    // Customize Chart
    		    chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Scatter);
    		    chart.getStyler().setChartTitleVisible(false);
    		    chart.getStyler().setLegendPosition(LegendPosition.InsideSW);
    		    chart.getStyler().setMarkerSize(16);
 		  List<Double> titlles =  ds.select("New-Title").javaRDD().map(m->m.getDouble(0)).collect( );
 		  List<Double> comapnys =  ds.select("new-Company").javaRDD().map(m->m.getDouble(0)).collect();
 		  System.out.println("sizeeeeeeeeeeeeeeeeeeeeeeeeeee"+titlles.size()+"  "+comapnys.size());
 		  chart.addSeries("tilte comapny",titlles,comapnys);
 		  new SwingWrapper(chart).displayChart();
    }
}
