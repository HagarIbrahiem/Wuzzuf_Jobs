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
        _JobDaoImp.GetPopularJobTitle();
        
        //Finding out the most popular job Areas & Display bar chart
       // _JobDaoImp.GetPopularArea();
       
        List <TitanicPassenger> PasngrList = new ArrayList<TitanicPassenger>();
        PasngrList = getPassengersFromJsonFile();
        graphPassAges(PasngrList);

    }
 
       public static List <TitanicPassenger> getPassengersFromJsonFile() {
        List<TitanicPassenger> allPassengers = new ArrayList<TitanicPassenger> ();
        ObjectMapper objectMapper = new ObjectMapper ();
        objectMapper.configure (DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try (InputStream input = new FileInputStream ("titanic_csv.json")) {
            //Read JSON file
            allPassengers = objectMapper.readValue (input, new TypeReference<List<TitanicPassenger>> () {
            });
            System.out.println("Done .......");
        } catch (FileNotFoundException e) {
            e.printStackTrace ();
        } catch (IOException e) {
            e.printStackTrace ();
        } 
        return allPassengers;
    }
    
    public static void graphPassAges(List <TitanicPassenger> PasngrList){
    List<Float> pAgeslst = PasngrList.stream().map(TitanicPassenger :: getAge).limit(8).collect(Collectors.toList());
    List<String> pNameslst = PasngrList.stream().map(TitanicPassenger :: getName).limit(8).collect(Collectors.toList());
    
    CategoryChart chart = new CategoryChartBuilder().width(500).height(500).title("Passengers Names & Ages").
                          xAxisTitle("Name").yAxisTitle("Age").build();
    chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
    chart.getStyler().setHasAnnotations(true);
    chart.getStyler().setStacked(true);
    
    chart.addSeries("Series Name : Passengers Names & Ages", pNameslst, pAgeslst);
    
    new SwingWrapper(chart).displayChart();

    }

}
