
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JobDaoImp {
    List<Job> Jobs = new ArrayList <Job>();

    //Constructor
    public JobDaoImp( String CSVFileName) {
        //Read Data from CSV file
       Jobs =  ReadFromCSV(CSVFileName);
    }
       
    public List<Job> GetAllJobs() {
        return  Jobs;
    }
    
    private List<Job> ReadFromCSV(String CSVFileName)
    {
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
   
}
