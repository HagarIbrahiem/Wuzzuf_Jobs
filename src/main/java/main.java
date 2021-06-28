
import java.util.List;


public class main {
    public static void main(String[] args) {
        //Get Data
        JobDaoImp _JobDaoImp = new JobDaoImp("Wuzzuf_Jobs.csv");
        List<Job> jobLst = (List<Job>)_JobDaoImp.GetAllJobs();
        System.out.println("************ This CSV file contains top  = " + jobLst.size()+ " Jobs ***************");

    }
 
}
