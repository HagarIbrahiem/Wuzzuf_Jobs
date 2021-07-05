package com.test;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
@XmlRootElement(name="jobService")
@Path("/jobService")
@Singleton
public  class jobService {

	 
	 private JobDaoImp daoImp ;
	 private Dataset<Row> ds;
	 public jobService() {
		daoImp = new JobDaoImp();
		ds = daoImp.readCsv_OmittingDuplicates("Wuzzuf_Jobs.csv");
	}
	
	@GET
	@Produces("application/json")
	public List<Job> getJobs() {
		System.out.println("reading file");
		return daoImp.ReadFromCSV("Wuzzuf_Jobs.csv");
	}
	@GET
	@Path("/jobdemand")
	@Produces("application/json")
	public Map<String,Long> getJobdemands() {
		
		
		return daoImp.jobsdemand(ds);
	}
	@GET
	@Path("/skilldemand")
	@Produces("application/json")
	public Map<String,Long> getskillsdemands() {
		
		return daoImp.skillsdemand(ds);
	}
	@GET
	@Path("/jobsareas")
	@Produces("application/json")
	public Map<String,Long> getTitles() {
		return daoImp.GetPopularArea(ds);
	}
	@GET
	@Path("/jobstitles")
	@Produces("application/json")
	public Map<String,Long> getjobtiltles() {
		return daoImp.GetPopularJobTitle(ds);
	}
	@GET
	@Path("/summary")
	@Produces("application/json")
	public String[][] summary() {
		return daoImp.Getsummary("Wuzzuf_Jobs.csv");
	}
	
	
}
