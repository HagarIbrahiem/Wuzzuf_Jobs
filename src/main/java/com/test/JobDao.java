package com.test;

import java.util.List;

public interface JobDao {
    List<Job> ReadFromCSV(String filename);
}
