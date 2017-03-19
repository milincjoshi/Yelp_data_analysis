package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * A mock source to provide data
 */
public class MockSource implements Source<MockData> {
    int index = 0;

    @Override
    public boolean hasNext() {
        return index < 1;
    }
    
    public String readTweet(String filename) throws IOException{
    	String content = "";
    	BufferedReader br = new BufferedReader(new FileReader(filename));
    	try {
    	    StringBuilder sb = new StringBuilder();
    	    String line = br.readLine();

    	    while (line != null) {
    	        sb.append(line);
    	        sb.append(System.lineSeparator());
    	        line = br.readLine();
    	    }
    	    content = sb.toString();
    	} finally {
    	    br.close();
    	}
    	return content;
    }


    @Override
    public Collection<MockData> next() {
        
    	String content1="", content2="";
		try {
			content1 = readTweet("C:\\Users\\vallabh\\git\\beautiful-data-project-datascience_2016\\src\\test\\java\\edu\\csula\\datascience\\acquisition\\tweet1.txt");
			content2 = readTweet("C:\\Users\\vallabh\\git\\beautiful-data-project-datascience_2016\\src\\test\\java\\edu\\csula\\datascience\\acquisition\\tweet2.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	    	
        return Lists.newArrayList(
            new MockData("1", null),
            new MockData("2", content1),
            new MockData("3", content2)
        );
    	
    }
}
