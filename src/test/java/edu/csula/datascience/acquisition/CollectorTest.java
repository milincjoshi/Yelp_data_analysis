package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class CollectorTest {
    private Collector<SimpleModel, MockData> collector;
    private Source<MockData> source;

    @Before
    public void setup() {
        collector = new MockCollector();
        source = new MockSource();
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

    
    @Test
    public void mungee() throws Exception {
        
    	String content1="", content2="";
		try {
			content1 = readTweet("C:\\Users\\vallabh\\git\\beautiful-data-project-datascience_2016\\src\\test\\java\\edu\\csula\\datascience\\acquisition\\tweet1.txt");
			content2 = readTweet("C:\\Users\\vallabh\\git\\beautiful-data-project-datascience_2016\\src\\test\\java\\edu\\csula\\datascience\\acquisition\\tweet2.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	List<SimpleModel> list = (List<SimpleModel>) collector.mungee(source.next());
        List<SimpleModel> expectedList = Lists.newArrayList(
            new SimpleModel("2", content1),
            new SimpleModel("3", content2)
        );

        Assert.assertEquals(list.size(), 2);

        for (int i = 0; i < 2; i ++) {
            Assert.assertEquals(list.get(i).getId(), expectedList.get(i).getId());
            Assert.assertEquals(list.get(i).getContent(), expectedList.get(i).getContent());
        }
    }
}