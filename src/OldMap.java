import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counters;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;


public class OldMap {

	//For the page counter
	private static enum PageTypes
	{
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, OTHER;
	}
	
	//Controller
	public static void main(String[] args) throws IOException
	{
		
		//Get Job Configuration and Set job name
		JobConf wikiJob = new JobConf(OldMap.class);
		wikiJob.setJobName("wikiJobs");
		
		//Set output configuration
		wikiJob.setOutputKeyClass(Text.class);
		wikiJob.setOutputValueClass(Text.class);
		
		//Set mapper combiner and reducer
		wikiJob.setMapperClass(WikiPediaMapper.class);
		//wikiJob.setCombinerClass(WikiPediaCombiner.class);
		wikiJob.setReducerClass(WikiPediaReducer.class);
		
		//Set input output format
		wikiJob.setInputFormat(WikipediaPageInputFormat.class);
		wikiJob.setOutputFormat(TextOutputFormat.class);
		
		//Set input output path
		FileInputFormat.setInputPaths(wikiJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wikiJob, new Path(args[1]));
		//Start Controller
		JobClient.runJob(wikiJob);
	}
	
	//Mapper Class
	private static class WikiPediaMapper extends MapReduceBase implements Mapper<LongWritable, WikipediaPage, Text, Text>
	{
	    private Text word = new Text();
	    private Text detail=new Text();
	    private String d="";
		@Override
		public void map(LongWritable key, WikipediaPage w, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
		{
			//Construct counter Object
			Counters counter = new Counters();
			
			//Add the total page count incr 1
			counter.findCounter(PageTypes.TOTAL).increment(1);
			
			if(w.isRedirect()) //If page has been redirected
				counter.findCounter(PageTypes.REDIRECT).increment(1);
			else if(w.isDisambiguation()) //If page is disambiguation
				counter.findCounter(PageTypes.DISAMBIGUATION).increment(1);
			else if(w.isEmpty()) //If page is empty
				counter.findCounter(PageTypes.EMPTY).increment(1);
			else if(w.isArticle()) //If page is an article
			{
				counter.findCounter(PageTypes.ARTICLE).increment(1); //Increase the Article counter
				if(w.isStub())
					counter.findCounter(PageTypes.STUB).increment(1); //If page is too short, then is a stub, increment the counter by 1
				
				String line = w.getContent().replaceAll("[^0-9A-Za-z]", " ").toLowerCase();
	        	String[] lineResults = line.split("\\s+");
	        	int lineNum=0;
	        	for(String s : lineResults)
	        	{
	        		word.set(s);
          		    d=w.getDocid()+", "+lineNum;
					detail.set(d);
					lineNum++;
					output.collect(word,detail);
	        	}
			}
			else
				counter.findCounter(PageTypes.OTHER).increment(1); //Then the other counter is increased
		}
		
		
	}
	
	//Use this combiner when applicable
	@SuppressWarnings("unused")
	private static class WikiPediaCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
		{
			//Empty
		}
	}
	
	//Reducer class
	private static class WikiPediaReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
		{
			String outputValues = ""; //The output String
			int sum=0;
			Hashtable<String, ArrayList<String>> DocPositions=new Hashtable<String,ArrayList<String>>();
			while(values.hasNext())
			{
				String detail=values.next().toString();
				StringTokenizer tokenizer=new StringTokenizer(detail," ,");
				String Docid=tokenizer.nextToken();
				String position=tokenizer.nextToken();
				if(DocPositions.containsKey(Docid)){
					DocPositions.get(Docid).add(position);
				}
				else{
					ArrayList<String> newPositions=new ArrayList<String>();
					newPositions.add(position);
					DocPositions.put(Docid, newPositions);
				}
				//outputValues += "("+values.next().toString()+") "; //Just connect the String
			}

			Enumeration<String> allkeys=DocPositions.keys();
			
			while(allkeys.hasMoreElements()){
				String docid=allkeys.nextElement();
				ArrayList<String> positionvalues=DocPositions.get(docid);
				Object[] arr=positionvalues.toArray();
				Arrays.sort(arr);
				List<Object> result=Arrays.asList(arr);
				String p=result.get(0).toString();
				sum++;
				for(int i = 1;i<result.size();i++){
					p+=" "+result.get(i);
					sum++;
				}
				outputValues+="("+docid+", "+p+") ";			

			}	
			if(sum<=200000) output.collect(key, this.textWrapper(outputValues));
		}
		
		private Text textWrapper(String s)
		{
			return new Text(s);
		}
	}
}
