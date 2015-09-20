import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GetPersonalRecom {
//getting a person's predicted ratings of the movies he didn't watch
	public static class TextDoubleIntWritable implements Writable{

		private Text name;
		private DoubleWritable sim;
		private IntWritable rating;
		
		
		public TextDoubleIntWritable(){
			set(new Text(), new DoubleWritable(), new IntWritable());
		}
		
		public TextDoubleIntWritable(String name, double sim, int rating){
			set(new Text(name), new DoubleWritable(sim), new IntWritable(rating));
		}
		
		public Text getName(){
			return name;
		}
		
		public DoubleWritable getSim(){
			return sim;
		}
		
		public IntWritable getRating(){
			return rating;
		}
		
		public void set(Text name, DoubleWritable sim, IntWritable rating){
			this.name = name;
			this.sim = sim;
			this.rating = rating;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			name.readFields(in);
			sim.readFields(in);
			rating.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			name.write(out);
			sim.write(out);
			rating.write(out);
		}
		
	    @Override
	    public String toString() {
	        return name.toString() + " " + sim.toString() + " " + rating.toString();
	    }
	 
	 
	  @Override
		public int hashCode(){
	        return name.hashCode()*17 + rating.hashCode()*31 + sim.hashCode()*13;
	    }
	 
	    @Override
	    public boolean equals(Object o)
	    {
	        if(o instanceof TextDoubleIntWritable)
	        {
	            TextDoubleIntWritable t = (TextDoubleIntWritable) o;
	            return name.toString().equals(t.name.toString()) && sim.equals(t.sim) && rating.equals(t.rating);
	            //object fields should equal
	        }
	        return false;
	    }	
	  }
	
	public static class MapThree
    extends Mapper<Object, Text, Text, TextDoubleIntWritable>{
		
	private Map<String, Integer> cacheMap = new HashMap<String, Integer>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.getLocal(conf);
	    Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
	    
		//Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(cacheFiles[0])));
		
		String line=null;
		while((line=reader.readLine())!=null){
			String[] str = line.split("::");
			cacheMap.put(str[1], Integer.parseInt(str[2]));
			//put in movie, rating in the map
		}
		reader.close();
		
	}
		
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
		 // item1 item2 sim
	   String[] record = value.toString().split(",");
	   String item1 = record[0].trim();
	   String item2 = record[1].trim();
	   String sim = record[2].trim();
	   // if item1 is not in user's pref but item2 is
	   if(!cacheMap.containsKey(item1)){
		   if(cacheMap.containsKey(item2)){
			   context.write(new Text(item1), 
					   new TextDoubleIntWritable(item2, 
							   Double.parseDouble(sim), 
							           cacheMap.get(item2).intValue()));
		   }
	   }//else 
	   if(!cacheMap.containsKey(item2)){
		   if(cacheMap.containsKey(item1)){
			   context.write(new Text(item2), 
					   new TextDoubleIntWritable(item1, 
							   Double.parseDouble(sim), 
							           cacheMap.get(item1).intValue()));
		   }
	   }
	 }
	 
	}
	
	public static class ReduceThree
	    extends Reducer<Text,TextDoubleIntWritable,Text,DoubleWritable> { 
	 
	 public void reduce(Text key, Iterable<TextDoubleIntWritable> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
	 double result;
	 double sum_sim=0.0;
	 double sum_rxsim=0.0;
	 for(TextDoubleIntWritable t: values){
		 sum_sim += (double) t.getSim().get();
		 sum_rxsim += ((double) t.getRating().get())*t.getSim().get();
	 }
	 
	 if(sum_sim==0){
		 result=0.0;
	 }else{
		 result = sum_rxsim/sum_sim;
	 }
			   
	   context.write(key, new DoubleWritable(result));
	 }
	}
	
	public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
	 DistributedCache.addCacheFile(new URI("/user/localhost/test/sampleinput2/user628.txt"), conf);
	 Job job = Job.getInstance(conf, "third mapred");
	 job.setJarByClass(GetPersonalRecom.class);
	 job.setMapperClass(MapThree.class);
	 job.setReducerClass(ReduceThree.class);
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(TextDoubleIntWritable.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(DoubleWritable.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
