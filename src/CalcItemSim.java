import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CalcItemSim {
	
	public static class TwoTextKey implements WritableComparable<TwoTextKey>{
	
		private Text movie1;
		private Text movie2;
		
		public TwoTextKey(){
			set(new Text(), new Text());
		}
		
		public TwoTextKey(String m1, String m2){
			set(new Text(m1), new Text(m2));
		}
		
		public Text getMovie1(){
			return movie1;
		}
		
		public Text getMovie2(){
			return movie2;
		}
		
		public void set(Text movie1, Text movie2){
			this.movie1 = movie1;
			this.movie2 = movie2;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			movie1.readFields(in);
			movie2.readFields(in);
		}
	
		@Override
		public void write(DataOutput out) throws IOException {
			movie1.write(out);
			movie2.write(out);
		}
		
	 @Override
	 public String toString() {
	     return movie1.toString() + "," + movie2.toString() + ",";
	 }
	
	
	@Override
		public int hashCode(){
	     return movie1.toString().hashCode()*17 + movie2.toString().hashCode()*31;
	 }
	
	 @Override
	 public boolean equals(Object o)
	 {
	     if(o instanceof TwoTextKey)
	     {
	    	 TwoTextKey t = (TwoTextKey) o;
	         return movie1.toString().compareTo(t.movie1.toString())==0 && 
	        		 movie2.toString().compareTo(t.movie2.toString())==0;
	         //objects must have same fields
	     }
	     return false;
	 }

	 //if equals, then same hashcode
	 //compareTo==0 iff equals
	@Override
	public int compareTo(TwoTextKey o) {
		int cmp = movie1.toString().compareTo(o.movie1.toString());
		if (cmp != 0) {
			return cmp;
		}
		return movie2.toString().compareTo(o.movie2.toString());
	}	
}
	
	public static class TwoIntWritable implements Writable{
		private IntWritable first;
		private IntWritable second;
		
		public TwoIntWritable() {
			set(new IntWritable(), new IntWritable());
		}
		
		public TwoIntWritable(Integer first, Integer second) {
			set(new IntWritable(first), new IntWritable(second));
		}
		
		public void set(IntWritable first, IntWritable second) {
			this.first = first;
			this.second = second;
		}
		
		public IntWritable getFirst() {
			return first;
		}
		
		public IntWritable getSecond() {
			return second;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}
		
		@Override
		public String toString(){
			return first.toString() + " " + second.toString();
		}
		
		@Override
		public int hashCode() {
			return first.hashCode()*41 + second.hashCode() *31;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof TwoIntWritable) {
				TwoIntWritable tp = (TwoIntWritable) o;
				return first.equals(tp.first) && second.equals(tp.second);
			}
			return false;
		}
	}
	
	public static class ArrayListWritable implements Writable{

		private ArrayList<TwoIntWritable> data;
		
		public ArrayListWritable(){
			data = new ArrayList<TwoIntWritable>();
		}
		
		public void set(ArrayList<TwoIntWritable> data){
			this.data = new ArrayList<TwoIntWritable>();
			this.data.addAll(data);
		}
		
		public ArrayList<TwoIntWritable> get(){
			return data;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			 int size = in.readInt();
			 data = new ArrayList<TwoIntWritable>(size);
			 for(int i=0; i<size; i++){
			     TwoIntWritable t = new TwoIntWritable();
			     t.readFields(in);
			     data.add(t);
			 }
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(data.size());
		    for (TwoIntWritable w : data) {
		        w.write(out);
		    }
		}
	}
	
	public static class MapTwo
    extends Mapper<Object, Text, TwoTextKey, ArrayListWritable>{
	/*
	private Map<TwoTextKey, ArrayList<TwoIntWritable>> map = new HashMap<TwoTextKey, ArrayList<TwoIntWritable>>();
		*/
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
	
	   String[] record = value.toString().split(",");
	   for(int i=1; i<record.length; i++){
		   for(int j=1; j<record.length; j++){
			   if(i!=j){
				   String[] itemRatingPair1 = record[i].split(" ");
				   String[] itemRatingPair2 = record[j].split(" ");
				   TwoTextKey mapKey1 = new TwoTextKey(itemRatingPair1[0], itemRatingPair2[0]);
				   TwoTextKey mapKey2 = new TwoTextKey(itemRatingPair2[0], itemRatingPair1[0]);
				   
				   TwoIntWritable mapVal= new TwoIntWritable(Integer.parseInt(itemRatingPair1[1]), Integer.parseInt(itemRatingPair2[1]));
				   ArrayList<TwoIntWritable> newList = new ArrayList<TwoIntWritable>();
				   newList.add(mapVal);
				   ArrayListWritable mapVal2 = new ArrayListWritable();
				   mapVal2.set(newList);
				   TwoIntWritable mapValt = new TwoIntWritable(Integer.parseInt(itemRatingPair2[1]), Integer.parseInt(itemRatingPair1[1]));
				   ArrayList<TwoIntWritable> newListt = new ArrayList<TwoIntWritable>();
				   newListt.add(mapValt);
				   ArrayListWritable mapVal2t = new ArrayListWritable();
				   mapVal2t.set(newListt);
				   context.write(mapKey1, mapVal2);
				   context.write(mapKey2, mapVal2t);
				 
				   
			   }
		   }
	   }
	   
	   //in-map combiner
	   
	  /* for(int i=1; i<record.length-1; i++){
		   for(int j=i+1; j<record.length; j++){
			   if(i!=j){
				   String[] itemRatingPair1 = record[i].split(" ");
				   String[] itemRatingPair2 = record[j].split(" ");
				   TwoTextKey mapKey1 = new TwoTextKey(itemRatingPair1[0], itemRatingPair2[0]);
				   TwoTextKey mapKey2 = new TwoTextKey(itemRatingPair2[0], itemRatingPair1[0]);
				   
				   TwoIntWritable mapVal;
				   
				   //increment the hashmap, with n (item1,item2)==(item2,item1)
				   if(map.containsKey(mapKey1)){
					   mapVal = new TwoIntWritable(Integer.parseInt(itemRatingPair1[1]), Integer.parseInt(itemRatingPair2[1]));
					   map.get(mapKey1).add(mapVal);
				   }else if(map.containsKey(mapKey2)){
					   mapVal = new TwoIntWritable(Integer.parseInt(itemRatingPair2[1]), Integer.parseInt(itemRatingPair1[1]));
					   map.get(mapKey2).add(mapVal);
				   }
				   else{
					   mapVal = new TwoIntWritable(Integer.parseInt(itemRatingPair1[1]), Integer.parseInt(itemRatingPair2[1]));
					   ArrayList<TwoIntWritable> newList = new ArrayList<TwoIntWritable>();
					   newList.add(mapVal);
					   map.put(new TwoTextKey(itemRatingPair1[0], itemRatingPair2[0]), newList);
				   }
				   
			   }
		   }
	   }*/
	   
	 }
	/* 
	 @Override
	 protected void cleanup(Context context) throws InterruptedException, IOException{
		 for(TwoTextKey key : map.keySet()){
			 if(map.get(key).size()>1){
				 ArrayListWritable arr = new ArrayListWritable();
				 arr.set(map.get(key));
				 context.write(key, arr);
			 }
			 
		 }
	 }
	 */
	 
	}
	
	public static class ReduceTwo
	    extends Reducer<TwoTextKey,ArrayListWritable,TwoTextKey,DoubleWritable> { //edit output
	 
	 public void reduce(TwoTextKey key, Iterable<ArrayListWritable> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
		 
	   double sum1Sq=0.0;
	   double pSum=0.0;
	   double sum2Sq=0.0;
	   double sum1=0.0;
	   double sum2=0.0;
	   double n=0.0;
	   double rating1;
	   double rating2;
	   for(ArrayListWritable a: values){
		 
		  ArrayList<TwoIntWritable> arr = a.get();
		  
		  for(TwoIntWritable w: arr){
			  rating1 = (double) w.getFirst().get();
			  rating2 = (double) w.getSecond().get();
			  
			  sum1Sq += rating1*rating1;
			  pSum += rating1*rating2;
			  sum2Sq += rating2*rating2;
			  sum1 += rating1;
			  sum2 += rating2;
			  n += 1.0;
			  
		  }
	   }
	   double pearson = 0.0;
	   
	   double num= pSum- (sum1*sum2/n);
	   double den= Math.sqrt((sum1Sq-sum1*sum1/n)*(sum2Sq-sum2*sum2/n));
	   
	   if(den!=0.0){
		   pearson=num/den;
	   }
			   
	   context.write(key, new DoubleWritable(pearson));
	 }
	}
	
	public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
	 Job job = Job.getInstance(conf, "second mapred");
	 job.setJarByClass(CalcItemSim.class);
	 job.setMapperClass(MapTwo.class);
	 job.setReducerClass(ReduceTwo.class);
	 job.setMapOutputKeyClass(TwoTextKey.class);
	 job.setMapOutputValueClass(ArrayListWritable.class);
	 job.setOutputKeyClass(TwoTextKey.class);
	 job.setOutputValueClass(DoubleWritable.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
		
}
