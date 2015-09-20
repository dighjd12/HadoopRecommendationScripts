
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GroupByUser {

  public static class MapOne
       extends Mapper<Object, Text, Text, TextIntWritable>{

    private Text userID;
    private Text movieID;
    private IntWritable rating;
    private TextIntWritable val = new TextIntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] record = value.toString().split("::");
      
      userID = new Text(record[0]);
      movieID = new Text(record[1]);
      rating = new IntWritable(Integer.parseInt(record[2]));
      val.set(movieID, rating);
      
      context.write(userID, val);
    }
  }
  
  public static class TextIntWritable implements Writable{

	private Text name;
	private IntWritable rating;
	
	public TextIntWritable(){
		set(new Text(), new IntWritable());
	}
	
	public TextIntWritable(Text name, IntWritable rating){
		set(name, rating);
	}
	
	public Text getName(){
		return name;
	}
	
	public IntWritable getRating(){
		return rating;
	}
	
	public void set(Text name, IntWritable rating){
		this.name = name;
		this.rating = rating;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		name.readFields(in);
		rating.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		name.write(out);
		rating.write(out);
	}
	
    @Override
    public String toString() {
        return name.toString() + " " + rating.toString();
    }
 
 
  @Override
	public int hashCode(){
        return name.hashCode()*17 + rating.hashCode()*31;
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof TextIntWritable)
        {
            TextIntWritable t = (TextIntWritable) o;
            return name.toString().equals(t.name.toString()) && rating.equals(t.rating);
        }
        return false;
    }	
  }

  public static class ReduceOne
       extends Reducer<Text,TextIntWritable,Text,ArrayTextIntWritable> { //edit output
    
    private ArrayTextIntWritable result = new ArrayTextIntWritable();

    public void reduce(Text key, Iterable<TextIntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      ArrayList<TextIntWritable> tempList = new ArrayList<TextIntWritable>(); 
    	for (TextIntWritable val : values) {
    		tempList.add(new TextIntWritable(new Text(val.getName().toString()), new IntWritable(val.getRating().get())));

    	}
      result.set(tempList.toArray(new TextIntWritable[tempList.size()]));
      context.write(key, result);
    }
  }

  public static class ArrayTextIntWritable extends ArrayWritable{
		
	  
	    public ArrayTextIntWritable(){
			super(TextIntWritable.class);
		}
		
		public ArrayTextIntWritable(Writable[] values) {
			super(TextIntWritable.class, values);
	    }
		
		@Override
		public String toString() {
		  Writable[] array = super.get();
		  
		  String str = "";
		  for(int i=0; i<array.length; i++){
			  str =  str + "," + array[i].toString();
		  }
		  //str = str.substring(0, str.length()-1);
		  return str;
		  
		}
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "first mapred");
    job.setJarByClass(GroupByUser.class);
    job.setMapperClass(MapOne.class);
    job.setReducerClass(ReduceOne.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TextIntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ArrayTextIntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}