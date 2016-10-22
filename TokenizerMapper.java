import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		/*problem1*/
		String s = value.toString();
		int count = s.split(",").length;
		if(count==6){
			context.write(new Text("CORRECT"), ONE);
		}else{
			context.write(new Text("MISSING"), ONE);
		}
	
		
	}
}
