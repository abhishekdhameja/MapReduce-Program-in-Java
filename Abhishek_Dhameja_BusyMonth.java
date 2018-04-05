import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BusyMonth {

	public static class BusyMonthMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text monthyear = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] record = value.toString().split(",");
			String mmyyyy = record[1].split(" ")[0].split("/")[0] + "/" + record[1].split(" ")[0].split("/")[2];
			IntWritable count = new IntWritable(Integer.parseInt(record[5]));
			if (record[2].equals("Terminal 1") || record[2].equals("Terminal 2") || record[2].equals("Terminal 3")
					|| record[2].equals("Terminal 4") || record[2].equals("Terminal 5") || record[2].equals("Terminal 6")
					|| record[2].equals("Terminal 7") || record[2].equals("Terminal 8")
					|| record[2].equals("Tom Bradley International Terminal")) {
				monthyear.set(mmyyyy);
				context.write(monthyear, count);
			}
		}
	}

	public static class BusyMonthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			if (count > 5000000) {
				result.set(count);
				context.write(key, result);
				System.out.println(key.toString() + " " + result.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: BusyMonth <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "busy month");
		job.setJarByClass(BusyMonth.class);
		job.setMapperClass(BusyMonthMapper.class);
		job.setCombinerClass(BusyMonthReducer.class);
		job.setReducerClass(BusyMonthReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
