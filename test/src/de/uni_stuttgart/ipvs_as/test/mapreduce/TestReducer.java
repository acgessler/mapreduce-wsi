package de.uni_stuttgart.ipvs_as.test.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Test reducer for integration testing.
 * 
 * See {@link de.uni_stuttgart.ipvs_as.test.EndToEndTest} for a high-level
 * outline.
 * 
 * @author acg
 */
public class TestReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private IntWritable result = new IntWritable();

	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		long sum = 0L;
		int count = 0;
		// Use a long to avoid overflow for reasonable input sizes
		for (IntWritable value : values) {
			sum += value.get();
			++count;
		}

		// The assumption in the test is that the mean will
		// be exactly 3. The extra term is to avoid any
		// deviations being accidentally hidden by the integer division.
		result.set((int) ((sum / count) - (sum % count) * 100));
		context.write(key, result);
	}
}