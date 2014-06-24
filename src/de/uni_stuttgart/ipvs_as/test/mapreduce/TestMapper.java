package de.uni_stuttgart.ipvs_as.test.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Test mapper for integration testing.
 * 
 * See {@link de.uni_stuttgart.ipvs_as.test.EndToEndTest} for a high-level
 * outline.
 * 
 * @author acg
 */
public class TestMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private IntWritable outIdx = new IntWritable();
	private IntWritable outElemVal = new IntWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Each input value is a CVS line with 6 integers, i.e
		// 4, 7, 1224, 24, 3, 9. Input keys are sequence numbers
		// and meaningless for this purpose.

		String[] parts = value.toString().split(",");
		assert parts.length == 5;

		int i = 0;
		for (String part : parts) {
			final int elem = Integer.parseInt(part.trim());

			outIdx.set(i);
			outElemVal.set(elem);
			context.write(outIdx, outElemVal);
			++i;
		}
	}
}
