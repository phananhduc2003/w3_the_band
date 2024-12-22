import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        // Tính tổng số giờ học và đếm số lượng
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        result.set(sum / count);  // Tính trung bình
        context.write(key, result);  // Xuất (Success Level, Average Study Hours)
    }
}