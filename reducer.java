import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        // Tính tổng và đếm số lượng
        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }

        // Tính trung bình
        result.set(sum / count);
        context.write(key, result);
    }
}
