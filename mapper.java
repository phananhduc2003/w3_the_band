import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<Object, Text, Text, IntWritable> {
    private Text successLevel = new Text();
    private IntWritable studyHours = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Phân tách từng dòng dữ liệu
        String[] fields = value.toString().split(",");
        if (!fields[0].equals("Student_ID")) {  // Bỏ qua dòng tiêu đề
            successLevel.set(fields[4]);  // Mức độ thành công (Low/Moderate/High)
            studyHours.set(Integer.parseInt(fields[1]));  // Số giờ học mỗi ngày
            context.write(successLevel, studyHours);  // Xuất cặp (Success Level, Study Hours)
        }
    }
}