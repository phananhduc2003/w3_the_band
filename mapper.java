import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<Object, Text, Text, FloatWritable> {
    private Text successLevel = new Text();
    private FloatWritable studyHours = new FloatWritable();
    private boolean isFirstLine = true;  // Cờ để bỏ qua dòng đầu tiên

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Đọc từng dòng dữ liệu
        String line = value.toString();

        // Bỏ qua dòng đầu tiên
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        // Tách các trường dữ liệu
        String[] fields = line.split(",");

        // Kiểm tra số lượng cột
        if (fields.length < 5) {
            return;  // Bỏ qua dòng không hợp lệ
        }

        try {
            // Lấy mức độ thành công và số giờ học
            successLevel.set(fields[4]);  // Mức độ thành công (Low/Moderate/High)
            studyHours.set(Float.parseFloat(fields[1]));  // Số giờ học (dạng thập phân)
            context.write(successLevel, studyHours);
        } catch (NumberFormatException e) {
            // Bỏ qua dòng dữ liệu không hợp lệ
        }
    }
}
