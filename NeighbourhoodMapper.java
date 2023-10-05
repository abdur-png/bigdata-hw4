import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NeighbourhoodMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> fields = parseCSV(value.toString());
        if (fields.size() > 5) {
            word.set(fields.get(4) + " " + fields.get(5));
            context.write(word, one);
        }
    }

    private List<String> parseCSV(String csvLine) {
        List<String> result = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder field = new StringBuilder();

        for (char c : csvLine.toCharArray()) {
            switch (c) {
                case ',':
                    if (inQuotes) {
                        field.append(c);
                    } else {
                        result.add(field.toString());
                        field.setLength(0); // clear the field
                    }
                    break;
                case '"':
                    inQuotes = !inQuotes;
                    break;
                default:
                    field.append(c);
                    break;
            }
        }
        result.add(field.toString()); // add the last field
        return result;
    }
}
