package progressive;

import javax.swing.text.html.StyleSheet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        List<List<String>> records = new ArrayList<>();
        double final_double = 215012153d;  // value will vary
        try (BufferedReader br = new BufferedReader(new FileReader("sample.csv"))) {  // file name may vary
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        }

        for (int i = 1; i < records.size(); i++) {
            String current_str = records.get(i).get(0);
            double current_double = Double.parseDouble(current_str);
            double percent = current_double / final_double;
            double diff = Math.abs(1 - percent);
            System.out.println(diff);
        }

    }
}
