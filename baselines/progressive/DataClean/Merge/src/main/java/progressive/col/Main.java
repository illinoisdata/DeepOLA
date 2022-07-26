package progressive.col;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("col.csv"))) {  // file name may vary
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        }

        for (int i = 0; i < records.size(); i++) {
            double sum = 0.0d;
            for (int j = 0; j < records.get(i).size(); j++) {
                String current_str = records.get(i).get(j);
                double current_double = Double.parseDouble(current_str);
                sum += current_double;
            }
            System.out.println(sum);
        }
    }
}
