package progressive.row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("row.csv"))) {  // file name may vary
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        }

        int count = 0;
        double sum = 0.0d;

        for (int i = 0; i < records.size(); i++) {
            count++;
            String current_str = records.get(i).get(0);
            double current_double = Double.parseDouble(current_str);

            if (count == 4) { // count = the number of group in each partition
                System.out.println(sum);
                count = 1;
                sum = 0.0d;
            }

            sum += current_double;

        }
        System.out.println(sum);
    }
}
