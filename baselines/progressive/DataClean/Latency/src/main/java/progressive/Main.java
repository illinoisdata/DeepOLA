package progressive;

import org.joda.time.*;
import org.joda.time.format.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("sample.csv"))) {  // file name may vary
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        }

        DateTimeFormatter format = ISODateTimeFormat.dateTime();
        String startText = records.get(0).get(0);
        DateTime start = format.parseDateTime(startText);

        for (int i = 1; i < records.size(); i++) {
            String endText = records.get(i).get(0);
            DateTime end = format.parseDateTime(endText);
            Duration duration = new Duration(start, end);
            System.out.println(duration.getMillis());
        }

    }
}
