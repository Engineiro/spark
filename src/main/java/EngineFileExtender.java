import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by decision-iq on 2/2/15.
 */
public class EngineFileExtender {

    public static void main(String[] args) {

//        int cycleLimit = Integer.valueOf(args[0]);
//        int cycleLimit = 336; // Two Weeks
        int cycleLimit = 360000;

        HashMap<String,Integer> sensorNameToPos = new HashMap<String, Integer>();
        sensorNameToPos.put("T2_NULL" , 5);
        sensorNameToPos.put("T24",6);
        sensorNameToPos.put("T30",7);
        sensorNameToPos.put("T50",8);
        sensorNameToPos.put("P2_NULL",9);
        sensorNameToPos.put("P15",10);
        sensorNameToPos.put("P30",11);
        sensorNameToPos.put("NF",12);
        sensorNameToPos.put("NC",13);
        sensorNameToPos.put("EPR",14);
        sensorNameToPos.put("PS30",15);
        sensorNameToPos.put("PHI",16);
        sensorNameToPos.put("NRF",17);
        sensorNameToPos.put("NRC",18);
        sensorNameToPos.put("BPR",19);
        sensorNameToPos.put("FARB",20);
        sensorNameToPos.put("HTBLEED",21);
        sensorNameToPos.put("NF_DMD",22);
        sensorNameToPos.put("PCNFR_DMD",23);
        sensorNameToPos.put("W31",24);
        sensorNameToPos.put("W32",25);

//        ImmutableMap.copyOf(sensorNameToPos);

        int engineIndexPos = 0;
        int cycleIndexPos = 1;

        int currEngIndexVal = 0;
        int prevEngIndexVal = 0;

        CSVReader in = null;
        CSVWriter out = null;

        try {

            in = new CSVReader(new FileReader("test_FD001.csv"));
            out = new CSVWriter(new FileWriter("out_test_FD001.csv"), ',', CSVWriter.NO_QUOTE_CHARACTER);

            String[] rowIn;
            String[] prevRowin = null;
            String[] rowOut;

            while((rowIn = in.readNext()) != null){
                System.out.println(rowIn[0]);

                currEngIndexVal = Integer.valueOf(rowIn[engineIndexPos]);


                // Find next engine index.
                if (currEngIndexVal != prevEngIndexVal){
                    for (int i = 0; i < cycleLimit; i++) {
                        out.writeNext(prevRowin);
                    }
                }
                out.writeNext(rowIn);
                // Compare cycle time to input cycle

                // remember the last set of csv values and repeat until input cycle.
                prevEngIndexVal = Integer.valueOf(rowIn[engineIndexPos]);
                prevRowin = rowIn;
            }
        } catch (FileNotFoundException fnfe ) {
            System.out.println("Did not find file.");
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("Something happened reading IO");
            ioe.printStackTrace();
        } finally {
            IOUtils.closeQuietly(out);
            IOUtils.closeQuietly(in);
        }

    }
}
