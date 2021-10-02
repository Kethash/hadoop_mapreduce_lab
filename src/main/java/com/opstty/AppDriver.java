package com.opstty;

import com.opstty.job.DistrictTrees;
import com.opstty.job.KindCount;
import com.opstty.job.ShowSpecies;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtrees", DistrictTrees.class,
                    "A map/reduce program counts the addresses of the different trees");
            programDriver.addClass("showspecies", ShowSpecies.class,
                    "A map/reduce program showing the different species of the different trees");
            programDriver.addClass("kindcount", KindCount.class,
                    "A map/reduce program counting the different kind of the different trees");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
