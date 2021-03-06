package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtrees", DistrictTrees.class,
                    "A map/reduce program counts the number of trees by district.");
            programDriver.addClass("showspecies", ShowSpecies.class,
                    "A map/reduce program showing the different species of the different trees.");
            programDriver.addClass("kindcount", KindCount.class,
                    "A map/reduce program counting the different kind of the different trees.");
            programDriver.addClass("tallesttree", TallestTree.class,
                    "A map/reduce program showing the tallest trees from different species.");
            programDriver.addClass("sortTreeheight", SortTreeHeight.class,
                    "A map/reduce program sorting the tree height.");
            programDriver.addClass("districtoldestTree", DistrictOldestTree.class,
                    "A map/reduce program displaying the district with the oldest tree");
            programDriver.addClass("districtmostTrees", DistrictMostTrees.class,
                    "A map/reduce program displaying the district with the most trees");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
