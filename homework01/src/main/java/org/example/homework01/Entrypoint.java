package org.example.homework01;

import org.apache.hadoop.util.ProgramDriver;

/**
 * @author zhangdongdong <zhangdongdong@kuaishou.com>
 * Created on 2022-03-12
 */
public class Entrypoint {

    public static void main(String argv[]){
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("mobiletraffic", MobileTraffic.class,
                    "A map/reduce program that caculate the mobile traffic in the input files.");
            exitCode = pgd.run(argv);
        } catch(Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }

}
