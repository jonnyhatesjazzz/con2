package ekn.converter.ekn.utils.logging;

import java.util.Date;

public class Logger {
    static boolean    debug = true;
    static boolean    log = true;
    public static void log(String message ){
        System.out.printf ( " %s %s  \n" , new Date(System.currentTimeMillis()), message);
    }

    public static void debug(String message ){
        if (debug)
        System.out.printf ( "DEBUG: %s %s \n" , new Date(System.currentTimeMillis()), message);
        else return;
    }

}
