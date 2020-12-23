package org.hades.flume.source;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;

public class Utils {

    public static void saveString(String s, String objName) {
        try {
            FileOutputStream fs = new FileOutputStream(objName);
            byte[] contentInBytes = s.getBytes();
            fs.write(contentInBytes);
            fs.flush();
            fs.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static String getString(String objName) {
        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        StringBuffer sb = new StringBuffer();
        try {
            fileInputStream = new FileInputStream(objName);
            inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");

            BufferedReader in = new BufferedReader(inputStreamReader);
            String str = null;

            while ((str = in.readLine()) != null) {
                sb.append(str);
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        return sb.toString();
    }

}
