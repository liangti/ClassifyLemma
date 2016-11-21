package Split;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Scanner;
 

public class SplitProfession {

	private static void readTxtFile(String filepath){ 
		int profCount= 673988;
		int splitPrcnt=5;
		int testCount=splitPrcnt*profCount/100;
		System.out.println(testCount);
        try {  
            Scanner in=new Scanner(new File(filepath),"UTF-8");  
            PrintWriter out1=new PrintWriter("/Users/uuisafresh/Documents/workspace/ClassifyLemma/src/main/java/Split/p1.txt"); 
            PrintWriter out2=new PrintWriter("/Users/uuisafresh/Documents/workspace/ClassifyLemma/src/main/java/Split/p2.txt"); 
            int num=0;
            while(in.hasNext()){  
            	String str=in.nextLine();
                if(num<testCount)out2.write(str+"\n");
                else out1.write(str+"\n");
                num++;
            }  
            in.close();
            out1.close();
            out2.close();
            System.out.println(num);
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        }  
    }  
     
    public static void main(String argv[]){
        String filePath = "/Users/uuisafresh/Downloads/professions.txt";
//      "res/";
        readTxtFile(filePath);
    }
     
     
 
}

