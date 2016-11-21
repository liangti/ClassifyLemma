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
 

public class Evaluation {

	private static void readTxtFile(String filepath){ 
		
        try {  
            Scanner in=new Scanner(new File(filepath),"UTF-8");  
            //PrintWriter out1=new PrintWriter("/Users/uuisafresh/Documents/workspace/ClassifyLemma/src/main/java/Split/p1.txt"); 
            //PrintWriter out2=new PrintWriter("/Users/uuisafresh/Documents/workspace/ClassifyLemma/src/main/java/Split/p2.txt"); 
            int num=0;
            int total=0;
            while(in.hasNext()){  
                String str=in.nextLine().split(":")[1];  
                if(str.charAt(0)=='Y')num++;
                total++;
                //if(num<testCount)out2.write(str+"\n");
                //else out1.write(str+"\n");
                
            }  
            in.close();
            //out1.close();
            //out2.close();
            float accuracy=(float)num/total;
            System.out.println(num+"\t"+total+"\t"+accuracy);
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        }  
    }  
     
    public static void main(String argv[]){
        String filePath = "/Users/uuisafresh/Documents/hadoop/lt";
//      "res/";
        readTxtFile(filePath);
    }
     
     
 
}

