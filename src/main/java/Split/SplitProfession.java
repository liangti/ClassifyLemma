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
//	private void loadProfessions() throws IOException {
//		
//		
//		String PROFESSIONS_FILE = "professions.txt";
//		
//		
//		
//		// Scan the people.txt file inside jar
//		
//        File text=new File(PROFESSIONS_FILE);
//		Scanner sc = new Scanner(text);
//		String line=null;
//		String name=null;
//		String profString=null;
//		String[] professionsArr=null;
//		while(sc.hasNext())
//		{
//		
//			line=sc.nextLine();
//			if(line==null)
//				continue;
//			int lastColIndx=line.lastIndexOf(":");				
//			 profString=line.substring(lastColIndx+1);
//			 name=line.substring(0, lastColIndx);
//			
//			if("".equals(name))
//				continue;
//			if("".equals(profString.trim()))
//					continue;
//			professionsArr=profString.split(",");
//			for (int i=0;i<professionsArr.length;i++)
//				professionsArr[i]=professionsArr[i].trim();
//							
//			
//			 trainProfessions.put(name,professionsArr);
//			 
//		
//		}
//		
//		sc.close();
//		int profCount= trainProfessions.size();
//		int splitPrcnt=5;
//		int testCount=splitPrcnt*profCount/100;
//		
//		String [] namesSet=(String[])trainProfessions.keySet().toArray();
//		int index=0;
//		String key=null;
//		String[] value=null;
//		for(int i=0;i<testCount;i++)
//		{
//			value =null;
//			while(value==null)
//			{
//			index=ThreadLocalRandom.current().nextInt(0, namesSet.length);
//			key=namesSet[index];
//			value=trainProfessions.get(key);
//			}
//			trainProfessions.remove(key);
//			testProfessions.put(key, value);
//			
//				
//			}
//		
//		
//
//	
//
//	}
//}
	private static void readTxtFile(String filepath){ 
		int profCount= 673988;
		int splitPrcnt=5;
		int testCount=splitPrcnt*profCount/100;
		System.out.println(testCount);
        try {  
            Scanner in=new Scanner(new File(filepath),"UTF-8");  
            //PrintWriter out1=new PrintWriter("/Users/uuisafresh/Documents/workspace/ClassifyLemma/src/main/java/Split/p1.txt"); 
            //PrintWriter out2=new PrintWriter("/Users/uuisafresh/Documents/workspace/ClassifyLemma/src/main/java/Split/p2.txt"); 
            int num=0;
            while(in.hasNext()){  
                String str=in.nextLine().split(":")[1];  
                if(str.charAt(0)=='Y')num++;
                //if(num<testCount)out2.write(str+"\n");
                //else out1.write(str+"\n");
                
            }  
            in.close();
            //out1.close();
            //out2.close();
            System.out.println(num);
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        }  
    }  
     
    public static void main(String argv[]){
        String filePath = "/Users/uuisafresh/Documents/hadoop/part-r-00000";
//      "res/";
        readTxtFile(filePath);
    }
     
     
 
}

