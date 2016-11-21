package hadoop16.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

public class LemmaToSeq {
	private static HashMap<String,String[]> professions=new HashMap<String,String[]>();

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		loadProfessions(conf,args[3]);
		FileSystem fs = FileSystem.get(conf);

		Path outputPath = new Path(args[2]);
		String inputPath=args[1];		
		FileStatus[] status = fs.listStatus(new Path(inputPath));		
		String line=null;
		String[] prof=null;
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, outputPath, Text.class, Text.class);
		for (int i=0;i<status.length;i++){
            BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			
			while((line=reader.readLine())!=null)
			{
				String[] split=line.split("\t",2);
				if(split[0]==null || "".equals(split[0]))
					continue;
				prof=professions.get(split[0]);
				if(prof==null)
					continue;
				
				Text value=createTextFromLemma(split[1]);
				//for each profession write a single <key, value> pair into the sequence file
				for(int j=0;j<prof.length;j++){
					Text key=new Text("/"+prof[j]+"/1");
				    writer.append(key, value);
				}
			}
		}
		writer.close();
	}
	
	public static Text createTextFromLemma(String content) throws IOException
	{
		StringIntegerList sil=new StringIntegerList();
		sil.readFromString(content);			
		List<StringInteger> pairList=sil.getIndices();
		Text outValue=new Text();
		
		String strValue="";
	
		for (StringInteger pair : pairList) {  
			for(int j=0;j<pair.getValue();j++)
				strValue+=pair.getString()+" ";		  
		} 
		outValue.set(strValue);
		return outValue;
	}
	
	public static void loadProfessions(Configuration conf,String path) throws IOException {
		
		
//		String PROFESSIONS_FILE = "professions.txt";
//		
//		ClassLoader cl = LemaToSeq.class.getClassLoader();
//		
//		String fileUrl = cl.getResource(PROFESSIONS_FILE).getFile();
//		
//		// Get jar path
//		String jarUrl = fileUrl.substring(5, fileUrl.length() - PROFESSIONS_FILE.length() - 2);
//		
//		JarFile jf = new JarFile(new File(jarUrl));
		
		Path professionsPath = new Path (path);
		FileSystem fs = FileSystem.get(conf);		
		// Scan the people.txt file inside jar
		Scanner sc = new Scanner(fs.open(professionsPath),"UTF-8");
		String line=null;
		String name=null;
		String[] professionsArr=null;
		while(sc.hasNext())
		{
		
			line=sc.nextLine();
			if(line==null)
				continue;
			String[] splt=line.split(" : ");
			if(splt.length<2)
				continue;
			name=splt[0].trim();
			if("".equals(name))
				continue;
			if("".equals(splt[1].trim()))
					continue;
			professionsArr=splt[1].split(",");
			for (int i=0;i<professionsArr.length;i++)
				professionsArr[i]=professionsArr[i].trim();
							
			
			 professions.put(name,professionsArr);
			 
		
		}
		
//		jf.close();
		sc.close();
	

	}
}


