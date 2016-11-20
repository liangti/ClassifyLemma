package hadoop16.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class MapReduceClassifier {

	public static class ClassifierMap extends Mapper<Text, Text, Text, NullWritable> {
		private final static Text outputKey = new Text();
		
		private static Classifier classifier;
		private static HashMap<String,String[]> professions=new HashMap<String,String[]>();
		private final static NullWritable nullWritable = NullWritable.get();

		@Override
		protected void setup(Context context) throws IOException {
			initClassifier(context);
			loadProfessions();
		}
public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] prof=professions.get(key.toString().trim());
			if(prof==null)
				return;
			String[] bestLabels = classifier.classify(value.toString());
			boolean flag=false;
			for(int i=0;i<prof.length;i++){
				if(prof[i].equals(bestLabels[0].trim())){flag=true;break;}
				if(prof[i].equals(bestLabels[1].trim())){flag=true;break;}
				if(prof[i].equals(bestLabels[2].trim())){flag=true;break;}
			}
			String outputValue;
			if(flag)outputValue="Y";
			else outputValue="N";
			
			outputKey.set(key+":"+outputValue+","+bestLabels[0]+","+bestLabels[1]+","+bestLabels[2]);
			context.write(outputKey, nullWritable);
		}
		private static void initClassifier(Context context) throws IOException {
			if (classifier == null) {
				synchronized (ClassifierMap.class) {
					if (classifier == null) {
						classifier = new Classifier(context.getConfiguration());
					}
				}
			}
		}

private static void loadProfessions() throws IOException {
		
			
			String PROFESSIONS_FILE = "p1.txt";
			
			ClassLoader cl = MapReduceClassifier.class.getClassLoader();
			
			String fileUrl = cl.getResource(PROFESSIONS_FILE).getFile();
			
			// Get jar path
			String jarUrl = fileUrl.substring(5, fileUrl.length() - PROFESSIONS_FILE.length() - 2);
			
			JarFile jf = new JarFile(new File(jarUrl));
			
			
			// Scan the people.txt file inside jar
			Scanner sc = new Scanner(jf.getInputStream(jf.getEntry(PROFESSIONS_FILE)),"UTF-8");
			String line=null;
			String name=null;
			String profString=null;
			String[] professionsArr=null;
			while(sc.hasNext())
			{
			
				line=sc.nextLine();
				if(line==null)
					continue;
				String [] splt=line.split(" : ");
				if(splt.length<2)
					continue;
				 profString=splt[1];
				 name=splt[0];
				
				name=name.trim();
				profString=profString.trim();
				if("".equals(name))
					continue;
				if("".equals(profString))
						continue;
				professionsArr=profString.split(",");
				for (int i=0;i<professionsArr.length;i++)
					professionsArr[i]=professionsArr[i].trim();
								
				
				 professions.put(name,professionsArr);
				 
			
			}
			
			jf.close();
			sc.close();
		

		}
	

		
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 7) {
			System.out.println("Arguments: -Dmapreduce.job.queuename=hadoop16 [model] [labelIndex] [dictionnary] [document frequency] [lemmaIndex(input) path] [output path]");
			return;
		}
		Configuration conf = new Configuration();
		GenericOptionsParser gop=new GenericOptionsParser(conf, args);
		String[] otherArgs=gop.getRemainingArgs();
		
		String modelPath = args[1];
		String labelIndexPath = args[2];		
		String dictionaryPath = args[3];
		String documentFrequencyPath = args[4];
		String lemmaIndexPath = args[5];
		String outputPath = args[6];

		
	
//		String modelPath = otherArgs[0];
//		String labelIndexPath = otherArgs[1];		
//		String dictionaryPath = otherArgs[2];
//		String documentFrequencyPath = otherArgs[3];
//		String lemmaIndexPath = otherArgs[4];
//		String outputPath = otherArgs[5];
		
	
		conf.setStrings(Classifier.MODEL_PATH_CONF, modelPath);
		conf.setStrings(Classifier.DICTIONARY_PATH_CONF, dictionaryPath);
		conf.setStrings(Classifier.DOCUMENT_FREQUENCY_PATH_CONF, documentFrequencyPath);
		conf.setStrings(Classifier.LABEL_INDEX_PATH_CONF, labelIndexPath);
		// do not create a new jvm for each task
		//conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);
	
		
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "	"); 
		Job job=Job.getInstance(conf, "classifier");
		job.setJarByClass(MapReduceClassifier.class);
		job.setMapperClass(ClassifierMap.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);	
		job.setOutputFormatClass(TextOutputFormat.class);
	
		FileInputFormat.addInputPath(job, new Path(lemmaIndexPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
	
		job.waitForCompletion(true);
	}
}
