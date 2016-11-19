package hadoop16.mapreduce;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.ComplementaryNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.vectorizer.TFIDF;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

public class Classifier {
	public final static String MODEL_PATH_CONF = "modelPath";
	public final static String DICTIONARY_PATH_CONF = "dictionaryPath";
	public final static String DOCUMENT_FREQUENCY_PATH_CONF = "documentFrequencyPath";
	public final static String LABEL_INDEX_PATH_CONF = "labelIndexPath";
	
	private static ComplementaryNaiveBayesClassifier classifier;
	private static Map<String, Integer> dictionary;
	private static Map<Integer, Long> documentFrequency;
	private static Map<Integer, String> labelIndex;
	

	public Classifier(Configuration configuration) throws IOException {
		String modelPath = configuration.getStrings(MODEL_PATH_CONF)[0];
		String dictionaryPath = configuration.getStrings(DICTIONARY_PATH_CONF)[0];
		String documentFrequencyPath = configuration.getStrings(DOCUMENT_FREQUENCY_PATH_CONF)[0];
		String labelIndexPath = configuration.getStrings(LABEL_INDEX_PATH_CONF)[0];
		
		//loads the dictionary obtained from the training phase to a map
		dictionary = readDictionnary(configuration, new Path(dictionaryPath)); 
		//loads the document frequency obtained from the training phase to a map
		documentFrequency = readDocumentFrequency(configuration, new Path(documentFrequencyPath));
		//loads the labelIndex
		labelIndex=readLabelIndex(configuration, new Path(labelIndexPath));
		//creates a Naive Bayes Model object based on model that we obtained in the training phase
		NaiveBayesModel model=null;
		//try{
		 model = NaiveBayesModel.materialize(new Path(modelPath), configuration);
	//} catch (EOFException e) {
      //  e.printStackTrace();
   // }  
		//creates a classifier object
		classifier = new ComplementaryNaiveBayesClassifier(model);
	}
	
	public String[] classify(String lemmaIndex) throws IOException {
		//converts lemmaIndex from Text format to List of StringInteger format
		StringIntegerList sil=new StringIntegerList();
		sil.readFromString(lemmaIndex);			
		List<StringInteger> lemmaIndexList=sil.getIndices();
		
		//Counts the number of words inside the lemmaIndex which exist in the dictionary
		int wordCount=0;
		for (StringInteger entry:lemmaIndexList) {
			String word = entry.getString();
			Integer wordId = dictionary.get(word);
			if(wordId!=null) //skips if the word does not exist in the dictionary
				wordCount++;
		}
		
		//calculates the tfidf of the lemmaIndex vector
		int documentCount = documentFrequency.get(-1).intValue();
		Vector vector = new RandomAccessSparseVector(10000);
		TFIDF tfidf = new TFIDF();		for (StringInteger entry:lemmaIndexList) {
			String word = entry.getString();
			int count = entry.getValue();
			Integer wordId = dictionary.get(word);
			if(wordId==null) //skips if the word does not exist in the dictionary
				continue;
			Long freq = documentFrequency.get(wordId);
			if(freq==null)
				freq=(long)0;
			System.out.println(count);
			System.out.println(freq.intValue());
			System.out.println(wordCount);
			System.out.println(documentCount);
			double tfIdfValue = tfidf.calculate(count, freq.intValue(), wordCount, documentCount);
			System.out.println(tfIdfValue);
			vector.setQuick(wordId, tfIdfValue);
		}
		

		
		//classifies the tfidf
		Vector resultVector = classifier.classifyFull(vector);
		
		//obtain the 3 label index with higher wieght
		int[] bestIndecis={-2,-2,-2};
		double[] bestScores={-Double.MAX_VALUE,-Double.MAX_VALUE,-Double.MAX_VALUE};
		for(Element element: resultVector.all()) {
			int labelIndex = element.index();
			double score = element.get();
			if (score >= bestScores[0]) {
				bestScores[2]=bestScores[1];
				bestScores[1]=bestScores[0];
				bestScores[0]=score;
				
				bestIndecis[2]=bestIndecis[1];
				bestIndecis[1]=bestIndecis[0];
				bestIndecis[0]=labelIndex;
				
			}else if (score >= bestScores[1]) {
				bestScores[2]=bestScores[1];
				bestScores[1]=score;
				
				bestIndecis[2]=bestIndecis[1];
				bestIndecis[1]=labelIndex;
				
			}else if (score > bestScores[2]) {
				bestScores[2]=score;
				
				bestIndecis[2]=labelIndex;
				
			}
		}
String [] bestLabels=new String[3];
for(int i=0;i<bestLabels.length;i++)
	bestLabels[i]=labelIndex.get(bestIndecis[i]);

		return bestLabels;
	}
	
	private static Map<String, Integer> readDictionnary(Configuration conf, Path dictionnaryPath) {
		Map<String, Integer> dictionnary = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictionnaryPath, true, conf)) {
			dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		return dictionnary;
	}

	private static Map<Integer, Long> readDocumentFrequency(Configuration conf, Path documentFrequencyPath) {
		Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(documentFrequencyPath, true, conf)) {
			documentFrequency.put(pair.getFirst().get(), pair.getSecond().get());
		}
		return documentFrequency;
	}
	
	private static Map<Integer, String> readLabelIndex(Configuration conf, Path labelIndexPath) {
	Map<Integer, String> labels = BayesUtils.readLabelIndex(conf, labelIndexPath);
	return labels;
	}

}
