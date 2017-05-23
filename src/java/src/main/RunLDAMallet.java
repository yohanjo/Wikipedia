package main;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.CharSequenceLowercase;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.types.Alphabet;
import cc.mallet.types.IDSorter;
import cc.mallet.types.Instance;
import cc.mallet.types.InstanceList;
import util.DoubleMatrix;
import util.io.PrintFileWriter;


public class RunLDAMallet {
	
	// Command arguments
	public static class Parameters {
		@Parameter(names = "-t", description = "Number of topics.", required=true)
		public int numTopics = -1;

		@Parameter(names = "-i", description = "Number of iterations.", required=true)
		public int numIters = -1;

		@Parameter(names = "-th", description = "Number of threads.")
		public int numThreads = 1;

		@Parameter(names = "-to", description = "Number of iterations for temporary output files.")
		public int numTmpIters = -1;

		@Parameter(names = "-log", description = "Number of iterations for calculating log-likelihood.")
		public int numlogIters = -1;

		@Parameter(names = "-pw", description = "Number of top words to display.")
		public int numProbWords = 100;
		
		@Parameter(names = "-a", description = "Alpha.", required=true)
		public double alpha = -1;

		@Parameter(names = "-b", description = "Beta.", required=true)
		public double beta = -1;

		@Parameter(names = "-d", description = "Input directory.", required=true)
		public String inDir = null;

		@Parameter(names = "-o", description = "Output directory.")
		public String outDir = null;

		@Parameter(names = "-data", description = "Data file name prefix.", required=true)
		public String dataFileName = null;
		
		@Parameter(names = "-sw", description = "Stopwords file name.")
		public String stopwordsFileName = null;

		@Parameter(names = "-mw", description = "Minimum number of words to include.")
		public int minNumWords = 1;
		
		@Parameter(names = "-help", description = "Command description.", help=true)
		public boolean help;
	}


	public static void main(String[] args) throws Exception {
		
		// Load command arguments
		Parameters inParams = new Parameters();
		JCommander cmd = new JCommander(inParams, args);
		if (inParams.help) {
			cmd.usage();
			System.exit(0);
		}
		
		String dataPath = inParams.inDir+"/"+inParams.dataFileName;
		int numTopics = inParams.numTopics;
		String outDir = inParams.outDir;
		if (outDir == null) outDir = inParams.inDir; 

		ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
		pipeList.add( new CharSequenceLowercase() );
		pipeList.add( new CharSequence2TokenSequence(Pattern.compile("[a-z\\d]*[a-z]+[a-z\\d]*")) );
		pipeList.add( new TokenSequence2FeatureSequence() );


		System.out.println("Loading training data...");
		InstanceList instances = new InstanceList (new SerialPipes(pipeList));
		CSVParser inData = new CSVParser(new FileReader(dataPath), CSVFormat.EXCEL.withHeader());
		int cnt = 0;
		for (CSVRecord record : inData) {
			String name = record.get("DocId");
			Instance inst = new Instance(record.get("Text"), null, name, null);
			instances.addThruPipe(inst); // data, label, name fields
			
			cnt++;
			if (cnt % 1000 == 0) System.out.print(".");
			if (cnt % 10000 == 0) System.out.print("("+cnt+")");
			if (cnt % 100000 == 0) System.out.println();
//			if (cnt == 1000) break;
		}
		System.out.println();
		inData.close();
		
		String outPrefix = "MalletLDA-"+inParams.dataFileName+
		                   "-T"+numTopics+
		                   "-A"+inParams.alpha+
		                   "-B"+inParams.beta+
		                   "-I"+inParams.numIters;
		
		
		ParallelTopicModel model = new ParallelTopicModel(numTopics, numTopics*inParams.alpha, inParams.beta);
		model.addInstances(instances);
		model.setNumThreads(inParams.numThreads);
		model.setNumIterations(inParams.numIters);
		model.estimate();
		
		Alphabet dataAlphabet = instances.getDataAlphabet();

		// Print Phi
		DoubleMatrix phi = new DoubleMatrix(numTopics, model.numTypes);
		for (int topic = 0; topic < numTopics; topic++) {
			for (int type = 0; type < model.numTypes; type++) {
				int[] topicCounts = model.typeTopicCounts[type];
				double weight = inParams.beta;
				int index = 0;
				while (index < topicCounts.length && topicCounts[index] > 0) {
					int currentTopic = topicCounts[index] & model.topicMask;
					if (currentTopic == topic) {
						weight += topicCounts[index] >> model.topicBits;
						break;
					}
					index++;
				}
				phi.setValue(topic, type, weight);
			}
		}
		
		CSVPrinter outPhi = new CSVPrinter(new PrintFileWriter(outDir+"/"+outPrefix+"-Phi.csv"), CSVFormat.EXCEL);
		outPhi.print("");
		for (int t = 0; t < numTopics; t++) outPhi.print("T"+t);
		outPhi.println();

		for (int type = 0; type < model.numTypes; type++) {
			outPhi.print(dataAlphabet.lookupObject(type));
			for (int topic = 0; topic < numTopics; topic++) {
				outPhi.print(phi.getValue(topic, type) / phi.getRowSum(topic));
			}
			outPhi.println();
		}
		outPhi.flush();
		outPhi.close();
		
		
		// Print Theta
		CSVPrinter outTheta = new CSVPrinter(new PrintFileWriter(outDir+"/"+outPrefix+"-Theta.csv"), CSVFormat.EXCEL);
		outTheta.print("DocId");
		for (int t = 0; t < numTopics; t++) outTheta.print("T"+t);
		outTheta.println();

		for (int i = 0; i < instances.size(); i++) {
			Instance inst = instances.get(i);
			outTheta.print(inst.getName());
			double[] topicDistribution = model.getTopicProbabilities(i);
			for (int topic = 0; topic < numTopics; topic++) {
				outTheta.print(topicDistribution[topic]);
			}
			outTheta.println();
		}
		outTheta.flush();
		outTheta.close();
		
		// Print prob words
		ArrayList<TreeSet<IDSorter>> topicSortedWords = model.getSortedWords();
		CSVPrinter outProbWords = new CSVPrinter(new PrintFileWriter(outDir+"/"+outPrefix+"-ProbWords.csv"), CSVFormat.EXCEL);
		for (int t = 0; t < numTopics; t++) outProbWords.print("T"+t);
		outProbWords.println();
		
		Vector<Iterator<IDSorter>> iterators = new Vector<Iterator<IDSorter>>();
		for (int topic = 0; topic < numTopics; topic++) {
			iterators.add(topicSortedWords.get(topic).iterator());
		}
		for (int w = 0; w < 100; w++) {
			for (int topic = 0; topic < numTopics; topic++) {
				if (iterators.get(topic).hasNext()) {
					IDSorter idCountPair = iterators.get(topic).next();
					outProbWords.print(String.format("%s (%.0f) ", dataAlphabet.lookupObject(idCountPair.getID()), idCountPair.getWeight()));
				}
				else {
					outProbWords.print("");
				}
			}
			outProbWords.println();
		}
		outProbWords.flush();
		outProbWords.close();
		

		
//		// Estimate the topic distribution of the first instance, 
//		//  given the current Gibbs state.
//		double[] topicDistribution = model.getTopicProbabilities(0);
//
//		// Get an array of sorted sets of word ID/count pairs
//		ArrayList<TreeSet<IDSorter>> topicSortedWords = model.getSortedWords();
//
//		// Show top 5 words in topics with proportions for the first document
//		for (int topic = 0; topic < numTopics; topic++) {
//			Iterator<IDSorter> iterator = topicSortedWords.get(topic).iterator();
//
//			out = new Formatter(new StringBuilder(), Locale.US);
//			out.format("%d\t%.3f\t", topic, topicDistribution[topic]);
//			int rank = 0;
//			while (iterator.hasNext() && rank < 100) {
//				IDSorter idCountPair = iterator.next();
//				out.format("%s (%.0f) ", dataAlphabet.lookupObject(idCountPair.getID()), idCountPair.getWeight());
//				rank++;
//			}
//			System.out.println(out);
//		}
//
//		// Create a new instance with high probability of topic 0
//		StringBuilder topicZeroText = new StringBuilder();
//		Iterator<IDSorter> iterator = topicSortedWords.get(0).iterator();
//
//		int rank = 0;
//		while (iterator.hasNext() && rank < 5) {
//			IDSorter idCountPair = iterator.next();
//			topicZeroText.append(dataAlphabet.lookupObject(idCountPair.getID()) + " ");
//			rank++;
//		}
//
//		// Create a new instance named "test instance" with empty target and source fields.
//		InstanceList testing = new InstanceList(instances.getPipe());
//		testing.addThruPipe(new Instance(topicZeroText.toString(), null, "test instance", null));
//
//		TopicInferencer inferencer = model.getInferencer();
//		double[] testProbabilities = inferencer.getSampledDistribution(testing.get(0), 10, 1, 5);
//		System.out.println("0\t" + testProbabilities[0]);
	}

}
