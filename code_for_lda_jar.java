
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import au.com.bytecode.opencsv.CSVReader;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.pipe.iterator.CsvIterator;
import cc.mallet.types.InstanceList;
import cc.mallet.util.Randoms;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import edu.umass.cs.mallet.users.kan.topics.ParallelTopicModel;

public class ToRun {
	
	private String writeLDAFile(String out_dir,
					String checkinFile,String venueFile,String postfix,boolean isBinary) {
		
		String outputFileName = out_dir+"input_"+postfix+".txt";
		
		HashMap<String,HashMultiset<String> > usersToVenues = new HashMap<String,HashMultiset<String>>();
		try {
			//HashSet<String> venues = new HashSet<String>();
			//BufferedReader vReader = new BufferedReader(new FileReader(venueFile));
			//String vLine = "";
			//while((vLine=vReader.readLine())!= null){
			//	venues.add(vLine.replace("\"",""));
			//}
			//vReader.close();
			
			CSVReader reader = new CSVReader(new FileReader(checkinFile), ',', '\"', 1);
			String[] line;
			while((line=reader.readNext())!= null){
				//if(!venues.contains(line[2])){
				//	continue;
				//}
				if(!usersToVenues.containsKey(line[1])){
					HashMultiset<String> hs = HashMultiset.create();
					usersToVenues.put(line[1],hs);		
				} 
				usersToVenues.get(line[1]).add(line[2]);
			}
			reader.close();
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
			for(Entry<String,HashMultiset<String> > e:usersToVenues.entrySet()){
				writer.write(e.getKey());
				if(isBinary){
					for(String s:e.getValue().elementSet()){
						writer.write(" " + s);
					}
				} else{
					for(Multiset.Entry<String> entry:e.getValue().entrySet()){
						for(int i = 0; i < entry.getCount();i++){
							writer.write(" " +entry.getElement());
						}
					}
				}
				
				writer.newLine();
			}
			writer.close();
			return outputFileName;
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public ToRun(int ntopics,String homeDirectory, String city, 
			String checkinsFile,String venueFile, boolean isBinary,
			double alphaSum, double beta) throws IOException{
		
		String out_dir = homeDirectory+city+"/";
		String checkinsFileFull = out_dir+checkinsFile;
		String venuesFileFull = out_dir+venueFile;
		String postfix = checkinsFile.replace("checkins_","").replace(".csv","") + "_"+isBinary+"_"+ntopics+"_"+beta+"_"+alphaSum;
		String outputFile = writeLDAFile(out_dir,checkinsFileFull,venuesFileFull,
				postfix,isBinary);
		
		//from http://mallet.cs.umass.edu/import-devel.php
		ArrayList pipeList = new ArrayList();
        pipeList.add( new CharSequence2TokenSequence(Pattern.compile("[\\p{L}\\p{N}_]+")) );
        pipeList.add( new TokenSequence2FeatureSequence() );
        InstanceList ldaInstances = new InstanceList (new SerialPipes(pipeList));
        Reader fileReader = new InputStreamReader(new FileInputStream(new File(outputFile)), "UTF-8");
        ldaInstances.addThruPipe(new CsvIterator (fileReader, Pattern.compile("(\\w+)\\s+(.*)"),
                                               2, -1, 1)); // data, name fields
     
        BufferedWriter writer = new BufferedWriter(
        		new FileWriter("/Users/kjoseph/Dropbox/Foursquare/"+city+"_"+postfix+".txt"));
        for(int i = 0; i < 10; i++){
        	InstanceList[] ldaTrainTest =
                    ldaInstances.split(new Randoms(i),
                    new double[] {0.9, 0.1, 0.0});
             
        	ParallelTopicModel model = new ParallelTopicModel(ntopics,alphaSum,beta);
        	model.addInstances(ldaTrainTest[0]);
        	model.setTopicDisplay(0, 50);
        	model.setBurninPeriod(1000);
        	model.setNumIterations(2500);
        	model.estimate();
        	model.setNumThreads(3);
        	model.printTopicWordWeights(new File(out_dir+"topicWordWeights"+postfix+"_"+i+".txt"));
        	writer.write(i + " " + model.getProbEstimator().evaluateLeftToRight(ldaTrainTest[1], 
        			10, false,null) + "\n");
            model.printDocumentTopics(new File(out_dir+"docTopics"+postfix+".txt"));
        	
        }
        writer.close();
        //String topWordsFileName = out_dir+"topWords"+postfix+".txt";
        //model.printTopWords(new File(topWordsFileName), 50, true);

               //model.printTypeTopicCounts(new File(out_dir+"typeTopicCounts"+postfix+".txt"));
        //model.printState(new File(out_dir+"state"+postfix+".txt"));
     
        //writer.write(top +"\t"+i+"\t"+model.getProbEstimator().evaluateLeftToRight(ldaTrainTest[1], 10, false,null));
        //ObjectOutputStream oos = 
    	//		new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("lda_f")));
         //   oos.writeObject(model);
          //  oos.close();
		
	}

	public static void main(String[] args){
		
		int ntopics = Integer.valueOf(args[0]);
		String h_dir = args[1];
		String city = args[2];
		String checkins_file = args[3];
		String venues_file = args[4];
		boolean isBinary = args[5].equals("1");
		double alphaSum = Double.valueOf(args[6]);
		double beta =Double.valueOf(args[7]);
		try{
			System.out.println(args[1]);
			new ToRun(ntopics,h_dir,city,checkins_file,venues_file,isBinary,alphaSum,beta);
		} catch(IOException e){
			e.printStackTrace();
		}
	}
}
