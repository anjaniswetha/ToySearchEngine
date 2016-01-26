import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.TreeMap;
 
public class Search {
 
    public static void main(String[] args) {
    	
    	 Map<String, List<String>> map = new HashMap<String, List<String>>();
    	 
    	  System.out.print("Search for words: ");
          Scanner in = new Scanner(System.in);
          String token= in.nextLine();
          double starttime  = System.currentTimeMillis();
          ArrayList<String> al=new ArrayList<String>();
          ArrayList<String> firstletter=new ArrayList<String>();
          
          StringTokenizer itr1 = new StringTokenizer(token);
          
          //get words and first letters of words in an ArrayLists
          
          while(itr1.hasMoreTokens())
          {
          	String words = itr1.nextToken();
          	
          	String firstletters = Character.toString(words.charAt(0)).toLowerCase();
          	al.add(words);
          	firstletter.add(firstletters);
          	 
          }	         	 
          
         
   // for each letter get the approprate files and store them in a hash map       
          
       for(String letter: firstletter)
            {     
          
       try (BufferedReader br = new BufferedReader(new FileReader("/home/cosc6376/bigd24/InvertedIndex/build/lib/InvertedOutputFile4/"+letter   +"-WORD-r-00000")))
           {
       
            String sCurrentLine;
            
            while ((sCurrentLine = br.readLine()) != null) {
                
                StringTokenizer itr = new StringTokenizer(sCurrentLine);
                List<String> valSetOne = new ArrayList<String>();
                int i = 0;
               String key = itr.nextToken();
                while(itr.hasMoreTokens())
                {
                	valSetOne.add(itr.nextToken());
                	
                }
                
                map.put(key, valSetOne);
            }
            
                
 
        } catch (IOException e) {
            e.printStackTrace();
        } 
        
    }
        
       
        
  //check for the given words in the files and get their frequencies
        Map<String, List<String>> searchwords = new HashMap<String, List<String>>();
                 
        for(String temp : al)
        {
          if(map.containsKey(temp))
          {
        	  searchwords.put(temp, map.get(temp));
          }
        }
              
        Map<String, List<String>> finalmap = new HashMap<String, List<String>>();
        
        
        //put docid and frequences in a map
        for (Map.Entry<String, List<String>> entry : searchwords.entrySet()) {
            String key1 = entry.getKey();
            List<String> values = new ArrayList<String>();
            values = entry.getValue();
            for (String temp: values){
            	  List<String> values1 = new ArrayList<String>();
            	 String[] s = temp.split(":");
            	
               	 String doc = s[0];
              	if(finalmap.containsKey(doc))
                	finalmap.get(doc).add(s[1]);
               	else{
            	  values1.add(s[1]);
                  finalmap.put(doc, values1);   
               	}
                       
             }

            
        }
        
         
        Map<Integer, String> finalmap1 = new HashMap<Integer, String>();
        for (Map.Entry<String, List<String>> entry : finalmap.entrySet()) {
        
        	   List<String> values2 = new ArrayList<String>();
        	   values2 = entry.getValue();
        	   String doc = entry.getKey();
        	   int sum = 0;
        	   for(String temp: values2)
        		   sum  = sum + Integer.parseInt(temp);
        	   
        	   finalmap1.put(sum, doc);
        	        		   
        }
        
        
       //sort the MAP and print the top ten documents with their frequencies 
       Map<Integer, String> treemap = new TreeMap<Integer, String>(finalmap1);
       
        ArrayList<Integer> keys = new ArrayList<Integer>(treemap.keySet());
       
        int count = 0;
        for(int i=keys.size()-1; i>=0;i--){
            System.out.println(treemap.get(keys.get(i))+ ":"+keys.get(i));
            count++;
            if (count == 10)
            	break;
        }
        
        double endtime  = System.currentTimeMillis();
        System.out.println("Time taken for execution"+(endtime-starttime)/1000);
        
    }
    
}
