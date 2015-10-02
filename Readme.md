Code for calculating shortest path from 1 page of wikipedia to another by creating a webgraph of wikipedia in distributed manner
and by solving for shortest path using BFS, also in distributed manner.

Download Wikipedia Data from their downloads page http://en.wikipedia.org/wiki/Wikipedia:Database_download. It should be around 10 GB
or use the provided simple wikipedia data 

Just do the following steps
0. bunzip2 simplewiki-latest-pages-articles.xml.bz2 

Put the file in hdfs
1. hadoop fs -put simplewiki-latest-pages-articles.xml

Compile using 
2. mvn package

Convert it using XML2GraphConvert.java file in the manish.hadoop.Wikipedia package.
3. hadoop jar target/WikipediaAnalysis-1.0-SNAPSHOT.jar manish.hadoop.Wikipedia.ConvertXML2GraphDriver simplewiki-latest-pages-articles.xml dummy

Use
hadoop jar target/WikipediaAnalysis-1.0-SNAPSHOT.jar        # to use with default parameters
OR
hadoop jar target/WikipediaAnalysis-1.0-SNAPSHOT.jar [i/p_file] [o/p_folder] [source_page_title] [destination_page_title]

Eg. 
4. hadoop jar target/WikipediaAnalysis-1.0-SNAPSHOT.jar manish.hadoop.Wikipedia.WikiSSSPDriver dummy/part-r-00000 wiki_out Azure Felt

5. hadoop fs -cat wiki_out/part-r-00000

O/P will be in the below format
Azure-----Minimum---->Felt==========12
Azure-----Minimum---->BBC_Radio_7==========INFINITY # if no such path is there that connects these 2 nodes
Empty if node entered don't exist.

=================================================================================================================
TF-IDF
tf–idf, short for term frequency–inverse document frequency, is a numerical statistic that is intended to reflect
how important a word is to a document in a collection or corpus. It is often used as a weighting factor in
information retrieval and text mining. The tf-idf value increases proportionally to the number of times a word
appears in the document, but is offset by the frequency of the word in the corpus, which helps to adjust for the
fact that some words appear more frequently in general.

Variations of the tf–idf weighting scheme are often used by search engines as a central tool in scoring and
ranking a document's relevance given a user query. tf–idf can be successfully used for stop-words filtering in
various subject fields including text summarization and classification.

One of the simplest ranking functions is computed by summing the tf–idf for each query term; many more sophisticated
ranking functions are variants of this simple model.

Example from http://en.wikipedia.org/wiki/Tf%E2%80%93idf#Example_of_tf.E2.80.93idf

Main class is manish.hadoop.TIF.TFIDFDriver

All the custom writable and readable input format classes are created in WritableTools Folder.
TupleWritable is custom class that can make use of passing array of texts.

input files are in tfidf_inp same as example from wikipedia for more clarity

output result is in tfidf_out folder.

Intermediate results are in s[x]_out folder.
