# Lexicon
MapReduce program for Hadoop. This practice aims to design and implement a small MapReduce application to calculate the trending topics on twitter and feelings associated with such hashtags. **IntelliJ project**

## Classes
Here will be described all the classes of de project. They can be found on the **Sources/src/** folder.

### Cleaner
This class uses the **chain mapper** and **hadoop-predefined FieldSelectionMapper** in order to normalize and cleanup of the input data.
- All the tweets will be changed to lower-case.
- All the tweets with lacking of the processed fields (hashtags, text) will be removed.
- Remove all the tweets in any language different from Spanish.

#### Input/Output
- Input: Directory with all tweets data. All files will be processed.
- Output: Directory for the process output.

### TrendingTopics
This class uses the **RegexMapper** in order to generate a list of all hashtags. Also, it uses a custom **Reducer** just to count the occurrences of each hashtag to determine the **trending topics**. Also, to improve the performance, **Combiners** and **Partitioners** are used.

#### Input/Output
- Input: Directory with all tweets data (all files will be processed). Number of partitioners.
- Output: Directory for the process output.

### TopNPattern
This class uses **TopNPattern** to get the **N** trending topics.

#### Input/Output
- Input: **N** and Output directory of **TrendingTopics** class.
- Output: Directory for the process output.

### HashtagSentiment
This class uses **Distributed Cached** files to define a set of positive and negative words. For each hashtag will get a ratio of the positive and negative words in order to "determine" the global sentiment about that hashtag. In this case, the **mapper** will generate, for each hashtag in each tweet of the input file an **OwnWritable** data type. This is a custom **Writable** class composed by two **IntWritables**:
  - Tweet's polarity (Tweet's positive words - Tweet's negative words)
  - Tweet's length.

The **reducer** will receive this structure in order to get the global sentiment about that hashtag.

#### Input/Output
- Input: Negative words file, Positive words file and output directory of **Cleaner** class.
- Output: Directory for the process output.

### JobsJoint
This class gathers all the previous MapReduce jobs. Use the compressed sequence file format for the partial results.

#### Input/Output
- Input: Directory with all tweets data, **N**, Negative words file and Positive words file.
- Output: CleanedData output, Topics output, TrendingTopics output and HashtagSentiment output.

## Running
- Step by step: **Cleaner** > **TrendingTopics** > **TopNPattern** > **HashtagSentiment**
- All together: **JobsJoint**
