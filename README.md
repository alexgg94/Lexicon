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
This class uses the **RegexMapper** in order to generate a list of all hashtags. Also, it uses a custom **Reducer** just to count the occurrences of each hashtag to determine the **trending topics**.

#### Input/Output
- Input: Directory with all tweets data. All files will be processed.
- Output: Directory for the process output.

### TopNPattern
This class uses **TopNPattern** to get the **N** trending topics.

#### Input/Output
- Input: Output directory of **TrendingTopics** class.
- Output: Directory for the process output.
