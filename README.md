The repo contains collection of  custom I/O Format, File Format, log processing, ipLookup, secondary sort and custom patitioner.

**Packaging :** From the root of the project run:
```
mvn package
```
 The package contains the list of the following programs:

**1. Average Word Count**
The program uses the map output record counter to implement the average word count.The MAP_OUTPUT_RECORDS gives the total number of records emitted from mapper which is used by reducer to find average.

```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.averageworcount.WordCountDriver input_path output_path
```
 
**2.  Custom Input Format **
It reads the file with fixed width format.For simplicity the width of the field is encoded in the driver program. The class RecordReader and FileInputFormat is extended to implement the input format.The sample fixed width test-data is under the resources.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.custominputformat.FixedWidthColumnDriver input_path output_path
```

**3. Custom Log Input Format**
The log input format reads the three fields from the log file ipAddress, timestamp, and request page.It also extends the class Recordreader and FileInputFormat.The sample log file is under resources.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.customloginputformat.LogInputFormatDriver input_path output_path
```
**4. Custom Output Format**
It writes the output with fixed width as specified.Space is padded to unused byte.Extends OutputFormat and Recordwriter class to implement cutom output.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.customoutputformat.FixedWidthColumnOutputDriver input_path output_path
```
**5. Custom Log Output Format**
This writes the custom log to the output.Used the output from 3 as a input to the program.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.customlogoutputformat.LogOutputDriver output_from_3 output_path
```

**6. Custom Writable**
Implements the custom writable to read the pair-text input.Implements the
WritableComparable interface for pair input.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.customwritable.TextPairDriver input_path output_path
```

**7. IP LookUp**
The IP from the log file is parse to find the location of IP address. The Geolite2-City datbase from Maxmind [http://dev.maxmind.com/] is used for IP Lookup.The Maxmind API site contains more inforamtion on data base and the API. The database is distributed using distributed cache.Download the Geolite2-City database from [http://dev.maxmind.com/geoip/geoip2/geolite2/] and pass it to distributed cache.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.iplookup.GeoLocationLookupDriver -files Geolite2-City.mmdb input_path output_path
```
**8. Log Custom Writable**
Implemets the WritableComparable to process ipAddress, timestamp and requestPage for the log file. 
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.logcustomwritable input_path output_path
```
**9. Status Counter **
Simple program to count number times status code  appears in log file.Similar to WordCount program.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.statuscount.StatusCountDriver input_path output_path
```
**10. Status Code Count using Counter**
The map function uses dynamic counter to count the number of occurrence of status word. This program just implements the Mapper, since the counter in the mapper is incremented each time the status code appears in the log file. The output is not written to file, instead shown in the console.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.logprocessingbycounter.LogProcessCounterDriver input_path 
```
**11. Custom Partitioner**
The custom partitoner class is written so that the log with particular status code always ends in same reducer.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.custompartitioner.LogPartitionerDriver input_path output_path
``` 
**12. Secondary Sort **
The MapReduce framework sorts the records by the key before they reach the reducers but for any paricular key, however, the values are not sorted.
In some scenario, we need the value to be sorted and sometime sorting the values eliminates the extra work also. This program uses the secondary sort on ipAddress as key and timestamp as value from the log file.
```
hadoop jar target/collections-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudwick.mapreduce.secondarysort.SecondarySortDriver input_path output_path
``` 


