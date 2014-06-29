hadoop
======

Master repository for hadoop jobs

### hadoop-job1
This is the hadoop classic WordCount example, created with Matthias Friedrich's maven archetype for hadoop jobs.
This project was created by following [this][2] blog, go there for details. Thanks Matthias.

To run copy to the cluster and execute:
```
hadoop jar hadoop-job1.jar org.javasucks.hadoop.WordCount /falcon/demo/bcp/processed/enron/2014-02-28-00 /user/hue/wordcount
```


[2]: http://blog.mafr.de/2010/08/01/maven-archetype-hadoop/
