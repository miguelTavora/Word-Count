To run the code it has to have the input and output on the following directories:
- examples\input
- examples\output

Inside each directory it must have the following directories (with files):
- examples\input\gutenberg
- examples\input\gutenberg-mixed
- examples\input\gutenberg-small
- examples\input\gutenberg-zip
- examples\input\wikipedia

The same applies to the HDFS, must have the following directories:
- /user/usermr/input
- /user/usermr/output
- The directories must follow the same as the local (gutenberg ...)


To change that it must be on the run2.sh file


To run the code use command:
- bash run2.sh <type of filesystem>
- <type of filesystem> can be: local || HDFS

To change the input file:
- go to the run2.sh and change the variable: CORPUS_NAME
- don't add any space on the equal


To change the properties of the MapReduce:
- Go to file: configuration-mapreduce.xml
- change the value to the wanted one

To change the properties of the word count:
- Go to file: configuration-wordcount
- change the value to the wanted one
