# hdfs-debug-util

###HDFS file copy utility, Several options for move file from Local file system to HDFS.

1) No of thread to move files

2) Wait time for each file move

3) Specify the protocol to move file (hdfs:// or webhdfs://).
   default is hdfs://
   
4) For simulation, It can copy the same file to HDFS and rename it by appending the filename with number(1...n)
   eg: file.java it copies to,
   
                     file.java
                     
                     file.java1
                     
                     file.java2
                     ...
                     ...
                     
                   

###Usage : 
  
  java -cp `hadoop classpath` com.hdfs.util.FileUtility 
  
  --src \<source dir\> 
  
  --dest \<destination dir\>  [--increment \<copy the same file with increment\>]
  
  [--op-thread \<no of thread to perform the operation,default 1\>] 
  
  [--op-type \<webhdfs://\<namenode\>\> | <hdfs://<namenode:port\> , default hdfs protocol]
  
  [--op-conf \<specify the conf directory\>] 
  
  [--op-wait \<Specify the operation/transaction wait time in millisecond,default 0ms\>]  
  
  

