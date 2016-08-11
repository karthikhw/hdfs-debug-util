package com.hdfs.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtility {


    private static Configuration conf = null;
    private static final String DEFAULT = "/etc/hadoop/conf/";
    private static String OPTHREAD="op-thread";
    private static String OPTYPE="op-type";
    private static String OPCONF="op-conf";
    private static String OPWAIT="op-wait";
    private static String FILEINCREMENTAL="increment";
    private static String SRC="src";
    private static String DEST="dest";
    private static int threads=1;
    private static String protocol="hdfs://localhost:8020/";
    private static long waittime=0l;
    private static int increment=1;

    
    //Setting the user config
        public void setConf(CommandLine cmdline) {
                if(cmdline.hasOption("c")){
                        File file = new File(cmdline.getOptionValue(OPCONF));
                        if(file.isDirectory()){
                                setHadoopConf(file.getName());
                        }else{
                                System.out.println("Please check the conf directory");
                                System.exit(-1);
                        }
                }
                if (!cmdline.hasOption("t")) {
                        setHadoopConf(DEFAULT);
                }
                if (cmdline.hasOption("T")) {
                        threads=Integer.parseInt(cmdline.getOptionValue(OPTHREAD));
                }

                if (cmdline.hasOption("w")) {
                        waittime=Long.parseLong(cmdline.getOptionValue(OPWAIT));
                }

                if (cmdline.hasOption("i")) {
                        increment=Integer.parseInt(cmdline.getOptionValue(FILEINCREMENTAL));
                }
                if (!cmdline.hasOption("s")) {
                    
            }

        }

        //Adding the default configuration
        public static void setHadoopConf(String path){
                conf.addResource(path+"/core-site.xml");
                conf.addResource(path+"/hdfs-site.xml");
        }


        //Get the list of files to copy
        public static File[] getFileList(String src){
                File file = new File(src);
                if(file.exists()){
                        return file.listFiles();
                }else{
                        System.out.println("Source directory not exist.");
                        System.exit(-1);
                }
                return null;
        }

        
        public void usage(Options options) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(
                            "java -cp `hadoop classpath` com.hdfs.util.FileUtility [--op-thread <no of thread to perform the operation,default 1>] [--op-type <webhdfs://<namenode>/> | <hdfs://<namenode:port/> > >, default hdfs protocol] [--op-conf <specify the conf directory>] [--op-wait <Specify the operation/transaction wait time in millisecond,default 0ms>]  --src <source dir> --dest=<destination dir>  [--increment <copy the same file with increment>] >>   2>/tmp/_progress.log\n",
                            options);
            System.exit(-1);
    }
        
        //validating the user input
        public void validate(CommandLine cmdline,Options options){
        	if(!cmdline.hasOption("s") || !cmdline.hasOption("d")){
        		usage(options);
        	}
        }


         //Worker thread
       static  class WorkerThread implements Runnable{

            private int index = -1;
            private String src = "";
            private String dest = "";
            private FileSystem fs = null;
            public WorkerThread(int index,String src,String dest){
                    this.index=index;
                    this.src=src;
                    this.dest=dest;
            }

            public void run() {
                    try {
                    	
                            fs= FileSystem.get(new URI(protocol), conf);
                            for(File file:getFileList(src)){
                                    int hashcode = file.getAbsolutePath().hashCode();
                                    if(Math.abs(hashcode) % threads == index){
                                            //one copy of the source file to the destination directory.
                                            if(increment==1){
                                                    fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(dest+"/"+file.getName()));
                                            }else{
                                                    //Copy the same file to the destination directory Which name gets append the same till the increment count.
                                                    for (int i = 0; i <=increment; i++) {
                                                            fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(dest+"/"+file.getName()+i));
                                                            Thread.sleep(waittime);
                                                    }
                                            }
                                            Thread.sleep(waittime);
                }
                            }
                    } catch (IOException | URISyntaxException | InterruptedException e) {
                            e.printStackTrace();
                    }

            }

    }

       
    public static void main(String[] args) {
            Options options = new Options();
            FileUtility status = new FileUtility();
            try {
            	 
                    CommandLineParser parser = new GnuParser();
                    options.addOption("T", OPTHREAD, true, "No of thread to perform the opration, default is 1");
                    options.addOption("t", OPTYPE, true, "Operation type wehbhfs or hdfs");
                    options.addOption("c", OPCONF, true, "Specify the conf directory");
                    options.addOption("w", OPWAIT, true, "Specify the operation wait time in millisecond,default 0ms");
                    options.addOption("i", FILEINCREMENTAL, true, "Copy the same file with increment");
                    options.addOption("s", SRC, true, "Specify the source directory");
                    options.addOption("d", DEST, true, "Specify the destination directory");
                    CommandLine line = parser.parse(options, args);
                    conf = new Configuration();
                    status.validate(line, options);
                    status.setConf(line);
                    if(line.hasOption("t")){
                            protocol=line.getOptionValue("t");
                    }else{
                            protocol=conf.get("fs.defaultFS");
                    }
                    Thread[] workers = new Thread[threads];
                    for (int i = 0; i < workers.length; i++) {
                            workers[i] = new Thread(new WorkerThread(i,line.getOptionValue(SRC),line.getOptionValue(DEST)));
                            workers[i].start();
                    }
            } catch (ParseException e) {
            	status.usage(options);
            } catch (Exception e) {
                    System.out.println(e.getMessage());
                    System.exit(-1);
            }
    }
}