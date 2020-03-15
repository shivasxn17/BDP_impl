Taken Sample of 50 files - europar_en_sample.zip

Dataset location - /proj_2_ds_sample_files/proj_2_dataset_sample/

Put dataset using these HDFS commands:

        hdfs dfs -mkdir /proj_2_ds_sample_files
        hdfs dfs -put proj_2_dataset_sample /proj_2_ds_sample_files/
  To check dataset copied properly:
        hdfs dfs -ls /proj_2_ds_sample_files/proj_2_dataset_sample/

------------------------------------------------------------------------------------------
All Job Execution outputs are in - Job_Outputs_All_tasks folder
All Jobs run console outputs are in - Console_Outputs_All_tasks folder

------------------------------------------------------------------------------------------

All 7 tasks Utility Jar - hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar

-------------------------------------------------------------------------------------------

Except task 3 which involves openNLP toolkit it cant be run with jar without dependencies.

Jar without packaged dependecies - hadoop_mr_tasks-0.0.1.jar

========================================================================================

******************************* Execution of Tasks: ************************************

Jar requires commandline 3 arguments:
        
        Arguments:
                1. Task number to be executed
                        values - 1,2,3,4,5,6,7
                2. HDFS Input location
                        In this case - proj_2_ds_sample_files/proj_2_dataset_sample/*
                3. HDFS Output location
                        In this case- /final_task_1_wc/output/


Sample Command to execute , for example to execute task 1:

hadoop jar hadoop_mr_tasks-0.0.1.jar WordCountDriver 1 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_1_with_counter/output

with hadoop_mr_tasks-0.0.1-jar-with-dependencies: 

hadoop jar hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar WordCountDriver 1 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_1_with_counter/output


To view the ouput of the job:
        hadoop fs -cat /final_task_1_wc/output/part-r-00000 >> p2_task1_op.txt


Main Java File:
        /hadoop_mr_tasks/src/main/java/WordCountDriver.java

----------------------------------------------------------------------------------------
TASK 1 :

hadoop jar hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar WordCountDriver 1 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_1_with_counter/output

Answer:

com.bdp.mapreduce.distinct.reducer.WordCountReducer$Counters
        UniqueTerms=66282

Java Files:
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/mapper/WordCountMapper.java
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/reducer/WordCountReducer.java


----------------------------------------------------------------------------------------
TASK 2:

hadoop jar hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar WordCountDriver 2 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_2/output

After stemming -

hadoop jar hadoop_mr_tasks-0.0.1-SNAPSHOT-jar-with-dependencies.jar WordCountDriver 2 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_2_stemmer_op/output


Java Files:
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/mapper/TextPreprocesMapper.java
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/reducer/WordCountReducer.java

-------------------------------------------------------------------------------------------
TASK 3:

hadoop jar hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar WordCountDriver 3 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_3/output

Answer:

com.bdp.mapreduce.distinct.reducer.LessThanFourReducer$ReducerCounters
                LessThanFour=9717

Java Files:
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/mapper/TextPreprocesMapper.java
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/reducer/LessThanFourReducer.java

------------------------------------------------------------------------------------------
TASK 4:

hadoop jar hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar WordCountDriver 4 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_4/output

me/my/mine/I  - 36331
us/our/ours/we  - 45036

Answer:

com.bdp.mapreduce.distinct.reducer.ProNounCounterReducer$ReducerCounters
                PLURAL=45036
                SINGULAR=36331

Java Files:
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/mapper/ProNounCounterMapper.java
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/reducer/ProNounCounterReducer.java

-----------------------------------------------------------------------------------------------
TASK 5:

hadoop jar hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar WordCountDriver 5 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_5_1/output

Answer:

 com.bdp.mapreduce.distinct.reducer.DocFrequencyReducer$Counters
                AppearedInOneDocument=11739

Java Files:
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/mapper/DocFrequencyMapper.java
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/reducer/DocFrequencyReducer.java

------------------------------------------------------------------------------------------------
TASK 6:

hadoop jar hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar WordCountDriver 6 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_6/output

Java Files:
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/mapper/NoStopWordsMapper.java
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/reducer/WordCountReducer.java

Stop words list used - 
        smart.txt

------------------------------------------------------------------------------------------------
TASK 7:

hadoop jar hadoop_mr_tasks-0.0.1-jar-with-dependencies.jar WordCountDriver 7 /proj_2_ds_sample_files/proj_2_dataset_sample /new_op_7/output

Java Files:
        /hadoop_mr_tasks/src/main/java/com/bdp/mapreduce/distinct/mapper/PoliticalConceptMapper.java
