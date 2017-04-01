# simple-spark-jobserver    

**Pre-requisites before running simple-spark-jobserver:**    
1) spark should be installed on the system and spark-submit command should be working on the shell    
2) activator should be installed    

**Run simple-spark-jobserver:**    
In order to start the simple-spark-jobserver, clone the repository and run the following commands within the simple-spark-jobserver folder:    
```
  cd jobs
  sbt package
  cd ..
  activator run
```
After the play application starts, send a POST request to the url `/jar/execute` with the following content:
```json
  {
    "jar-path":"jobs/target/scala-2.10/simple-jobs_2.10-1.0-SNAPSHOT.jar",
    "job-name":"SampleJob",
    "master":"",
    "deploy-mode":"",
    "parameters":{"string":"sample","array":[1,2,3,4]}
  }
```

Internally, a spark-submit command executes on the machine which looks something likes this:
```
  spark-submit --class SampleJob jobs/target/scala-2.10/simple-jobs_2.10-1.0-SNAPSHOT.jar 10 {"string":"sample","array":[1,2,3,4]}
```

If the master is set to yarn and the deploy-mode is set to cluster, the spark-submit command will look something like this:   
```
  spark-submit --class SampleJob --deploy-mode cluster --master yarn jobs/target/scala-2.10/simple-jobs_2.10-1.0-SNAPSHOT.jar 10 {"string":"sample","array":[1,2,3,4]}
```

**Note:**   
Please ensure that the version of the spark dependencies defined in the build.sbt file for the jobs needs to be same as the version of the spark cluster against which spark-submit command is getting executed.
