# Data Engineer Evaluation #


## Application Setup ##
1. Create project directory `mkdir -p /tmp/evaluation`
2. Unzip and copy the source files to the project directory
    `cp ofac.jsonl /tmp/evaluation`
    `cp gbr.jsonl /tmp/evaluation`

## Build Application Artifact  ##

1. Pull repo down
2. install sbt
3. run `sbt assembly`
4. From the `scala-evaluation` directory, copy jar to project directory `cp target/scala-2.11/scala-evaluation-assembly-0.0.1-SNAPSHOT.jar /tmp/`

## Spark Submit Command ##
The spark submit command takes 3 positional parameters
1. ofac source file location
2. gbr source file location
3. final result sink path location

```
./bin/spark-submit \
--class com.sayari.eval.EvalRunner \
--master local[8] \
/tmp/evaluation/scala-evaluation-assembly-0.0.1-SNAPSHOT.jar /tmp/evaluation/ofac.jsonl /tmp/evaluation/gbr.jsonl /tmp/evaluation/final
```

You may need to clean the checkpoint directory:
`rm -rf /tmp/evaluation/checkpoint/*`

