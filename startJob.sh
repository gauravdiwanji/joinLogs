#!/bin/sh
script_dir=$(dirname $0)

#To ensure proerty files is present
propertyFile=$script_dir"/config.properties"
if ! [ -f "$propertyFile" ]
then
	echo "Please place the jobPropertiesFile.properties file in the directory: "$(pwd)
    exit 1
fi

#To ensure spark Job jar file is present
jarFile=$script_dir"/GauravDiwanjiAssetsAdJob-assembly-1.0.jar"
if ! [ -f "$jarFile" ]
then
	echo "Please place the" $jarFile "in the directory: "$(pwd)
    exit 1
fi

#To get Spark environment for spark-submit
if [ -z "$SPARK_HOME" ]
then
    echo "Please set SPARK_HOME environment variable."
    exit 1
fi

#Submit Spark  Job
echo "Submitting Spark job..."
$SPARK_HOME/bin/spark-submit --class main.scala.com.gauravdiwanji.spark.joinLogs.AssetsAdJob --master local --deploy-mode client --properties-file  $propertyFile --name assetsAdJob --conf "spark.app.id=AssetsAdJob" $jarFile
exit 0

