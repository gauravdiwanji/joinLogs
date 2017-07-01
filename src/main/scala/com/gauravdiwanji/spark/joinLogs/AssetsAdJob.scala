package main.scala.com.gauravdiwanji.spark.joinLogs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.regex.Matcher
import org.apache.spark.sql.SQLContext
import java.util.NoSuchElementException
import org.apache.spark.sql.DataFrame

class JoinLogsJob(sc : SparkContext) {

def run(assetLoc: String, adLoc: String) : DataFrame = {

//Define SQL Context    
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//Load log file into RDD    
val assetRaw = sc.textFile(assetLoc)
val adRaw = sc.textFile(adLoc)

//Regex to extract the json string from a record line
val pattern = """\w{3} \w{3} \d{1,2} \d{2}:\d{2}:\d{2} .{4,5} \d{4}, \{.*?\}, (.*?)""".r
    
//To extract out only the needed json from a record line, used in map
val parseRecord = (record: String) => {
    try {
        record match {
        case pattern(rem) => s"$rem"
        }
    }
    catch {
       case e:Exception => null
    }
}

//RDD to DataFrame: Extract needed json from each record, using parseRecord function ==> Filter out unmatched records,if any ==> generate a DataFrame from json record
val asset_df = sqlContext.read.json(assetRaw.map(parseRecord).filter(x => x != null))
val ad_df = sqlContext.read.json(adRaw.map(parseRecord).filter(x => x != null))

//Register datasets as a temporary views, to facilitate SQL queries
asset_df.createOrReplaceTempView("assets")
ad_df.createOrReplaceTempView("ad")

//Group assets by pv, to get asset impressions
val assets_grouped_df = sqlContext.sql("""select a.pv, count(*) as asset_impressions from assets a group by a.pv""")

//SQL Query for e in click or view, group ads by pv, and count the number of views and clicks, 
val ad_grouped_df = sqlContext.sql("""select pv, sum(case UPPER(e) when "CLICK" then 1 else 0 end) count_click, sum(case UPPER(e) when "VIEW" then 1 else 0 end) view_click from ad where UPPER(e) in ("VIEW","CLICK") group by pv""")

//Register temporary views on previous query results
assets_grouped_df.createOrReplaceTempView("assets_g")
ad_grouped_df.createOrReplaceTempView("ad_g")

//Outer join on previous table, so as to get the entire grouped assets table, and the needed columns, show nulls as 0
val result_df = sqlContext.sql("""select a.pv page_view_id, a.asset_impressions, nvl(b.view_click,0) views, nvl(b.count_click,0) clicks from assets_g a LEFT JOIN ad_g b ON a.pv = b.pv""")

//Return DataFrame
return result_df

}
}

object AssetsAdJob{

def main(args: Array[String]) {
      
// create Spark context
val sc = new SparkContext(new SparkConf().setAppName("assetsAdJob"))

//String to store input file location
var assetsLoc : String = null
var adLoc : String = null

//In case of incorrect or missing config file, this method can be used to supply a deafault input file, or fail the job as done here.
def defaultConfigErrorBehaviour() {
sc.stop
println("[ERROR]        :Please check your config.properties file for incorrect/missing values")
System.exit(1)
}

//Method to set the input file location from config file
def setInputPaths(scheme : String) {
val inp_assetsFile = sc.getConf.get("spark.assetsFile")
val inp_adFile = sc.getConf.get("spark.adFile")
assetsLoc = scheme+"://"+inp_assetsFile 
adLoc = scheme+"://"+inp_adFile
println( "[MESSAGE]     :Reading from "+scheme+"...")
}

//Input file path based on config
try {
    val inp_inputfiletype = sc.getConf.get("spark.inputfiletype")

    if(inp_inputfiletype.toUpperCase == "FILE") {
        try{
        setInputPaths("file")
        } catch {
            case ex: NoSuchElementException => {
            defaultConfigErrorBehaviour()
            println( "[MESSAGE]     :Error reading spark.assetsFile or spark.adFile property.")
            }
        }
    } else if (inp_inputfiletype.toUpperCase == "S3") {
        try{
        setInputPaths("s3n")
        val inp_awsAccessKeyId = sc.getConf.get("spark.awsAccessKeyId")
        val inp_awsSecretAccessKey = sc.getConf.get("spark.awsSecretAccessKey") 
        sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", inp_awsAccessKeyId)
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", inp_awsSecretAccessKey)
        } catch {
            case ex: NoSuchElementException => {
            defaultConfigErrorBehaviour()
            println( "[MESSAGE]     :Error reading either of [spark.assetsFile, spark.adFile, spark.awsAccessKeyId, spark.awsSecretAccessKey] property.")
            }
        }   
    } else {
    defaultConfigErrorBehaviour()
    println( "[MESSAGE]     :Incorrect value for spark.inputfiletype property. Possible values: [s3,file].")  
    }
} catch {
    case ex: NoSuchElementException => {
    defaultConfigErrorBehaviour()
    println( "[MESSAGE]     :Error reading spark.inputfiletype property")
    }
}

println("[MESSAGE]      : Assets File Location => "+assetsLoc)
println("[MESSAGE]      : Ad File Location => "+adLoc)

//Instantiate class and run job
val job = new JoinLogsJob(sc)
val result_df = job.run(assetsLoc, adLoc)
  
//Get output file path from config
var outputLocation : String = null
    
try{
outputLocation = sc.getConf.get("spark.outputFile")
} catch {
  case ex: NoSuchElementException => outputLocation = "/tmp/gauravJob"
}
    
//Write output to file
result_df.coalesce(1).write
    .mode("overwrite")
    .option("header", "true")
    .csv(outputLocation)

println( "[MESSAGE]     : Output file created at => " + outputLocation);

//Stop the Spark context
sc.stop

}
}
