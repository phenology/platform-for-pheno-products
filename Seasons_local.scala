import sys.process._
import java.io.File
import java.io.PrintWriter
import scala.collection.mutable.ListBuffer


object Seasons_local extends App {
  val dir = "/home/vik/Documents/mproject/TimeSatLocal/"
  var settingsName = "200_200_px_USA"
  val currentDirectory = new java.io.File(".").getCanonicalPath

  val scriptPath= dir + "TSF_process.x64"
  val mergePath = dir +"TSF_merge.x64"
  val sea2ImgPath = dir + "TSF_seas2img.x64"
  val jobName = "Final"
  val tpa_file_cluster_names =  new ListBuffer[String]()


  //define the whole processing window
  // Current : 2632 6621      %Image dimension (nrow ncol)
  val startRow = 1 //split tasks on rows only
  val endRow = 2632  //split tasks on rows only
  val starColumn = 1
  val endColumn = 6621
  val numberJobs = 3


  CreateSettigsFilesForEachNode()

  //Execute each partition
  //In Cluster mode this loop is to be replaced with some
  //framework for node job distribution
  var exitCode_merge_last = -1000
  for( i <- 1 to numberJobs) {


    //call the scala script and wait it to finish
    // TSF_merge must be in path !
    val settingsPath = dir + settingsName+"_cluster" + ".set"+i

    val exitCode_process = Seq(scriptPath, settingsPath, "7").!




    if (exitCode_process.equals(0)) {
      //the name of the job from settings file !
      tpa_file_cluster_names += "cluster"+ i +"_TS.tpa"
      val jobNamePath = currentDirectory + "/cluster"+ i + "_script.sh"

      ModifyParallelScript(jobNamePath)

      //     TSF_process must be in path !
      //     the settings files produces by above process must be in same dir of executing the script with merge
      //    merge requires the _TS.tpa files as input
      // merge back results from each CPU partition
      exitCode_merge_last = Seq(jobNamePath, settingsPath).!
    }
  }


  if (exitCode_merge_last.equals(0)) {

    //merge back results from each cluster partition
    // Arg list: 1:arglist, 2:number of merges, 2-n: the tpa file names; n+1: outTpa name
    val allMergeExitCode = (mergePath +" " +"1"+" " + numberJobs.toString + " " + tpa_file_cluster_names.mkString(" ") +" " +  jobName + "_TS.tpa").!

    val success = Seq(sea2ImgPath, jobName + "_TS.tpa", "1", "30", "82", "0", "1", "seasons", "2").!

    if (success.equals(0)){
      print ("Job done")
    }




  }





  def ModifyParallelScript(jobNamePath:String ) : Unit = {

    val f2 = new File(dir +"temp.sh") // Temporary File

    val lines = scala.io.Source.fromFile(jobNamePath).mkString.split("\\r?\\n")
    lines(27) = "  line=$(ps -A | grep TSF_process.x64)"


    new PrintWriter(f2){write(lines.mkString("\n"))}.close() //close file

    new File(jobNamePath).delete()
    new java.io.File(dir +"temp.sh") renameTo new java.io.File(jobNamePath)
    new File(jobNamePath).setExecutable(true)

  }

  def CreateSettigsFilesForEachNode() : Unit = {

    val rowRange = endRow - startRow
    var rowCounter = startRow

    val settingFile = new File(dir +settingsName + ".set") // Temporary File

    val lines = scala.io.Source.fromFile(settingFile).mkString.split("\\r?\\n")
    var endRowInc =startRow

    for( i <- 1 to numberJobs){
      if( i != numberJobs) {
        endRowInc = rowCounter + rowRange / numberJobs
      }
      else{
        // last task, compensate for the pixel increment and non-whole division
        endRowInc = (rowCounter + rowRange  / numberJobs) - numberJobs + 1 + (rowRange % numberJobs)
      }
      //change corresponding lines
      lines(1)= "cluster" + i +"      %Job_name (no blanks)" //change job name
      lines(10) = rowCounter.toString + " "+endRowInc.toString + " "+ starColumn.toString+" "+ endColumn.toString +"      %Processing window (start row end row start col end col)"

      rowCounter +=  rowRange /numberJobs +1

      println( "Settings file: " + i )
      println(lines(1).mkString)
      println(lines(10))

      val settingFileOut = new File(dir + settingsName+"_cluster" + ".set"+i) // Temporary File
      new PrintWriter(settingFileOut){write(lines.mkString("\n"))}.close() //close file

    }



  }

}