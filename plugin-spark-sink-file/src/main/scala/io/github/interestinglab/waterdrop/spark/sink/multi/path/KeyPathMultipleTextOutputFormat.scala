package io.github.interestinglab.waterdrop.spark.sink.multi.path

/**
 *
 * @author jian.huang
 * @version 5.3
 *          2021/3/9
 */
import java.io.IOException
import java.util.UUID

import io.github.interestinglab.waterdrop.spark.sink.multi.path.KeyPathMultipleTextOutputFormat.fileSuffix
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.util.Progressable
import test.multipath.AppendTextOutputFormat

class KeyPathMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  private var theTextOutputFormat: TextOutputFormat[Any, Any] = null

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {

    key + "/" + fileSuffix

  }
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override protected def getBaseRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[Any, Any] = {
    if (theTextOutputFormat == null) theTextOutputFormat = new AppendTextOutputFormat[Any, Any]
    theTextOutputFormat.getRecordWriter(fs, job, name, arg3)
  }

  @throws[FileAlreadyExistsException]
  @throws[InvalidJobConfException]
  @throws[IOException]
  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = { // Ensure that the output directory is set and not already there
    var outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null && job.getNumReduceTasks != 0) throw new InvalidJobConfException("Output directory not set in JobConf.")
    if (outDir != null) {
      val fs = outDir.getFileSystem(job)
      // normalize the output directory
      outDir = fs.makeQualified(outDir)
      FileOutputFormat.setOutputPath(job, outDir)
      // get delegation token for the outDir's file system
      TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job)
    }
  }
}

object KeyPathMultipleTextOutputFormat{
  val fileSuffix: String = UUID.randomUUID().toString()
}


