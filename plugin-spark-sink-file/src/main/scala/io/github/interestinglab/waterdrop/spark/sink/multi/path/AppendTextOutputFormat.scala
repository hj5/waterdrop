package test.multipath

import java.io.IOException

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.mapred.TextOutputFormat.LineRecordWriter
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, RecordWriter, TextOutputFormat}
import org.apache.hadoop.util.Progressable


/**
 * 支持hdfsappend操作
 *
 * @author jian.huang
 * @version 4.2
 *          2021-03-09 21:26:27
 */
class AppendTextOutputFormat[K, V] extends TextOutputFormat[K, V] {

  @throws[IOException]
  override def getRecordWriter(ignored: FileSystem, job: JobConf, path: String, progress: Progressable): RecordWriter[K, V] = {

    val keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t")
    //task输出的临时数据文件路径：
    val tempFilePath = FileOutputFormat.getTaskOutputPath(job, path)
    val fs = tempFilePath.getFileSystem(job)
    val pathWithFileExtension = path
    //task输出的最终数据文件路径：
    val realFilePath = new Path(FileOutputFormat.getOutputPath(job), pathWithFileExtension)
    var fileOut: FSDataOutputStream = null
    if (fs.exists(realFilePath)) fileOut = fs.append(realFilePath, 4096, progress)
    else fileOut = fs.create(realFilePath, progress)
    new LineRecordWriter[K, V](fileOut, keyValueSeparator).asInstanceOf[RecordWriter[K, V]]
  }

}