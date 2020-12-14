package _debug

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import java.net.URI

object Test extends App {

  val conf = new Configuration
  //  val hdfsCoreSitePath = new Path("core-site.xml")
  //  val hdfsHDFSSitePath = new Path("hdfs-site.xml")
  //  conf.addResource(hdfsCoreSitePath)
  //  conf.addResource(hdfsHDFSSitePath)

  val hdfsPath = "hdfs://localhost:9000"
  val fs = FileSystem.get(new URI(hdfsPath), conf)

  val stageDir: String = "/stage/"
  val stagePath = s"$hdfsPath$stageDir"

  val odsDir: String = "/ods/"
  val odsPath = s"$hdfsPath$odsDir"

  val contentEx1: Array[FileStatus] = fs.listStatus(new Path(s"$hdfsPath/"))
  //  println(contentEx1.toList)
  //  println(contentEx1.toList.map(_.getPath.toString))

  //  println(contentEx1.toList.map(_.getPath.toString))

  // Получение содержимого директории /stage/
  val stageContent: Array[FileStatus] = fs.listStatus(new Path(stagePath))

  // Получение путей для для поддиректорий /stage/
  val stageDirList: List[String] =
    stageContent
      .toList
      .filter(_.isDirectory)
      .map(_.getPath.toString)
  //  println(stageDirList)

  val stageFileList: Map[String, List[String]] =
    stageDirList
      .flatMap(path => fs.listStatus(new Path(path)))
      .filter(_.isFile)
      .map(_.getPath.toString)
      .filter(_.endsWith(".csv"))
      .map(_.drop("hdfs://localhost:9000/stage/".length))
      .groupBy(_.take("date=2020-12-01".length))

  val processDirList: List[String] =
    stageFileList
      .keys
      .toList
      .map(el => s"$stagePath$el")
  //  println(processDirList)

  val emptyDirs = stageDirList.filter(x => !processDirList.contains(x))
  //  println(emptyDirs)

  createFolder(odsPath)

  //  stageFileList.values.foreach(println)
  stageFileList.values.foreach(process)

  deleteFolders(emptyDirs)

  val contentEx2: Array[FileStatus] = fs.listStatus(new Path(odsPath))
  //  println(contentEx2.toList)

  //  println(s"${odsPath}date=2020-12-01/")
  val contentEx3: Array[FileStatus] = fs.listStatus(new Path(s"${odsPath}date=2020-12-03/"))
  println(contentEx3.toList)

  //  val contentEx4: Array[FileStatus] = fs.listStatus(new Path(s"${stagePath}date=2020-12-03/"))
  //  println(contentEx4.toList)

  def process(files: List[String]): Unit = {
    if (files.nonEmpty) {
      val dateDir = getDateDir(files)
      val ods = s"$odsPath$dateDir"
      val stage = s"$stagePath$dateDir"
      createFolder(ods)

      val odsFilePath = s"${ods}part-0000.csv"

      files.length match {
        case x: Int if x == 1 =>
          fs.rename(new Path(s"$stage${getFileName(files)}"), new Path(odsFilePath))
          deleteFolder(stage)
        case x: Int if x > 1 =>
          //          println(odsFilePath)
          //          println(files.map(el => s"$stagePath$el"))
          mergeFiles(odsFilePath, files.map(el => s"$stagePath$el"))
          deleteFolder(stage)
      }

    }
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fs.exists(path)) fs.mkdirs(path)
  }

  def getDateDir(fileList: List[String]): String = fileList.head.split("/")(0) + "/"

  def getFileName(fileList: List[String]): String = fileList.head.split("/")(1)

  def mergeFiles(dest: String, source: List[String]): Unit = {
    //    println(dest)
    val out: FSDataOutputStream = fs.create(new Path(dest))

    def appendFromFile(path: String): Unit = {
      val filepath = new Path(path)
      val file = fs.open(filepath)
      val in: InputStream = new BufferedInputStream(file)
      val b = new Array[Byte](1024)
      var numBytes = in.read(b)

      while (numBytes > 0) {
        out.write(b, 0, numBytes)
        numBytes = in.read(b)
      }

      in.close()
    }

    for { file <- source } appendFromFile(file)
    out.close()
  }

  def deleteFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (fs.exists(path)) fs.delete(path, true)
  }

  def deleteFolders(foldersPath: List[String]): Unit =
    foldersPath.map(new Path(_)).foreach(path => if (fs.exists(path)) fs.delete(path, true))

}

