import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import scala.util.Try

object HDFSPractice extends App {

  val conf = new Configuration

  val hdfsPath = "hdfs://localhost:9000"
  val fs = FileSystem.get(new URI(hdfsPath), conf)

  val stageDir: String = "/stage/"
  val stagePath = s"$hdfsPath$stageDir"

  val odsDir: String = "/ods/"
  val odsPath = s"$hdfsPath$odsDir"

  val res: Try[String] = Try {
  // Получение содержимого директории /stage/
  val stageContent: Array[FileStatus] = fs.listStatus(new Path(stagePath))

    // Получение путей для поддиректорий /stage/
    val stageDirList: List[String] =
      stageContent
        .toList
        .filter(_.isDirectory)
        .map(_.getPath.toString)

    // Получение мапы: ключ: дата, значение: список с путями к файлам для этой даты
    val stageFileList: Map[String, List[String]] =
      stageDirList
        .flatMap(path => fs.listStatus(new Path(path)))
        .filter(_.isFile)
        .map(_.getPath.toString)
        .filter(_.endsWith(".csv"))
        .map(_.drop("hdfs://localhost:9000/stage/".length))
        .groupBy(_.take("date=2020-12-01".length))

    // Получение путей для поддиректорий /stage/ , которые содержат файлы для обработки
    val processDirList: List[String] =
      stageFileList
        .keys
        .toList
        .map(el => s"$stagePath$el")

    // Получение путей для поддиректорий /stage/ , которые содержат только недописанные файлы / не содержат файлы
    val uselessDirs: List[String] = stageDirList.filter(x => !processDirList.contains(x))
    deleteFolders(uselessDirs)

    createFolder(odsPath)
    stageFileList.values.foreach(process)

    "Done"
  }

  def process(files: List[String]): Unit = {
    val dateDir = getDateDir(files)
    val ods = s"$odsPath$dateDir"
    val stage = s"$stagePath$dateDir"
    createFolder(ods)

    val odsFilePath = s"${ods}part-0000.csv"

    files.length match {
      // Если папка содержит всего 1 файл для обработки - просто меняем ему путь
      case x: Int if x == 1 =>
        fs.rename(new Path(s"$stage${getFileName(files)}"), new Path(odsFilePath))
      case x: Int if x > 1 =>
        mergeFiles(odsFilePath, files.map(el => s"$stagePath$el"))
    }

    deleteFolder(stage)
  }

  def mergeFiles(dest: String, source: List[String]): Unit = {
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

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fs.exists(path)) fs.mkdirs(path)
  }

  def getDateDir(fileList: List[String]): String = fileList.head.split("/")(0) + "/"

  def getFileName(fileList: List[String]): String = fileList.head.split("/")(1)

  def deleteFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (fs.exists(path)) fs.delete(path, true)
  }

  def deleteFolders(foldersPath: List[String]): Unit =
    foldersPath.map(new Path(_)).foreach(path => if (fs.exists(path)) fs.delete(path, true))

}
