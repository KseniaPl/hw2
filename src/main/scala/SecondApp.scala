import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}



object SecondApp extends App {
  val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  private val conf = new Configuration()
  //  изменяем policy, иначе был exception при работе с  date=2020-12-03
  conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  val fs = FileSystem.get(conf)
  val odsPath = "hdfs://namenode:9000/ods"
  val stagePath = "hdfs://namenode:9000/stage"
  fs.mkdirs(new Path(odsPath))
  //  обход файлов и папок
  dirWalking(new Path(stagePath), odsPath)

  //убедиться, что все на месте в ods
  checkResult(odsPath)
  //удалить stage
  fs.delete(new Path(stagePath), true)
  checkResult("hdfs://namenode:9000/")

  private def checkResult(hdfspath: String): Unit = {
    logger.info("---")
    logger.info("\n")
    logger.info("check result in " + hdfspath)
    val ods_status = fs.listStatus(new Path(hdfspath))
    ods_status.foreach(y => {
      logger.info(y.getPath.toString)
      if (y.isDirectory) {
        val st = fs.listStatus(y.getPath)
        st.foreach(x => logger.info(x.getPath.toString))
      }
    })
  }


  private def dirWalking(path: Path, odsPath: String): Unit = {
    val status = fs.listStatus(path)
    var first = true
    var firstFilePath = ""
    status.foreach(x => {
      if (x.isDirectory) {
        val path = x.getPath
        val newOdsPath = odsPath + "/" + path.getName
        createFolder(newOdsPath)

        dirWalking(x.getPath, newOdsPath)
      }
      if (x.isFile) {
        breakable {
          if (first) {
            if (x.getPath.getName == ".DS_Store") break
            firstFilePath = odsPath + '/' + x.getPath.getName
            copyFile(x.getPath, new Path(firstFilePath))
            first = false

          }
          else {
            appendFile(x.getPath, new Path(firstFilePath))
          }
        }
      }
    })

    def createFolder(folderPath: String): Unit = {
      val path = new Path(folderPath)
      if (!fs.exists(path)) {
        fs.mkdirs(path)
      }
    }

  }

  def copyFile(srcPath: Path, dstPath: Path): Unit = {
    val outFileStream = fs.create(dstPath)
    try {
      val inStream = fs.open(srcPath)
      try {
        logger.info("copy")
        logger.info(srcPath.toString)
        logger.info("\n")
        IOUtils.copy(inStream, outFileStream)
      }
      finally {
        inStream.close()
      }
    }
    finally {
      outFileStream.close()
    }
  }

  def appendFile(srcPath: Path, dstPath: Path): Unit = {
    logger.info("----")
    logger.info("\n")
    logger.info("append")
    logger.info(srcPath.toString)
    logger.info(dstPath.toString)
    val outFileStream = fs.append(dstPath)
    try {
      val inStream = fs.open(srcPath)
      try {
        IOUtils.copy(inStream, outFileStream)
      }
      finally {
        inStream.close()
      }
    }
    finally {
      outFileStream.close()
    }
    val inStream2 = fs.open(dstPath)
    try {
      def readLines = Stream.cons(inStream2.readLine, Stream.continually(inStream2.readLine))
      readLines.takeWhile(_ != null).foreach(line => logger.info(line))
    }
    finally {
      inStream2.close()
    }
    logger.info("----")
  }
}
