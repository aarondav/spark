package org.apache.spark.sql

/**
 * Created by aaron on 8/3/14.
 */
object HadoopDirectory {

  /**
   * Creates a HadoopDirectory object that contains a fully-qualified path. This method also ensures
   * that the given directory is writable by Spark, and, optionally, whether is already contains
   * files.
   */
  def createChecked(
      pathString: String,
      conf: Configuration,
      allowExisting: Boolean = false): HadoopDirectory = {
    val path = new Path(pathString)
    if (path == null) {
      throw new IllegalArgumentException("Unable to create ParquetRelation: path is null")
    }
    val fs = path.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to create ParquetRelation: incorrectly formatted path $path")
    }

    if (!allowExisting && fs.exists(path) && fs.listStatus(path).nonEmpty) {
      sys.error(s"File $path already exists and is not empty.")
    }

    if (fs.exists(path) &&
      !fs.getFileStatus(path)
        .getPermission
        .getUserAction
        .implies(FsAction.READ_WRITE)) {
      throw new IOException(
        s"Unable to create ParquetRelation: path $path not read-writable")
    }
    new HadoopDirectory(fs.makeQualified(path))
  }
}

/**
 * Represents the location of a Parquet table that exists within a single Hadoop directory.
 */
class HadoopDirectory (path: String) extends TableLocation with HadoopDirectoryLike {
  def this(path: Path) = this(path.toString)

  def asString = path
  def asPath = new Path(path)
  def asHadoopDirectory = this

  override def equals(other: Any) = other match {
    case o: HadoopDirectory =>
      o.asString == asString
    case o: HadoopDirectoryLike =>
      o.asHadoopDirectory.asString == asString
    case _ => false
  }

  override def toString = asString
}