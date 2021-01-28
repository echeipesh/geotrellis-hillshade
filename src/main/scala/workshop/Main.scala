package workshop

import cats.implicits._
import com.monovore.decline._
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.vector.Extent
import geotrellis.spark._
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import org.apache.spark.SparkContext
import workshop.data.TerrainTiles
import org.log4s._

object Main {
  @transient private[this] lazy val logger = getLogger

  private val extentOpt = Opts.option[String]("extent", help = "The extent for which we'll read Terrain Tiles")
  private val outputOpt = Opts.option[String]("outputPath", help = "The path of the output tiffs")
  private val partitionsOpt =  Opts.option[Int]("numPartitions", help = "The number of partitions to use").orNone

  private val command = Command(name = "tail", header = "Print the last few lines of one or more files.") {
    (extentOpt, outputOpt, partitionsOpt).tupled
  }

  def main(args: Array[String]): Unit = {
    command.parse(args, sys.env) match {
      case Left(help) =>
        System.err.println(help)
        sys.exit(1)

      case Right((e, o, p)) =>
        try {
          run(Extent.fromString(e), o, p)(Spark.context)
        } finally {
          Spark.session.stop()
        }
    }
  }

  def run(extent: Extent, output: String, numPartitions: Option[Int])(implicit sc: SparkContext): Unit = {
    val keys  = TerrainTiles.layout.mapTransform.keysForGeometry(extent.toPolygon)
    val preferedPartitionCount = Some(keys.size / 4)

    val layer = TerrainTiles.layerForKeys(keys, numPartitions.orElse(preferedPartitionCount))
    val buffered = layer.bufferTiles(bufferSize = 1)
    val hillshade = buffered.map { case (key, bt) =>
      key -> bt.tile.hillshade(TerrainTiles.layout.cellSize, bounds = Some(bt.targetArea))
    }

    hillshade.foreach { case (key, hill) =>
      val tif = GeoTiff(hill, key.extent(TerrainTiles.layout), LatLng)
      val fileName = s"hillshade-buffered_${key.col}-${key.row}.tif"
      val path = s"$output/$fileName"
      logger.info(s"Writing: $path")

      if (output.startsWith("s3:")) {
        Util.writeToS3(S3ClientProducer.get(),
          uri = new AmazonS3URI(path),
          tif.toCloudOptimizedByteArray)
      } else {
        tif.write(path)
      }
    }
  }
}
