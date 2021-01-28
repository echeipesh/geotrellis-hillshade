package workshop

import cats.implicits._
import com.monovore.decline._
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.vector.Extent
import geotrellis.spark._
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import workshop.data.TerrainTiles

import org.log4s._

object Main extends CommandApp(
  name = "geotrellis-hillshade",
  header = "GeoTrellis Spark: geotrellis-hillshade",
  main = {
    @transient val logger = getLogger
    val extentOpt = Opts.option[String]("extent", help = "The extent for which we'll read Terrain Tiles")
    val outputOpt = Opts.option[String]("outputPath", help = "The path of the output tiffs")
    val numPartitionsOpt = Opts.option[Int]("numPartitions", help = "The number of partitions to use").orNone

    (extentOpt, outputOpt, numPartitionsOpt).mapN { (extentString, output, numPartitions) =>
      import Spark.context

      try {
        val extent = Extent.fromString(extentString)
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

      } finally {
        Spark.session.stop()
      }
    }
  }
)
