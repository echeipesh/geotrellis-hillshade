package workshop.data

import geotrellis.layer.{KeyBounds, LayoutDefinition, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.{RasterSource, ShortCellType, TileLayout}
import geotrellis.spark.{ContextRDD, TileLayerRDD}
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import geotrellis.vector.{Extent, Geometry}
import org.apache.spark.SparkContext
import org.log4s._
/** Mapzen Terrain Tiles
 * @see https://registry.opendata.aws/terrain-tiles
 */
object TerrainTiles {
  @transient lazy val logger = getLogger

  val layout: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(layoutCols = 360, layoutRows = 180, tileCols = 3601, tileRows = 3601)
    LayoutDefinition(worldExtent, tileLayout)
  }

  /** Skadi raster for each a 1x1 degree tile */
  def tileUri(key: SpatialKey): String = {
    val col = key.col - 180
    val long = if (col >= 0) f"E${col}%03d" else f"W${-col}%03d"

    val row = 89 - key.row
    val lat = if (row >= 0) f"N${row}%02d" else f"S${-row}%02d"

    f"s3://elevation-tiles-prod/v2/skadi/$lat/$lat$long.hgt.gz"
  }

  def getRasterSource(tileKey: SpatialKey): RasterSource = GDALRasterSource(tileUri(tileKey))

  def getRasterSource(tileKeys: Set[SpatialKey]): Seq[RasterSource] = tileKeys.toSeq.map({ key => GDALRasterSource(tileUri(key)) })

  def getRasterSource(geom: Geometry): Seq[RasterSource] = {
    val tileKeys = layout.mapTransform.keysForGeometry(geom)
    tileKeys.toSeq.map({ key => GDALRasterSource(tileUri(key)) })
  }

  def layerForKeys(
    keys: Set[SpatialKey],
    numPartitions: Option[Int] = None
  )(implicit sc: SparkContext): TileLayerRDD[SpatialKey] = {
    val partitions = numPartitions.getOrElse(sc.defaultParallelism)
    val rdd = sc.parallelize(keys.toSeq, partitions)

    val liveKeysRdd = rdd.flatMap { key =>
      val client = S3ClientProducer.get()
      val uri = new AmazonS3URI(tileUri(key))
      val keyExists = workshop.Util.listS3Prefix(client, uri.getBucket, uri.getKey).nonEmpty
      if (keyExists) Some(key) else None
    }

    val tilesRdd = liveKeysRdd.map { key =>
      logger.info(s"Reading $key from ${TerrainTiles.tileUri(key)}")
      val raster = TerrainTiles.getRasterSource(key).read().get
      key -> raster.tile.band(0)
    }

    val readBounds = keys.map{ k => KeyBounds(k, k) }.reduce( _ combine _)
    val layerExtent = layout.mapTransform.boundsToExtent(readBounds.toGridBounds())

    val md = TileLayerMetadata(
      cellType = ShortCellType,
      layout = TerrainTiles.layout,
      extent = layerExtent,
      crs = LatLng,
      bounds = readBounds
    )

    ContextRDD(tilesRdd, md)
  }
}
