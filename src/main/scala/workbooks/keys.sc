// place to experiment with code
import geotrellis.layer.KeyBounds
import geotrellis.vector._
import workshop.data.TerrainTiles

val paExtent = Extent(-78.22073164,40.85762673,-77.75926491,41.12019965)
paExtent.toPolygon().toGeoJson
val keys  = TerrainTiles.layout.mapTransform.keysForGeometry(paExtent.toPolygon)

val readExtent = keys.map(_.extent(TerrainTiles.layout)).reduce(_ combine _)
val readBounds = keys.map( k=> KeyBounds(k, k)).reduce( _ combine _)

keys.foreach { key =>
  println(s"Key: $key")
  val raster = TerrainTiles.getRasterSource(key).read().get
  println(raster.tile.band(0).findMinMax)

}
