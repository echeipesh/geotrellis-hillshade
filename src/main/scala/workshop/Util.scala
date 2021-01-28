package workshop

import geotrellis.store.s3.AmazonS3URI
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, PutObjectResponse, S3Object}
import software.amazon.awssdk.core.sync.RequestBody

object Util {

  def writeToS3(client: S3Client, uri: AmazonS3URI, bytes: Array[Byte]): PutObjectResponse = {
    val objectRequest = PutObjectRequest.builder.bucket(uri.getBucket).key(uri.getKey).build
    client.putObject(objectRequest, RequestBody.fromBytes(bytes))
  }

  def listS3Prefix(client: S3Client, bucket: String, prefix: String): Iterable[S3Object] = {
    import software.amazon.awssdk.services.s3.model._
    import scala.collection.JavaConverters._

    val req = ListObjectsV2Request.builder.bucket(bucket).prefix(prefix).build
    client.listObjectsV2Paginator(req).asScala.flatMap { res =>
      res.contents().asScala
    }
  }
}
