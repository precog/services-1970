package com.reportgrid.analytics


import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import org.joda.time.DateTime

/**
 * Extractors and decomposers for types where the same serialization format is shared
 * beween the service API and MongoDB
 */
trait AnalyticsSerialization {
  final implicit val JPathDecomposer: Decomposer[JPath] = new Decomposer[JPath] {
    def decompose(v: JPath): JValue = v.toString.serialize
  }

  final implicit val JPathExtractor: Extractor[JPath] = new Extractor[JPath] {
    def extract(v: JValue): JPath = JPath(v.deserialize[String])
  }

  final implicit val JPathNodeDecomposer: Decomposer[JPathNode] = new Decomposer[JPathNode] {
    def decompose(v: JPathNode): JValue = v.toString.serialize
  }

  final implicit val JPathNodeExtractor: Extractor[JPathNode] = new Extractor[JPathNode] {
    def extract(v: JValue): JPathNode = {
      JPath(v.deserialize[String]).nodes match {
        case node :: Nil => node
        case _ => sys.error("Too many or few nodes to extract JPath node from " + v)
      }
    }
  }

  final implicit val PeriodicityDecomposer = new Decomposer[Periodicity] {
    def decompose(periodicity: Periodicity): JValue = periodicity.name
  }

  final implicit val PeriodicityExtractor = new Extractor[Periodicity] {
    def extract(value: JValue): Periodicity = Periodicity(value.deserialize[String])
  }

  final implicit val HasChildDecomposer = new Decomposer[HasChild] {
    def decompose(v: HasChild): JValue = v.child.serialize
  }

  final implicit val HasChildExtractor = new Extractor[HasChild] {
    def extract(v: JValue): HasChild = HasChild(v.deserialize[JPathNode])
  }

  final implicit val HasValueDecomposer = new Decomposer[HasValue] {
    def decompose(v: HasValue): JValue = v.value
  }

  final implicit val HasValueExtractor = new Extractor[HasValue] {
    def extract(v: JValue): HasValue = HasValue(v)
  }

  final implicit val LimitsDecomposer = new Decomposer[Limits] {
    def decompose(limits: Limits): JValue = JObject(
      JField("order", limits.order.serialize) ::
      JField("limit", limits.limit.serialize) ::
      JField("depth", limits.depth.serialize) ::
      Nil
    )
  }

  final implicit val LimitsExtractor = new Extractor[Limits] {
    def extract(jvalue: JValue): Limits = Limits(
      order = (jvalue \ "order").deserialize[Int],
      limit = (jvalue \ "limit").deserialize[Int],
      depth = (jvalue \ "depth").deserialize[Int]
    )
  }

  final implicit val PathDecomposer = new Decomposer[Path] {
    def decompose(v: Path): JValue = JString(v.toString)
  }

  final implicit val PathExtractor = new Extractor[Path] {
    def extract(v: JValue): Path = Path(v.deserialize[String])
  }

  final implicit val PermissionsDecomposer = new Decomposer[Permissions] {
    def decompose(permissions: Permissions): JValue = JObject(
      JField("read",    permissions.read.serialize)  ::
      JField("write",   permissions.write.serialize) ::
      JField("share",   permissions.share.serialize) ::
      Nil
    )
  }

  final implicit val PermissionsExtractor = new Extractor[Permissions] {
    def extract(jvalue: JValue): Permissions = Permissions(
      read  = (jvalue \ "read").deserialize[Boolean],
      write = (jvalue \ "write").deserialize[Boolean],
      share = (jvalue \ "share").deserialize[Boolean]
    )
  }

  final implicit val TokenDecomposer = new Decomposer[Token] {
    def decompose(token: Token): JValue = JObject(
      JField("tokenId",         token.tokenId.serialize)  ::
      JField("parentTokenId",   token.parentTokenId.serialize) ::
      JField("accountTokenId",  token.accountTokenId.serialize) ::
      JField("path",            token.path.serialize) ::
      JField("permissions",     token.permissions.serialize) ::
      JField("expires",         token.expires.serialize) ::
      JField("limits",          token.limits.serialize) ::
      Nil
    )
  }

  final implicit val TokenExtractor = new Extractor[Token] {
    def extract(jvalue: JValue): Token = Token(
      tokenId         = (jvalue \ "tokenId").deserialize[String],
      parentTokenId   = (jvalue \ "parentTokenId").deserialize[Option[String]],
      accountTokenId  = (jvalue \ "accountTokenId").deserialize[String],
      path            = (jvalue \ "path").deserialize[Path],
      permissions     = (jvalue \ "permissions").deserialize[Permissions],
      expires         = (jvalue \ "expires").deserialize[DateTime],
      limits          = (jvalue \ "limits").deserialize[Limits]
    )
  }
}

// vim: set ts=4 sw=4 et:
