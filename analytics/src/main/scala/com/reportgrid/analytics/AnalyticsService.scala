package com.reportgrid.analytics

import blueeyes.{BlueEyesServiceBuilder, BlueEyesServer}
import blueeyes.concurrent.Future
import blueeyes.core.data.{Chunk, BijectionsChunkJson, BijectionsChunkString}
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.{JPath, JsonParser, JPathField}
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.util.{Clock, ClockSystem}

import net.lag.configgy.{Configgy, ConfigMap}

import org.joda.time.Instant
import org.joda.time.DateTime

import java.util.concurrent.TimeUnit

import com.reportgrid.analytics.AggregatorImplicits._
import com.reportgrid.analytics.persistence.MongoSupport._
import com.reportgrid.blueeyes.ReportGridInstrumentation
import com.reportgrid.api.ReportGridConfig
import com.reportgrid.api.ReportGridTrackingClient
import com.reportgrid.api.blueeyes._

case class AnalyticsState(aggregationEngine: AggregationEngine, tokenManager: TokenManager, clock: Clock, auditClient: ReportGridTrackingClient[JValue])

trait AnalyticsService extends BlueEyesServiceBuilder with BijectionsChunkJson with BijectionsChunkString with ReportGridInstrumentation {
  import AggregationEngine._
  import AnalyticsService._

  def mongoFactory(configMap: ConfigMap): Mongo

  def auditClientFactory(configMap: ConfigMap): ReportGridTrackingClient[JValue] 

  val analyticsService = service("analytics", "0.01") {
    requestLogging { 
    logging { logger =>
      healthMonitor { monitor => context =>
        startup {
          import context._

          val mongoConfig = config.configMap("mongo")

          val mongo = mongoFactory(mongoConfig)

          val database = mongo.database(mongoConfig.getString("database").getOrElse("analytics"))

          val tokensCollection = mongoConfig.getString("tokensCollection").getOrElse("tokens")

          val auditClient = auditClientFactory(config.configMap("audit"))

          for {
            tokenManager      <- TokenManager(database, tokensCollection)
            aggregationEngine <- AggregationEngine(config, logger, database)
          } yield {
            AnalyticsState(aggregationEngine, tokenManager, ClockSystem.realtimeClock, auditClient)
          }
        } ->
        request { (state: AnalyticsState) =>
          import state._

          def renderHistogram(histogram: Traversable[(HasValue, Long)]): JObject = {
            histogram.foldLeft(JObject.empty) {
              case (content, (hasValue, count)) =>
                val name = JPathField(renderNormalized(hasValue.value))

                content.set(name, JInt(count)).asInstanceOf[JObject]
            }
          }

          def tokenOf(request: HttpRequest[_]): Future[Token] = {
            request.parameters.get('tokenId) match {
              case None =>
                throw HttpException(HttpStatusCodes.BadRequest, "A tokenId query parameter is required to access this URL")

              case Some(tokenId) =>
                tokenManager.lookup(tokenId).map { token =>
                  token match {
                    case None =>
                      throw HttpException(HttpStatusCodes.BadRequest, "The specified token does not exist")

                    case Some(token) =>
                      if (token.expired) throw HttpException(HttpStatusCodes.Unauthorized, "The specified token has expired")

                      token
                  }
                }
            }
          }

          def fullPathOf(token: Token, request: HttpRequest[_]): Path = {
            val prefixPath = request.parameters.get('prefixPath) match {
              case None | Some(null) => ""
              case Some(s) => s
            }

            token.path + "/" + prefixPath
          }

          def variableOf(request: HttpRequest[_]): Variable = Variable(JPath(request.parameters.get('variable).getOrElse("")))

          def valueOf(request: HttpRequest[_]): JValue = {
            import java.net.URLDecoder

            val value = request.parameters('value) // URLDecoder.decode(request.parameters('value), "UTF-8")

            try JsonParser.parse(value)
            catch {
              case _ => JString(value)
            }
          }

          def periodicityOf(request: HttpRequest[_]): Periodicity = {
            try Periodicity(request.parameters('periodicity))
            catch {
              case _ => throw HttpException(HttpStatusCodes.BadRequest, "Unknown or unspecified periodicity")
            }
          }

          val audit = auditor[JValue, JValue](auditClient, clock, tokenOf)

          jsonp {
            /* The virtual file system, which is used for storing data,
             * retrieving data, and querying for metadata.
             */
            path("""/vfs/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])+)/?)?)""") { 
              $ {
                /* Post data to the virtual file system.
                 */
                audit("track") {
                  post { request: HttpRequest[JValue] =>
                    tokenOf(request).map { token =>
                      val path = fullPathOf(token, request)

                      request.content.foreach { content =>
                        val timestamp: Instant = (content \ "timestamp") match {
                          case JNothing => clock.instant()
                          case jvalue   => jvalue.deserialize[Instant]
                        }

                        val events: JObject = (content \ "events") match {
                          case jobject: JObject => jobject
                          case _ => sys.error("Expecting to find .events field containing object to aggregate")
                        }

                        val count: Int = (content \ "count") match {
                          case JNothing => 1
                          case jvalue   => jvalue.deserialize[Int]
                        }

                        aggregationEngine.aggregate(token, path, timestamp, events, count)
                      }

                      HttpResponse[JValue](content = None)
                    }
                  }
                } ~
                audit("explore paths") {
                  get { request: HttpRequest[JValue] =>
                    tokenOf(request).flatMap { token =>
                      val path = fullPathOf(token, request)

                      aggregationEngine.getPathChildren(token, path).map { children =>
                        HttpResponse[JValue](content = Some(children.serialize))
                      }
                    }
                  }
                }
              } ~
              path("""(?<variable>\.[^\n/]+)""") {
                $ {
                  audit("explore variables") {
                    get { request: HttpRequest[JValue] =>
                      tokenOf(request).flatMap { token =>
                        val path     = fullPathOf(token, request)
                        val variable = variableOf(request)

                        aggregationEngine.getVariableChildren(token, path, variable).map { children =>
                          HttpResponse[JValue](content = Some(children.serialize))
                        }
                      }
                    }
                  }
                } ~
                path("/") {
                  path("statistics") {
                    audit("variable statistics") {
                      get { request: HttpRequest[JValue] =>
                        tokenOf(request).flatMap { token =>
                          val path     = fullPathOf(token, request)
                          val variable = variableOf(request)

                          aggregationEngine.getVariableStatistics(token, path, variable).map { statistics =>
                            HttpResponse[JValue](content = Some(statistics.serialize))
                          }
                        }
                      }
                    }
                  } ~
                  path("count") {
                    audit("variable occurrence count") {
                      get { request: HttpRequest[JValue] =>
                        tokenOf(request).flatMap { token =>
                          val path     = fullPathOf(token, request)
                          val variable = variableOf(request)

                          aggregationEngine.getVariableCount(token, path, variable).map { count =>
                            HttpResponse[JValue](content = Some(count.serialize))
                          }
                        }
                      }
                    }
                  } ~
                  path("series/") {
                    audit("variable series") {
                      path('periodicity) {
                        getRange { (ranges, unit) => (request: HttpRequest[JValue]) =>
                          tokenOf(request).flatMap { token =>
                            val path        = fullPathOf(token, request)
                            val variable    = variableOf(request)
                            val periodicity = periodicityOf(request)

                            unit.toLowerCase match {
                              case "time" =>
                                val (start, end) = ranges.head

                                val startTime = new Instant(start)
                                val endTime   = new Instant(end)

                                aggregationEngine.getVariableSeries(token, path, variable, periodicity, Some(startTime), Some(endTime)).map { series =>
                                  HttpResponse[JValue](content = Some(series.toJValue))
                                }

                              case _ => throw HttpException(HttpStatusCodes.BadRequest, "GET with Range only accepts unit of 'time'")
                            }
                          }
                        } ~
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path        = fullPathOf(token, request)
                            val variable    = variableOf(request)
                            val periodicity = periodicityOf(request)

                            aggregationEngine.getVariableSeries(token, path, variable, periodicity).map { series =>
                              HttpResponse[JValue](content = Some(series.toJValue))
                            }
                          }
                        }
                      }
                    }
                  } ~
                  path("histogram/") {
                    $ {
                      audit("variable histogram") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)

                            aggregationEngine.getHistogram(token, path, variable).map { values =>
                              val content = renderHistogram(values)

                              HttpResponse[JValue](content = Some(content))
                            }
                          }
                        }
                      }
                    } ~
                    path("top/'limit") {
                      audit("variable histogram top") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            aggregationEngine.getHistogramTop(token, path, variable, limit).map { values =>
                              val content = renderHistogram(values)

                              HttpResponse[JValue](content = Some(content))
                            }
                          }
                        }
                      }
                    } ~
                    path("bottom/'limit") {
                      audit("variable histogram bottom") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            aggregationEngine.getHistogramBottom(token, path, variable, limit).map { values =>
                              val content = renderHistogram(values)

                              HttpResponse[JValue](content = Some(content))
                            }
                          }
                        }
                      }
                    }
                  } ~
                  path("length") {
                    audit("count of variable values") {
                      get { request: HttpRequest[JValue] =>
                        tokenOf(request).flatMap { token =>
                          val path     = fullPathOf(token, request)
                          val variable = variableOf(request)

                          aggregationEngine.getVariableLength(token, path, variable).map { length =>
                            HttpResponse[JValue](content = Some(length.serialize))
                          }
                        }
                      }
                    }
                  } ~
                  path("values/") {
                    $ {
                      audit("list of variable values") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)

                            aggregationEngine.getValues(token, path, variable).map { values =>
                              HttpResponse[JValue](content = Some(values.toList.serialize))
                            }
                          }
                        }
                      }
                    } ~
                    path("top/'limit") {
                      audit("list of top variable values") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            aggregationEngine.getValuesTop(token, path, variable, limit).map { values =>
                              HttpResponse[JValue](content = Some(values.serialize))
                            }
                          }
                        }
                      }
                    } ~
                    path("bottom/'limit") {
                      audit("list of bottom variable values") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            aggregationEngine.getValuesBottom(token, path, variable, limit).map { values =>
                              HttpResponse[JValue](content = Some(values.serialize))
                            }
                          }
                        }
                      }
                    } ~
                    path('value) {
                      $ {
                        audit("explore a variable value") {
                          get { request: HttpRequest[JValue] =>
                            tokenOf(request).map { token =>
                              val path     = fullPathOf(token, request)
                              val variable = variableOf(request)
                              val value    = valueOf(request)

                              val options = JArray(
                                JString("series/") ::
                                JString("geo/") ::
                                JString("count") ::
                                Nil
                              )

                              HttpResponse[JValue](content = Some(options))
                            }
                          }
                        }
                      } ~
                      path("/") {
                        path("count") {
                          audit("count occurrences of a variable value") {
                            get { request: HttpRequest[JValue] =>
                              tokenOf(request).flatMap { token =>
                                val path     = fullPathOf(token, request)
                                val observation = Obs.ofValue(variableOf(request), valueOf(request))
                                aggregationEngine.searchCount(token, path, observation) map { count =>
                                  HttpResponse[JValue](content = Some(count.serialize))
                                }
                              }
                            }
                          }
                        } ~
                        path("series/") {
                          audit("variable value series") {
                            get { request: HttpRequest[JValue] =>
                              tokenOf(request).map { token =>
                                val path     = fullPathOf(token, request)
                                val variable = variableOf(request)
                                val value    = valueOf(request)

                                val options = JArray(
                                  Periodicity.Default.map(p => JString(p.name))
                                )

                                HttpResponse[JValue](content = Some(options))
                              }
                            } ~
                            path('periodicity) {
                              getRange { (ranges, unit) => (request: HttpRequest[JValue]) =>
                                tokenOf(request).flatMap { token =>
                                  val path        = fullPathOf(token, request)
                                  val periodicity = periodicityOf(request)
                                  val observation = Obs.ofValue(variableOf(request), valueOf(request))

                                  unit.toLowerCase match {
                                    case "time" =>
                                      val (start, end) = ranges.head

                                      aggregationEngine.searchSeries(token, path, observation, periodicity, Some(new Instant(start)), Some(new Instant(end))) map { series =>
                                        HttpResponse[JValue](content = Some(series.toJValue))
                                      }

                                    case _ => throw HttpException(HttpStatusCodes.BadRequest, "GET with Range only accepts unit of 'time'")
                                  }
                                }
                              } ~
                              get { request: HttpRequest[JValue] =>
                                tokenOf(request).flatMap { token =>
                                  val path        = fullPathOf(token, request)
                                  val observation = Obs.ofValue(variableOf(request), valueOf(request))
                                  val periodicity = periodicityOf(request)

                                  aggregationEngine.searchSeries(token, path, observation, periodicity).map { series =>
                                    HttpResponse[JValue](content = Some(series.toJValue))
                                  }
                                }
                              }
                            }
                          }
                        } ~
                        path("geo") {
                          get { request: HttpRequest[JValue] =>
                            tokenOf(request).map { token =>
                              val path     = fullPathOf(token, request)
                              val variable = variableOf(request)

                              HttpResponse[JValue](content = None)
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            } ~
            path("/search") {
              audit("count or series query") {
                post { request: HttpRequest[JValue] =>
                  tokenOf(request).flatMap { token =>
                    request.content.map[Future[HttpResponse[JValue]]] { content =>
                      val select = (content \ "select").deserialize[String].toLowerCase

                      val from: Path = token.path + "/" + (content \ "from").deserialize[String]

                      val where = (content \ "where").children.collect {
                        case JField(name, value) =>
                          val variable  = Variable(JPath(name))
                          val predicate = HasValue(value)

                          (variable -> predicate)
                      }.toSet

                      val start = (content \ "start") match {
                        case JNothing | JNull => None
                        case jvalue   => Some(jvalue.deserialize[Instant])
                      }

                      val end = (content \ "end") match {
                        case JNothing | JNull => None
                        case jvalue   => Some(jvalue.deserialize[Instant])
                      }

                      Selection(select) match {
                        case Count => 
                          aggregationEngine.searchCount(token, from, where, start, end).map { count =>
                            HttpResponse[JValue](content = Some(count.serialize))
                          }

                        case Series(periodicity) => 
                          aggregationEngine.searchSeries(token, from, where, periodicity,  start, end).map { 
                            series => HttpResponse[JValue](content = Some(series.toJValue))
                          }
                      }
                    } getOrElse {
                      Future.sync(HttpResponse[JValue](content = None))
                    }
                  }
                }
              }
            } ~
            path("/intersect") {
              audit("intersection query") {
                post { request: HttpRequest[JValue] => 
                  tokenOf(request).flatMap { token => 
                    import VariableDescriptor._
                    val content = request.content.get
                      
                    val from: Path = token.path + "/" + (content \ "from").deserialize[String]
                    val properties = (content \ "properties").deserialize[List[VariableDescriptor]]

                    val select = Selection((content \ "select").deserialize[String].toLowerCase)

                    val start = (content \ "start") match {
                      case JNothing | JNull => None
                      case jvalue   => Some(jvalue.deserialize[Instant])
                    }

                    val end = (content \ "end") match {
                      case JNothing | JNull => None
                      case jvalue   => Some(jvalue.deserialize[Instant])
                    }

                    val resultContent = select match {
                      case Count => 
                        aggregationEngine.intersectCount(token, from, properties, start, end).map {
                          serializeIntersectionResult[CountType](_, _.serialize)
                        }

                      case Series(p) => 
                        aggregationEngine.intersectSeries(token, from, properties, p, start, end).map {
                          serializeIntersectionResult[TimeSeriesType](_, _.toJValue)
                        }
                    }
                    
                    resultContent map {
                      v => HttpResponse(content = Some(v))
                    }
                  }
                } 
              }
            } ~ 
            path("/tokens/") {
              audit("token") {
                get { request: HttpRequest[JValue] =>
                  tokenOf(request) flatMap { token =>
                    tokenManager.listDescendants(token) map { descendants =>
                      descendants.map { descendantToken =>
                        descendantToken.tokenId.serialize
                      }
                    } map { tokens =>
                      HttpResponse[JValue](content = Some(JArray(tokens)))
                    }
                  }
                } ~
                post { request: HttpRequest[JValue] =>
                  tokenOf(request).flatMap { token =>
                    val content: JValue = request.content.getOrElse("New token must be contained in POST content")

                    val path        = (content \ "path").deserialize[Option[String]].getOrElse("/")
                    val permissions = (content \ "permissions").deserialize[Option[Permissions]].getOrElse(token.permissions)
                    val expires     = (content \ "expires").deserialize[Option[DateTime]].getOrElse(token.expires)
                    val limits      = (content \ "limits").deserialize[Option[Limits]].getOrElse(token.limits)

                    tokenManager.issueNew(token, path, permissions, expires, limits).map { newToken =>
                      HttpResponse[JValue](content = Some(newToken.tokenId.serialize))
                    }
                  }
                } ~
                path('descendantTokenId) {
                  get { request: HttpRequest[JValue] =>
                    tokenOf(request).flatMap { token =>
                      tokenManager.getDescendant(token, request.parameters('descendantTokenId)).map { 
                        _.map { _.relativeTo(token).serialize }
                      } map { descendantToken =>
                        HttpResponse[JValue](content = descendantToken)
                      }
                    }
                  } ~
                  delete { (request: HttpRequest[JValue]) =>
                    tokenOf(request).flatMap { token =>
                      tokenManager.deleteDescendant(token, request.parameters('descendantTokenId)).map { _ =>
                        HttpResponse[JValue](content = None)
                      }
                    }
                  }
                }
              }
            }
          } 
        } ->
        shutdown { state =>
          state.aggregationEngine.stop
        }
      }
    }
    }
  }
}

object AnalyticsService {
  import AggregationEngine._
  def serializeIntersectionResult[T](result: IntersectionResult[T], f: T => JValue) = {
    result.foldLeft[JValue](JObject(Nil)) {
      case (result, (values, x)) => 
        result.set(JPath(values.map(v => JPathField(renderNormalized(v)))), f(x))
    }
  }
}

object AnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.RealMongo(configMap)
  }

  def auditClientFactory(config: ConfigMap) = {
    val auditToken = config.getString("token", Token.Test.tokenId)
    val environment = config.getString("environment", "production") match {
      case "production" => ReportGridConfig.Production
      case _            => ReportGridConfig.Local
    }

    ReportGrid(auditToken, environment)
  }
}

object TestAnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(config: ConfigMap) = {
    new blueeyes.persistence.mongo.mock.MockMongo()
  }

  def auditClientFactory(config: ConfigMap) = ReportGrid(Token.Test.tokenId, ReportGridConfig.Local)
}
