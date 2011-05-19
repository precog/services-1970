package com.reportgrid.analytics

import blueeyes.{BlueEyesServiceBuilder, BlueEyesServer}
import blueeyes.concurrent.Future
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.json.JsonAST._
import blueeyes.json.{JPath, JsonParser, JPathField}
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.core.data.{Chunk, BijectionsChunkJson, BijectionsChunkString}

import net.lag.configgy.{Configgy, ConfigMap}

import org.joda.time.{DateTime, DateTimeZone}

import java.util.concurrent.TimeUnit

import com.reportgrid.analytics.AggregatorImplicits._
import com.reportgrid.analytics.persistence.MongoSupport._

case class AnalyticsState(aggregationEngine: AggregationEngine, tokenManager: TokenManager)
trait AnalyticsService extends BlueEyesServiceBuilder with BijectionsChunkJson with BijectionsChunkString {
  def mongoFactory(configMap: ConfigMap): Mongo

  val analyticsService = service("analytics", "0.01") {
    logging { logger =>
      healthMonitor { monitor => context =>
        startup {
          import context._

          val mongoConfig = config.configMap("mongo")

          val mongo = mongoFactory(mongoConfig)

          val database = mongo.database(mongoConfig.getString("database").getOrElse("analytics"))

          val tokensCollection  = mongoConfig.getString("tokensCollection").getOrElse("tokens")
          val reportsCollection = mongoConfig.getString("reportsCollection").getOrElse("reports")

          val aggregationEngine = new AggregationEngine(config, logger, database)
          TokenManager(database, tokensCollection).map(AnalyticsState(aggregationEngine, _))
        } ->
        request { state =>
          import state._

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
            val value = request.parameters('value)

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

          jsonp {
            /* The virtual file system, which is used for storing data,
             * retrieving data, and querying for metadata.
             */
            path("""/vfs/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])+)/?)?)""") {
              $ {
                /* Post data to the virtual file system.
                 */
                post { request: HttpRequest[JValue] =>
                  tokenOf(request).map { token =>
                    val path = fullPathOf(token, request)

                    request.content.foreach { content =>
                      val timestamp: DateTime = (content \ "timestamp") match {
                        case JNothing => new DateTime(DateTimeZone.UTC)
                        case jvalue   => jvalue.deserialize[DateTime]
                      }

                      val events: JObject = (content \ "events") match {
                        case jobject: JObject => jobject

                        case _ => error("Expecting to find .events field containing object to aggregate")
                      }

                      val count: Int = (content \ "count") match {
                        case JNothing => 1
                        case jvalue   => jvalue.deserialize[Int]
                      }

                      aggregationEngine.aggregate(token, path, timestamp, events, count)
                    }

                    HttpResponse[JValue](content = None)
                  }
                } ~
                get { request: HttpRequest[JValue] =>
                  tokenOf(request).flatMap { token =>
                    val path = fullPathOf(token, request)

                    aggregationEngine.getChildren(token, path).map { children =>
                      HttpResponse[JValue](content = Some(children.serialize))
                    }
                  }
                }
              } ~
              path("""(?<variable>\.[^\n/]+)""") {
                $ {
                  get { request: HttpRequest[JValue] =>
                    tokenOf(request).flatMap { token =>
                      val path     = fullPathOf(token, request)
                      val variable = variableOf(request)

                      aggregationEngine.getChildren(token, path, variable).map { children =>
                        HttpResponse[JValue](content = Some(children.serialize))
                      }
                    }
                  }
                } ~
                path("/") {
                  path("count") {
                    get { request: HttpRequest[JValue] =>
                      tokenOf(request).flatMap { token =>
                        val path     = fullPathOf(token, request)
                        val variable = variableOf(request)

                        //println("Count request: " + (token, path, variable))
                        //aggregationEngine.printDatabase

                        aggregationEngine.getVariableCount(token, path, variable).map { count =>
                          HttpResponse[JValue](content = Some(count.serialize))
                        }
                      }
                    }
                  } ~
                  path("series/") {
                    path('periodicity) {
                      getRange { (ranges, unit) => (request: HttpRequest[JValue]) =>
                        tokenOf(request).flatMap { token =>
                          val path        = fullPathOf(token, request)
                          val variable    = variableOf(request)
                          val periodicity = periodicityOf(request)

                          unit.toLowerCase match {
                            case "time" =>
                              val (start, end) = ranges.head

                              val startTime = new DateTime(start, DateTimeZone.UTC)
                              val endTime   = new DateTime(end,   DateTimeZone.UTC)

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
                  } ~
                  path("values/") {
                    $ {
                      get { request: HttpRequest[JValue] =>
                        tokenOf(request).flatMap { token =>
                          val path     = fullPathOf(token, request)
                          val variable = variableOf(request)

                          aggregationEngine.getValues(token, path, variable).map { values =>
                            HttpResponse[JValue](content = Some(values.serialize))
                          }
                        }
                      }
                    } ~
                    path('value) {
                      $ {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
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
                      } ~
                      path("/") {
                        path("count") {
                          get { request: HttpRequest[JValue] =>
                            tokenOf(request).flatMap { token =>
                              val path     = fullPathOf(token, request)
                              val variable = variableOf(request)
                              val value    = valueOf(request)

                              aggregationEngine.getValueCount(token, path, variable, value).map { count =>
                                HttpResponse[JValue](content = Some(count.serialize))
                              }
                            }
                          }
                        } ~
                        path("series/") {
                          get { request: HttpRequest[JValue] =>
                            tokenOf(request).flatMap { token =>
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
                                val variable    = variableOf(request)
                                val periodicity = periodicityOf(request)
                                val value       = valueOf(request)

                                unit.toLowerCase match {
                                  case "time" =>
                                    val (start, end) = ranges.head

                                    val startTime = new DateTime(start, DateTimeZone.UTC)
                                    val endTime   = new DateTime(end,   DateTimeZone.UTC)

                                    aggregationEngine.getValueSeries(token, path, variable, value, periodicity, Some(startTime), Some(endTime)).map { series =>
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
                                val value       = valueOf(request)
                                val periodicity = periodicityOf(request)

                                aggregationEngine.getValueSeries(token, path, variable, value, periodicity).map { series =>
                                  HttpResponse[JValue](content = Some(series.toJValue))
                                }
                              }
                            }
                          }
                        } ~
                        path("geo") {
                          get { request: HttpRequest[JValue] =>
                            tokenOf(request).flatMap { token =>
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
                      case JNothing => None
                      case jvalue   => Some(jvalue.deserialize[DateTime])
                    }

                    val end = (content \ "end") match {
                      case JNothing => None
                      case jvalue   => Some(jvalue.deserialize[DateTime])
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
                  }.getOrElse[Future[HttpResponse[JValue]]] {
                    HttpResponse[JValue](content = None)
                  }
                }
              }
            } ~
            path("/intersect") {
              post { request: HttpRequest[JValue] => 
                tokenOf(request).flatMap { token => 
                  import VariableDescriptor._
                  val content = request.content.get
                    
                  val from: Path = token.path + "/" + (content \ "from").deserialize[String]
                  val properties = (content \ "properties").deserialize[List[VariableDescriptor]]

                  val select = Selection((content \ "select").deserialize[String].toLowerCase)

                  val start = (content \ "start") match {
                    case JNothing => None
                    case jvalue   => Some(jvalue.deserialize[DateTime])
                  }

                  val end = (content \ "end") match {
                    case JNothing => None
                    case jvalue   => Some(jvalue.deserialize[DateTime])
                  }

                  (select match {
                    case Count => 
                      aggregationEngine.intersectCount(token, from, properties, start, end).map {
                        _.foldLeft[JValue](JObject(Nil)) {
                          case (result, (values, count)) => 
                            result.set(JPath(values.map(v => JPathField(renderNormalized(v)))), count.serialize)
                        }
                      }

                    case Series(p) => 
                      aggregationEngine.intersectSeries(token, from, properties, p, start, end).map {
                        _.foldLeft[JValue](JObject(Nil)) {
                          case (result, (values, count)) => 
                            result.set(JPath(values.map(v => JPathField(renderNormalized(v)))), count.serialize)
                        }
                      }
                  }) map {
                    v => HttpResponse(content = Some(v))
                  }
                }
              } 
            } ~ 
            path("/tokens/") {
              get { request: HttpRequest[JValue] =>
                tokenOf(request).flatMap { token =>
                  tokenManager.listDescendants(token).map { descendants =>
                    descendants.map { descendantToken =>
                      descendantToken.tokenId.serialize
                    }
                  }.map { tokens =>
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
                //parameter('descendantTokenId) { descendantTokenId =>
                  get { request: HttpRequest[JValue] =>
                    //println(request.parameters)

                    tokenOf(request).flatMap { token =>
                      tokenManager.getDescendant(token, request.parameters('descendantTokenId)).map { descendantToken =>
                        descendantToken.map { _.relativeTo(token).serialize }
                      }.map { descendantToken =>
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
                //}
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

object AnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.RealMongo(configMap)
  }
}

object TestAnalyticsServer extends BlueEyesServer with AnalyticsService {
  val configuration = """
  """

  Configgy.configureFromString(configuration)

  def mongoFactory(config: ConfigMap) = {
    new blueeyes.persistence.mongo.mock.MockMongo()
  }
}
