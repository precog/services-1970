package com.reportgrid.analytics
package service

import blueeyes.concurrent.Future
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import AnalyticsService._
import AggregationEngine._
import AnalyticsServiceSerialization._
import com.reportgrid.ct.Mult.MDouble._
import com.weiglewilczek.slf4s.Logging

import scalaz.Scalaz._
import scalaz.Success
import scalaz.Validation

trait FutureContent[T] {
  def futureContent(request : HttpRequest[Future[T]]) = 
    request.content.map(_.map(Some[T](_))).getOrElse(Future.sync[Option[T]](None))
}

class ExplorePathService[A](aggregationEngine: AggregationEngine) 
extends CustomHttpService[A, (Token, Path) => Future[HttpResponse[JValue]]] {
  val service = (_: HttpRequest[A]) => Success(
    (token: Token, path: Path) => {
      if (token.permissions.explore) {
        aggregationEngine.getPathChildren(token, path).map(_.serialize.ok)
      } else {
        Future.sync(HttpResponse[JValue](Unauthorized, content = Some("The specified token does not permit exploration of the virtual filesystem.")))
      }
    }
  )

  val metadata = None
}

class ExploreVariableService[A](aggregationEngine: AggregationEngine) 
extends CustomHttpService[A, (Token, Path, Variable) => Future[HttpResponse[JValue]]] {
  val service = (_: HttpRequest[A]) => Success(
    (token: Token, path: Path, variable: Variable) => {
      if (token.permissions.explore) {
        aggregationEngine.getVariableChildren(token, path, variable).map(_.map(_._1.child).serialize.ok)
      } else {
        Future.sync(HttpResponse[JValue](Unauthorized, content = Some("The specified token does not permit exploration of variable children.")))
      }
    }
  )

  val metadata = None
}

class VariableChildCountService(val aggregationEngine: AggregationEngine) 
extends CustomHttpService[Future[JValue], (Token, Path, Variable) => Future[HttpResponse[JValue]]] 
with FutureContent[JValue]
with ChildLocationsService {
  val service = (request: HttpRequest[Future[JValue]]) => Success(
    (token: Token, path: Path, variable: Variable) => {
      if (token.permissions.explore) {
        futureContent(request).flatMap {
          requestContent => {
            val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, requestContent))
               
            withChildLocations(token, path, terms, request.parameters) { 
              aggregationEngine.getVariableChildren(token, path, variable, _).map(_.map{ info => (info._1.child, info._2) }.serialize)
            }.map(_.ok)
          }
        }
      } else {
        Future.sync(HttpResponse[JValue](Unauthorized, content = Some("The specified token does not permit exploration of variable children.")))
      }
    }
  )

  val metadata = None
}

class ExploreValuesService(val aggregationEngine: AggregationEngine, limit: Int) 
extends FutureContent[JValue]
with ChildLocationsService with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => //Success(
    (token: Token, path: Path, variable: Variable) => {
      val result : Future[HttpResponse[JValue]] = if (token.permissions.explore) {
        futureContent(request).flatMap {
          requestContent => {
            val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, requestContent))
               
            withChildLocations(token, path, terms, request.parameters) { 
              newTerms => {
                if (limit == 0) { // unlimited
                  aggregationEngine.getValues(token, path, variable, newTerms).map(_.serialize)
                } else if (limit > 0) { // topN 
                  aggregationEngine.getValuesTop(token, path, variable, limit, newTerms).map(_.serialize)
                } else { // bottomN
                  aggregationEngine.getValuesBottom(token, path, variable, -limit, newTerms).map(_.serialize)
                }
              }
            }.map(_.ok)
          }
        }
      } else {
        Future.sync(HttpResponse[JValue](Unauthorized, content = Some("The specified token does not permit exploration of the virtual filesystem.")))
      }
      result
    }
  //)
}

class ExploreTagsService[A](aggregationEngine: AggregationEngine) 
extends CustomHttpService[A, (Token, Path) => Future[HttpResponse[JValue]]] {
  val service = (_: HttpRequest[A]) => Success(
    (token: Token, path: Path) => {
      aggregationEngine.getPathTags(token, path).map(_.serialize.ok)
    }
  )

  val metadata = None
}

class ExploreHierarchyService[A](aggregationEngine: AggregationEngine) 
extends CustomHttpService[A, (Token, Path, Variable) => Future[HttpResponse[JValue]]] {
  val service = (_: HttpRequest[A]) => Success(
    (token: Token, path: Path, variable: Variable) => {
      variable.name.head flatMap { tagName =>
        tagName match {
          case JPathField(tagName) => Some(aggregationEngine.getHierarchyChildren(token, path, tagName, variable.name.tail).map(_.toList.serialize.ok))
          case _                   => None
        }
      } getOrElse {
        Future.sync(HttpResponse[JValue](NotFound, content = Some("The specified tag does not exist.")))
      }
    }
  )

  val metadata = None
}

trait ChildLocationsService {
  def aggregationEngine: AggregationEngine

  def withChildLocations(token: Token, path: Path, terms: List[TagTerm], parameters: Map[Symbol, String])(f: List[TagTerm] => Future[JValue]) : Future[JValue] = {
    parameters.get('use_tag_children) flatMap { tagName =>
      terms collectFirst { 
        case term @ HierarchyLocationTerm(`tagName`, location) =>
          val remainingTerms = terms.filterNot(_ == term)
          aggregationEngine.getHierarchyChildren(token, path, tagName, JPath(location.path.elements.map(JPathField(_)): _*)).flatMap { results =>
            val fields: Iterable[Future[JField]] = results map { tagChild =>
              val childLoc = location / tagChild
              f(HierarchyLocationTerm(tagName, childLoc) :: remainingTerms).map(JField(childLoc.path.path, _))
            }

            Future(fields.toSeq: _*).map(JObject(_).asInstanceOf[JValue])
          }
      }
    } getOrElse {
      f(terms)
    }
  }
}

class VariableCountService(val aggregationEngine: AggregationEngine)
extends CustomHttpService[Future[JValue], (Token, Path, Variable) => Future[HttpResponse[JValue]]] 
with ChildLocationsService {
  val service = (request: HttpRequest[Future[JValue]]) => Success(
    (token: Token, path: Path, variable: Variable) => {
      val futureContent: Future[Option[JValue]] = request.content.map(_.map[Option[JValue]](Some(_)))
                                                         .getOrElse(Future.sync[Option[JValue]](None))

      futureContent flatMap { requestContent => 
        val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, requestContent))

        val responseContent = withChildLocations(token, path, terms, request.parameters) {
          aggregationEngine.getVariableCount(token, path, variable, _).map(_.serialize)
        }

        responseContent.map(_.ok)
      }
    }
  )

  val metadata = None
}

class VariableSeriesService[T: Decomposer : AbelianGroup](val aggregationEngine: AggregationEngine, f: ValueStats => T) 
extends CustomHttpService[Future[JValue], (Token, Path, Variable) => Future[HttpResponse[JValue]]] 
with ChildLocationsService {
  //val service: HttpRequest[Future[JValue]] => Validation[NotServed,(Token, Path, Variable) => Future[HttpResponse[JValue]]] = 
  val service = (request: HttpRequest[Future[JValue]]) => {
    request.parameters.get('periodicity).flatMap(Periodicity.byName)
    .toSuccess(DispatchError(BadRequest, "A periodicity must be specified in order to query for a time series."))
    .map { periodicity =>
      (token: Token, path: Path, variable: Variable) => {
        request.content.map(_.map(Some(_))).getOrElse(Future.sync(None)).flatMap { content => 
          val terms = List(intervalTerm(periodicity), locationTerm).flatMap(_.apply(request.parameters, content))

          val responseContent = withChildLocations(token, path, terms, request.parameters) {
            aggregationEngine.getVariableSeries(token, path, variable, _) 
            .map(transformTimeSeries[ValueStats](request, periodicity))
            .map(_.map(f.second).serialize)
          }

          responseContent.map(_.ok)
        }
      }
    }
  }

  val metadata = None
}

class ValueCountService(val aggregationEngine: AggregationEngine) 
extends CustomHttpService[Future[JValue], (JValue) => (Token, Path, Variable) => Future[HttpResponse[JValue]]] 
with ChildLocationsService {
  val service = (request: HttpRequest[Future[JValue]]) => Success(
    (value: JValue) => (token: Token, path: Path, variable: Variable) => {
      val futureContent: Future[Option[JValue]] = request.content.map(_.map[Option[JValue]](Some(_)))
                                                         .getOrElse(Future.sync[Option[JValue]](None))

      futureContent.flatMap { content => 
        val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, content))
        val responseContent = withChildLocations(token, path, terms, request.parameters) {
          aggregationEngine.getObservationCount(token, path, JointObservation(HasValue(variable, value)), _)
          .map(_.serialize)
        }

        responseContent.map(_.ok)
      }
    }
  )

  val metadata = None
}

class ValueSeriesService(val aggregationEngine: AggregationEngine) 
extends CustomHttpService[Future[JValue], (JValue) => (Token, Path, Variable) => Future[HttpResponse[JValue]]] 
with ChildLocationsService {
  val service = (request: HttpRequest[Future[JValue]]) => {
    request.parameters.get('periodicity).flatMap(Periodicity.byName)
    .toSuccess(DispatchError(BadRequest, "A periodicity must be specified in order to query for a time series."))
    .map { periodicity =>
      (value: JValue) => (token: Token, path: Path, variable: Variable) => {
        request.content.map(_.map(Some(_))).getOrElse(Future.sync(None)).flatMap { content => 
          val terms = List(intervalTerm(periodicity), locationTerm).flatMap(_.apply(request.parameters, content))

          val responseContent = withChildLocations(token, path, terms, request.parameters) {
            aggregationEngine.getObservationSeries(token, path, JointObservation(HasValue(variable, value)), _)
            .map(transformTimeSeries(request, periodicity))
            .map(_.serialize)
          }
          
          responseContent.map(_.ok)
        }
      }
    }
  }

  val metadata = None
}

class SearchService(val aggregationEngine: AggregationEngine)
extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] 
with ChildLocationsService {
  import Extractor._
  val service = (request: HttpRequest[Future[JValue]]) => Success(
    (token: Token) => {
      request.content map { 
        _.flatMap { content => 
          val queryComponents = (content \ "select").validated[String].flatMap(s => Selection.parse(s).toSuccess(Invalid("Invalid selection type: " + s))) |@| 
                                (content \ "from").validated[String].map(token.path / _) |@|
                                (content \ "where").validated[Set[HasValue]].map(JointObservation(_))

          queryComponents.apply { case (select, path, observation) => 
            val responseContent = select match {
              case Count => 
                val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, Some(content)))
                withChildLocations(token, path, terms, request.parameters) {
                  aggregationEngine.getObservationCount(token, path, observation, _)
                  .map(_.serialize)
                }

              case Series(periodicity) => 
                val terms = List(intervalTerm(periodicity), locationTerm).flatMap(_.apply(request.parameters, Some(content)))
                withChildLocations(token, path, terms, request.parameters) {
                  aggregationEngine.getObservationSeries(token, path, observation, _)
                  .map(transformTimeSeries[CountType](request, periodicity))
                  .map(_.serialize)
                }

              case Related => 
                val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, Some(content)))
                withChildLocations(token, path, terms, request.parameters) {
                  aggregationEngine.findRelatedInfiniteValues(token, path, observation, _)
                  .map(_.toList.serialize)
                }
            }

            responseContent.map(_.ok)
          } ||| { 
            errors => Future.sync(HttpResponse[JValue](BadRequest, content = Some(errors.message.serialize))) 
          }
        }
      }
    } getOrElse Future.sync {
      HttpResponse[JValue](BadRequest, content = Some("""Request body was empty. The "select", "from", and "where" fields must be specified."""))
    }
  )

  val metadata = None
}

class IntersectionService(val aggregationEngine: AggregationEngine)
extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] 
with ChildLocationsService with Logging {
  import Extractor._
  val service = (request: HttpRequest[Future[JValue]]) => Success(
    (token: Token) => {
      request.content map { 
        _.flatMap { content => 
          val queryComponents = (content \ "select").validated[String].flatMap(s => Selection.parse(s).toSuccess(Invalid("Invalid selection type: " + s))) |@| 
                                (content \ "from").validated[String].map(token.path / _) |@|
                                (content \ "properties").validated[List[VariableDescriptor]]

          queryComponents.apply { case (select, path, where) => 
            val responseContent = select match {
              case Count => 
                val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, Some(content)))
                logger.trace("Intersection count terms = " + terms)
                withChildLocations(token, path, terms, request.parameters) {
                  aggregationEngine.getIntersectionCount(token, path, where, _)
                  .map(serializeIntersectionResult[CountType])
                }
                
              case Series(periodicity) =>
                val terms = List(intervalTerm(periodicity), locationTerm).flatMap(_.apply(request.parameters, Some(content)))
                logger.trace("Intersection series terms = " + terms)
                withChildLocations(token, path, terms, request.parameters) {
                  aggregationEngine.getIntersectionSeries(token, path, where, _)
                  .map(_.map(transformTimeSeries[CountType](request, periodicity).second))
                  .map(serializeIntersectionResult[ResultSet[JObject, CountType]])
                }
            }

            responseContent.map(_.ok)
          } ||| { 
            errors => Future.sync(HttpResponse[JValue](BadRequest, content = Some(errors.message.serialize))) 
          }
        }
      } getOrElse Future.sync {
        HttpResponse[JValue](BadRequest, content = Some("""Request body was empty. The "select", "from", and "properties" fields must be specified."""))
      }
    }
  )

  val metadata = None
}

object HistogramService {
  def childLocator(engine : AggregationEngine) = new ChildLocationsService { val aggregationEngine = engine }

  val getHistogram = (request: HttpRequest[Future[JValue]], aggregationEngine : AggregationEngine, token: Token, path: Path, variable: Variable, limit: Int) => {
    val futureContent: Future[Option[JValue]] = request.content.map(_.map[Option[JValue]](Some(_)))
    .getOrElse(Future.sync[Option[JValue]](None))

    futureContent flatMap { 
      requestContent => {
        val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, requestContent))
                           
        childLocator(aggregationEngine).withChildLocations(token, path, terms, request.parameters) { 
          newTerms => {
            if (limit == 0) { // unlimited
              aggregationEngine.getHistogram(token, path, variable, newTerms).map(_.serialize)
            } else if (limit > 0) { // topN 
              aggregationEngine.getHistogramTop(token, path, variable, limit, newTerms).map(_.serialize)
            } else { // bottomN
              aggregationEngine.getHistogramBottom(token, path, variable, -limit, newTerms).map(_.serialize)
            }
          }
        }.map(_.ok)
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
