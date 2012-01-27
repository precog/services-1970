package com.reportgrid.analytics
package service

import scala.collection.mutable.Stack

import blueeyes.concurrent.Future
import blueeyes.core.http.{HttpException, HttpRequest, HttpResponse, HttpStatusCodes, MimeType, MimeTypes}
import blueeyes.core.http.HttpHeaders.`Content-Type`
import blueeyes.core.service.{DelegatingService, DispatchError, HttpService, JsonpService, NotServed}
import blueeyes.json.JsonAST._
import blueeyes.json.Printer.{compact, render}

import scalaz.Scalaz._
import scalaz.Validation

import com.weiglewilczek.slf4s.Logging

/**
 * This class provides a service that can selectively format responses based on the
 * 'format' query parameter.
 */
case class FormatService[T](formatters : Map[String, FormatServiceHelpers.FormatFunction[T]], default : FormatServiceHelpers.FormatFunction[T], delegate: HttpService[Future[JValue], Future[HttpResponse[JValue]]])(implicit toJson: T => Future[JValue], fromJson: JValue => T) extends DelegatingService[T, Future[HttpResponse[T]], Future[JValue], Future[HttpResponse[JValue]]] {
  import JsonpService._

  def service = (r : HttpRequest[T]) => {
    r.parameters.get('format) match {
        case Some(format) if formatters.contains(format) => {
          runFormat(r, formatters(format))
        }
        case Some(format) => {
          failure(DispatchError(HttpException(HttpStatusCodes.BadRequest, "Unknown format requested: " + format)))
        }
        case None => {
          runFormat(r, default)
        }
    }
  }

  def metadata = None

  def runFormat(r : HttpRequest[T], formatter : FormatServiceHelpers.FormatFunction[T]) : Validation[NotServed, Future[HttpResponse[T]]] =
    jsonpConvertRequest(r).flatMap(delegate.service(_)).map(_.map(formatter(r, _)))
}

object FormatServiceHelpers extends Logging {
  import JsonpService._
  type FormatFunction[T] = (HttpRequest[T], HttpResponse[JValue]) => HttpResponse[T]

  def formatService[T](formatters : Map[String, FormatFunction[T]], default : FormatFunction[T])(delegate : HttpService[Future[JValue], Future[HttpResponse[JValue]]])(implicit toJson: T => Future[JValue], fromJson: JValue => T) =
    FormatService[T](formatters, default, delegate)

  def jsonToCSV[T](implicit fromString : String => T) : FormatFunction[T] = jsonToTable[T](",", MimeTypes.text/MimeTypes.csv)

  /** Converts 1- and 2-dimensional JSON arrays to CSV */
  def jsonToTable[T](separator : String, mimeType : MimeType)(implicit fromString : String => T) : FormatFunction[T] = (request : HttpRequest[T], response : HttpResponse[JValue]) => {
    val contentHeader = `Content-Type`(mimeType)

    // Get column headers from request (always assume CSV for header, but we reformat later)
    val columns = response.headers.get("Reportgrid-Columns").map(_.split(",").mkString(separator)).getOrElse("")

    def transformNumeric(jv : JValue) = response.copy(content = Some(fromString(columns + "\n" + jv.values.toString)), headers = response.headers + contentHeader)
    
    // Helps render inner elements             
    def renderElement(jv : JValue) : String = jv match {
      case JArray(columns) => columns.map(renderElement).mkString(separator)
      case JObject(fields) => fields.map(f => renderElement(f.value)).mkString(separator)
      case other           => compact(render(other))
    }

    def renderObject(jv : JValue, path : Stack[String], current : List[String]) : List[String] = jv match {
      case JObject(childFields) => childFields.flatMap {
        case JField(name, value) => { path.push(name); val subRows = renderObject(value, path, current); path.pop; subRows }
      }
      case JArray(values) => {
        // If we're in an array, objects are no longer to be descended, but have values extracted
        current ::: values.map{v => path.toList.mkString("",separator,renderElement(v))}
      }
      case other => current ::: List(path.toList.mkString("",separator,compact(render(other))))
    }

    response.content match {
      // Variable counts, length, etc
      case Some(ji @ JInt(_)) => transformNumeric(ji)
      case Some(jd @ JDouble(_)) => transformNumeric(jd)

      // Array : explore, series, values, histogram
      case Some(JArray(rows)) => {
        // An array of values or of arrays: count, children, 
        logger.trace("Rendering Arrays to separated format")
        
        // Render the outer element
        val newContent = rows.map(renderElement).mkString(columns + "\n", "\n","\n")
        response.copy(content = Some(fromString(newContent)), headers = response.headers + contentHeader)
      }

      // object: intersection, etc
      case Some(JObject(fields)) => {
        logger.trace("Rendering Object")
        val newContent = fields.flatMap(renderObject(_, Stack.empty[String], Nil)).mkString(columns + "\n", "\n","\n")
        response.copy(content = Some(fromString(newContent)), headers = response.headers + contentHeader)
      }

      case Some(other) => {
        HttpResponse[T](status = HttpStatusCodes.BadRequest, content = Some(fromString("Cannot format:\n" + compact(render(other)))))
      }
      case None => HttpResponse[T](status = response.status, headers = response.headers, version = response.version)
    }
  }

  def jsonToJsonp[T](implicit fromString : String => T) : FormatFunction[T] = (request : HttpRequest[T], response : HttpResponse[JValue]) => {
    jsonpConvertResponse[T](response, request.parameters.get('callback))
  }
}

