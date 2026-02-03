package io.supertable.spark

import sttp.client3._
import sttp.client3.circe._
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._

import scala.util.Try

/**
 * REST client for communicating with SuperTable catalog server.
 */
class SuperTableRestClient(baseUri: String) {
  
  private val backend = HttpClientSyncBackend()
  
  case class TableIdentifier(namespace: Seq[String], name: String)
  case class TableListResponse(identifiers: Seq[TableIdentifier])
  case class NamespaceListResponse(namespaces: Seq[Seq[String]])
  case class TableMetadata(
    `metadata-location`: String,
    metadata: Json
  )
  
  def listNamespaces(): Seq[String] = {
    val response = basicRequest
      .get(uri"$baseUri/v1/namespaces")
      .response(asJson[NamespaceListResponse])
      .send(backend)
    
    response.body match {
      case Right(resp) => resp.namespaces.map(_.mkString("."))
      case Left(_) => Seq.empty
    }
  }
  
  def createNamespace(namespace: String): Unit = {
    basicRequest
      .post(uri"$baseUri/v1/namespaces")
      .body(s"""{"namespace": ["$namespace"]}""")
      .contentType("application/json")
      .send(backend)
  }
  
  def listTables(namespace: String): Seq[String] = {
    val response = basicRequest
      .get(uri"$baseUri/v1/namespaces/$namespace/tables")
      .response(asJson[TableListResponse])
      .send(backend)
    
    response.body match {
      case Right(resp) => resp.identifiers.map(_.name)
      case Left(_) => Seq.empty
    }
  }
  
  def getTable(namespace: String, name: String): Json = {
    val response = basicRequest
      .get(uri"$baseUri/v1/namespaces/$namespace/tables/$name")
      .response(asJson[Json])
      .send(backend)
    
    response.body.getOrElse(Json.Null)
  }
  
  def createTable(
    namespace: String,
    name: String,
    schemaJson: String,
    properties: java.util.Map[String, String]
  ): Json = {
    import scala.collection.JavaConverters._
    
    val propsJson = properties.asScala.map { case (k, v) => s""""$k": "$v"""" }.mkString("{", ",", "}")
    val body = s"""{"name": "$name", "schema": $schemaJson, "properties": $propsJson}"""
    
    val response = basicRequest
      .post(uri"$baseUri/v1/namespaces/$namespace/tables")
      .body(body)
      .contentType("application/json")
      .response(asJson[Json])
      .send(backend)
    
    response.body.getOrElse(Json.Null)
  }
  
  def dropTable(namespace: String, name: String): Unit = {
    basicRequest
      .delete(uri"$baseUri/v1/namespaces/$namespace/tables/$name")
      .send(backend)
  }
  
  def addColumn(namespace: String, table: String, columnName: String, columnType: String): Unit = {
    // Would call a schema evolution endpoint
    // For now, this is a placeholder
  }
  
  def dropColumn(namespace: String, table: String, columnName: String): Unit = {
    // Would call a schema evolution endpoint
  }
  
  def renameColumn(namespace: String, table: String, oldName: String, newName: String): Unit = {
    // Would call a schema evolution endpoint
  }
}
