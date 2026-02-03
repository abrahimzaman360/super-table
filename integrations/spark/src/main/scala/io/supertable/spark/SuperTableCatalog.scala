package io.supertable.spark

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * SuperTable Spark Catalog implementation.
 * 
 * Usage in Spark:
 * {{{
 *   spark.conf.set("spark.sql.catalog.supertable", "io.supertable.spark.SuperTableCatalog")
 *   spark.conf.set("spark.sql.catalog.supertable.uri", "http://localhost:8080")
 *   
 *   spark.sql("CREATE TABLE supertable.default.events (id BIGINT, data STRING)")
 *   spark.sql("SELECT * FROM supertable.default.events")
 * }}}
 */
class SuperTableCatalog extends TableCatalog with SupportsNamespaces {
  
  private var catalogName: String = _
  private var catalogUri: String = _
  private var client: SuperTableRestClient = _
  
  override def name(): String = catalogName
  
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.catalogUri = options.getOrDefault("uri", "http://localhost:8080")
    this.client = new SuperTableRestClient(catalogUri)
  }
  
  // ==================== Table Operations ====================
  
  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val tables = client.listTables(namespace.mkString("."))
    tables.map(t => Identifier.of(namespace, t)).toArray
  }
  
  override def loadTable(ident: Identifier): Table = {
    val metadata = client.getTable(ident.namespace().mkString("."), ident.name())
    new SuperTableSparkTable(ident, metadata, client)
  }
  
  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: util.Map[String, String]
  ): Table = {
    val schemaJson = schemaToJson(schema)
    val metadata = client.createTable(
      ident.namespace().mkString("."),
      ident.name(),
      schemaJson,
      properties
    )
    new SuperTableSparkTable(ident, metadata, client)
  }
  
  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    // Handle schema evolution
    changes.foreach {
      case add: TableChange.AddColumn =>
        client.addColumn(
          ident.namespace().mkString("."),
          ident.name(),
          add.fieldNames().last,
          sparkTypeToSuperTable(add.dataType().simpleString)
        )
      case drop: TableChange.DeleteColumn =>
        client.dropColumn(
          ident.namespace().mkString("."),
          ident.name(),
          drop.fieldNames().last
        )
      case rename: TableChange.RenameColumn =>
        client.renameColumn(
          ident.namespace().mkString("."),
          ident.name(),
          rename.fieldNames().last,
          rename.newName()
        )
      case _ => // Other changes not yet supported
    }
    loadTable(ident)
  }
  
  override def dropTable(ident: Identifier): Boolean = {
    client.dropTable(ident.namespace().mkString("."), ident.name())
    true
  }
  
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Table rename not yet supported")
  }
  
  // ==================== Namespace Operations ====================
  
  override def listNamespaces(): Array[Array[String]] = {
    client.listNamespaces().map(ns => Array(ns)).toArray
  }
  
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    client.listNamespaces().filter(_.startsWith(namespace.mkString("."))).map(ns => Array(ns)).toArray
  }
  
  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    new util.HashMap[String, String]()
  }
  
  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    client.createNamespace(namespace.mkString("."))
  }
  
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    // Not yet implemented
  }
  
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    // Not yet implemented - would need to drop all tables first if cascade
    false
  }
  
  override def namespaceExists(namespace: Array[String]): Boolean = {
    listNamespaces().exists(_.sameElements(namespace))
  }
  
  // ==================== Schema Conversion ====================
  
  private def schemaToJson(schema: StructType): String = {
    import io.circe.syntax._
    import io.circe.generic.auto._
    
    case class FieldDef(id: Int, name: String, `type`: String, required: Boolean)
    case class SchemaDef(fields: Seq[FieldDef])
    
    val fields = schema.fields.zipWithIndex.map { case (f, idx) =>
      FieldDef(idx + 1, f.name, sparkTypeToSuperTable(f.dataType.simpleString), !f.nullable)
    }.toSeq
    
    SchemaDef(fields).asJson.noSpaces
  }
  
  private def sparkTypeToSuperTable(sparkType: String): String = sparkType.toLowerCase match {
    case "long" | "bigint" => "long"
    case "integer" | "int" => "int"
    case "string" => "string"
    case "boolean" => "boolean"
    case "float" => "float"
    case "double" => "double"
    case "date" => "date"
    case "timestamp" => "timestamp"
    case "binary" => "binary"
    case other => s"string" // Default fallback
  }
}
