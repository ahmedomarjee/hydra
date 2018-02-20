package hydra.sql

import java.sql.BatchUpdateException

import com.zaxxer.hikari.HikariDataSource
import hydra.avro.io.SaveMode.SaveMode
import hydra.avro.io._
import hydra.avro.util.{AvroUtils, SchemaWrapper}
import hydra.common.util.TryWith
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.NonFatal


/**
  * Created by alexsilva on 7/11/17.
  *
  * A batch size of 0 means that this class will never do any executeBatch and that external clients need to call
  * flush()
  *
  * If the primary keys are provided as a constructor argument, it overrides anything that
  * may have been provided by the schema.
  *
  * @param dataSource      The datasource to be used
  * @param schema   The initial schema to use when creating/updating/inserting records.
  * @param mode            See [hydra.avro.io.SaveMode]
  * @param dialect         The jdbc dialect to use.
  * @param dbSyntax        THe database syntax to use.
  * @param batchSize       The commit batch size; -1 to disable auto batching.
  * @param tableIdentifier The table identifier; defaults to using the schema's name if none provided.
  */
class JdbcRecordWriter(val dataSource: HikariDataSource,
                       val schema: SchemaWrapper,
                       val mode: SaveMode = SaveMode.ErrorIfExists,
                       dialect: JdbcDialect,
                       dbSyntax: DbSyntax = UnderscoreSyntax,
                       batchSize: Int = 50,
                       tableIdentifier: Option[TableIdentifier] = None) extends RecordWriter with JdbcHelper {

  private val store: Catalog = new JdbcCatalog(dataSource, dbSyntax, dialect)

  private val tableId = tableIdentifier.getOrElse(TableIdentifier(schema.getName))

  private val records = new mutable.ArrayBuffer[GenericRecord]()

  private var currentSchema = schema

  private val tableObj: Table = {
    val tableExists = store.tableExists(tableId)
    mode match {
      case SaveMode.ErrorIfExists if tableExists =>
        throw new AnalysisException(s"Table ${tableId.table} already exists.")
      case SaveMode.Overwrite => //todo: truncate table
        Table(tableId.table, schema, tableId.database)
      case _ =>
        val table = Table(tableId.table, schema, tableId.database)
        store.createOrAlterTable(table)
        table
    }
  }

  private val name = dbSyntax.format(tableObj.name)

  private var valueSetter = new AvroValueSetter(schema, dialect)

  private var upsertStmt = dialect.upsert(dbSyntax.format(name), schema, dbSyntax)

  // private val deleteStmt = dialect.deleteStatement(dbSyntax.format(name),, dbSyntax)

  private var _conn = dataSource.getConnection

  private def connection = {
    if (_conn.isClosed) {
      _conn = dataSource.getConnection
    }
    _conn
  }

  override def batch(operation: Operation): Unit = {
    operation match {
      case Upsert(record) => add(record)
      case Delete(schema, fields) => throw new UnsupportedOperationException("Not supported")
    }
  }

  private def add(record: GenericRecord): Unit = {
    if (AvroUtils.areEqual(currentSchema.schema, record.getSchema)) {
      records += record
      if (batchSize > 0 && records.size >= batchSize) flush()
    }
    else {
      // Each batch needs to have the same dbInfo, so get the buffered records out, reset state if possible,
      // add columns and re-attempt the add
      flush()
      updateDb(record)
      add(record)
    }
  }

  private def updateDb(record: GenericRecord): Unit = synchronized {
    val cpks = currentSchema.primaryKeys
    val wrapper = SchemaWrapper.from(record.getSchema, cpks)
    store.createOrAlterTable(Table(tableId.table, wrapper))
    currentSchema = wrapper
    upsertStmt = dialect.upsert(dbSyntax.format(name), currentSchema, dbSyntax)
    valueSetter = new AvroValueSetter(currentSchema, dialect)
  }

  /**
    * Convenience method to write exactly one record to the underlying database.
    *
    * @param record
    */
  private def upsert(record: GenericRecord): Unit = {
    if (AvroUtils.areEqual(currentSchema.schema, record.getSchema)) {
      TryWith(connection.prepareStatement(upsertStmt)) { pstmt =>
        valueSetter.bind(record, pstmt)
        pstmt.executeUpdate()
      }.get //TODO: better error handling here, we do the get just so that we throw an exception if there is one.
    }
    else {
      updateDb(record)
      upsert(record)
    }
  }

  override def execute(operation: Operation): Unit = {
    operation match {
      case Upsert(record) => upsert(record)
      case Delete(schema, fields) => throw new UnsupportedOperationException("Not supported")
    }
  }

  def flush(): Unit = synchronized {
    withConnection(dataSource.getConnection) { conn =>
      val supportsTransactions = try {
        conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
          conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()

      } catch {
        case NonFatal(e) =>
          JdbcRecordWriter.logger.warn("Exception while detecting transaction support", e)
          true
      }

      var committed = false

      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val pstmt = conn.prepareStatement(upsertStmt)
      records.foreach(valueSetter.bind(_, pstmt))
      try {
        pstmt.executeBatch()
        if (supportsTransactions) {
          conn.commit()
        }
        committed = true
      }
      catch {
        case e: BatchUpdateException =>
          JdbcRecordWriter.logger.error("Batch update error", e.getNextException()); throw e
        case e: Exception => throw e
      }
      finally {
        if (!committed && supportsTransactions) {
          conn.rollback()
        }
      }
      records.clear()
    }
  }

  def close(): Unit = {
    flush()
  }
}

object JdbcRecordWriter {

  val logger = LoggerFactory.getLogger(getClass)
}