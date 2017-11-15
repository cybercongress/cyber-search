package fund.cyber.pump.cassandra

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

class BookRepository(private val session: Session) {

    /**
     * Creates the books table.
     */
//    fun createTable() {
//        val sb = StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME).append("(").append("id uuid PRIMARY KEY, ").append("title text,").append("author text,").append("subject text);")
//
//        val query = sb.toString()
//        session.execute(query)
//    }
//
//    fun createTable(withName: String) {
//        val query = "CREATE TABLE IF NOT EXISTS $withName ()"
//    }
//
//    /**
//     * Creates the books table.
//     */
//    fun createTableBooksByTitle() {
//        val sb = StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME_BY_TITLE).append("(").append("id uuid, ").append("title text,").append("PRIMARY KEY (title, id));")
//
//        val query = sb.toString()
//        session.execute(query).executionInfo.
//    }
//
//    /**
//     * Alters the table books and adds an extra column.
//     */
//    fun alterTablebooks(columnName: String, columnType: String) {
//        val sb = StringBuilder("ALTER TABLE ").append(TABLE_NAME).append(" ADD ").append(columnName).append(" ").append(columnType).append(";")
//
//        val query = sb.toString()
//        session.execute(query)
//    }

//    /**
//     * Insert a row in the table books.
//     *
//     * @param book
//     */
//    fun insertbook(book: Book) {
//        val sb = StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(id, title, author, subject) ").append("VALUES (").append(book.getId()).append(", '").append(book.getTitle()).append("', '").append(book.getAuthor()).append("', '")
//                .append(book.getSubject()).append("');")
//
//        val query = sb.toString()
//        session.execute(query)
//    }
//
//    /**
//     * Insert a row in the table booksByTitle.
//     * @param book
//     */
//    fun insertbookByTitle(book: Book) {
//        val sb = StringBuilder("INSERT INTO ").append(TABLE_NAME_BY_TITLE).append("(id, title) ").append("VALUES (").append(book.getId()).append(", '").append(book.getTitle()).append("');")
//
//        val query = sb.toString()
//        session.execute(query)
//    }
//
//    /**
//     * Insert a book into two identical tables using a batch query.
//     *
//     * @param book
//     */
//    fun insertBookBatch(book: Book) {
//        val sb = StringBuilder("BEGIN BATCH ").append("INSERT INTO ").append(TABLE_NAME).append("(id, title, author, subject) ").append("VALUES (").append(book.getId()).append(", '").append(book.getTitle()).append("', '").append(book.getAuthor())
//                .append("', '").append(book.getSubject()).append("');").append("INSERT INTO ").append(TABLE_NAME_BY_TITLE).append("(id, title) ").append("VALUES (").append(book.getId()).append(", '").append(book.getTitle()).append("');")
//                .append("APPLY BATCH;")
//
//        val query = sb.toString()
//        session.execute(query)
//    }
//
//    /**
//     * Select book by id.
//     *
//     * @return
//     */
//    fun selectByTitle(title: String): Book {
//        val sb = StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_TITLE).append(" WHERE title = '").append(title).append("';")
//
//        val query = sb.toString()
//
//        val rs = session.execute(query)
//
//        val books = ArrayList<Book>()
//
//        for (r in rs) {
//            val s = Book(r.getUUID("id"), r.getString("title"), null, null)
//            books.add(s)
//        }
//
//        return books[0]
//    }
//
//    /**
//     * Select all books from books
//     *
//     * @return
//     */
//    fun selectAll(): List<Book> {
//        val sb = StringBuilder("SELECT * FROM ").append(TABLE_NAME)
//
//        val query = sb.toString()
//        val rs = session.execute(query)
//
//        val books = ArrayList<Book>()
//
//        for (r in rs) {
//            val book = Book(r.getUUID("id"), r.getString("title"), r.getString("author"), r.getString("subject"))
//            books.add(book)
//        }
//        return books
//    }
//
//    /**
//     * Select all books from booksByTitle
//     * @return
//     */
//    fun selectAllBookByTitle(): List<Book> {
//        val sb = StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_TITLE)
//
//        val query = sb.toString()
//        val rs = session.execute(query)
//
//        val books = ArrayList<Book>()
//
//        for (r in rs) {
//            val book = Book(r.getUUID("id"), r.getString("title"), null, null)
//            books.add(book)
//        }
//        return books
//    }
//
//    /**
//     * Delete a book by title.
//     */
//    fun deletebookByTitle(title: String) {
//        val sb = StringBuilder("DELETE FROM ").append(TABLE_NAME_BY_TITLE).append(" WHERE title = '").append(title).append("';")
//
//        val query = sb.toString()
//        session.execute(query)
//    }
//
//    /**
//     * Delete table.
//     *
//     * @param tableName the name of the table to delete.
//     */
//    fun deleteTable(tableName: String) {
//        val sb = StringBuilder("DROP TABLE IF EXISTS ").append(tableName)
//
//        val query = sb.toString()
//        session.execute(query)
//    }
//
//    companion object {
//
//        private val TABLE_NAME = "books"
//
//        private val TABLE_NAME_BY_TITLE = TABLE_NAME + "ByTitle"
//    }
}

//import com.datastax.driver.core.Session
//
//private var schemaRepository: KeyspaceRepository? = null
//private var session: Session? = null
//
////@Before
//fun connect() {
//    val client = CassandraConnector()
//    client.connect("127.0.0.1", 9142)
//    this.session = client.session
//    schemaRepository = KeyspaceRepository(session)
//}
//
//fun createKeyspace(
//        keyspaceName: String, replicationStrategy: String, replicationFactor: Int) {
//    val sb = StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
//            .append(keyspaceName).append(" WITH replication = {")
//            .append("'class':'").append(replicationStrategy)
//            .append("','replication_factor':").append(replicationFactor)
//            .append("};")
//
//    val query = sb.toString()
//    session.execute(query)
//}