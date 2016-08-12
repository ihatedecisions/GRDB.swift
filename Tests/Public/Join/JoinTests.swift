import XCTest
#if USING_SQLCIPHER
    import GRDBCipher
#elseif USING_CUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

class JoinTests: GRDBTestCase {
    func testAvailableVariantsWithNestedRelations() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                // a <- b <- c <- d
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, bID REFERENCES b(id))")
                try db.execute("CREATE TABLE d (id INTEGER PRIMARY KEY, cID REFERENCES c(id))")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                try db.execute("INSERT INTO b (id, aID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                try db.execute("INSERT INTO c (id, bID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                try db.execute("INSERT INTO d (id, cID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                
                let b = ForeignRelation(to: "b", through: ["id": "aID"])
                let c = ForeignRelation(to: "c", through: ["id": "bID"])
                let d = ForeignRelation(to: "d", through: ["id": "cID"])
                
                struct A : TableMapping {
                    static func databaseTableName() -> String { return "a" }
                }
                
                do {
                    let request = A.join(b)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                }
                
                do {
                    let request = A.include(b)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                    
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                }
                
                do {
                    let request = A.join(b.join(c))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                }
                
                do {
                    let request = A.include(b.join(c))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                    
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                }
                
                do {
                    let request = A.join(b.include(c))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                    
                    XCTAssertTrue(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.isEmpty)
                }
                
                do {
                    let request = A.include(b.include(c))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                    
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.isEmpty)
                }
                
                do {
                    let request = A.join(b.join(c.join(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "LEFT JOIN \"d\" ON (\"d\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                }
                
                do {
                    let request = A.include(b.join(c.join(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "LEFT JOIN \"d\" ON (\"d\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") == nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                    
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                }
                
                do {
                    let request = A.join(b.include(c.join(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "LEFT JOIN \"d\" ON (\"d\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                    
                    XCTAssertTrue(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.isEmpty)
                }
                
                do {
                    let request = A.include(b.include(c.join(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "LEFT JOIN \"d\" ON (\"d\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") == nil)
                    
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.isEmpty)
                }
                
                do {
                    let request = A.join(b.join(c.include(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"d\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "LEFT JOIN \"d\" ON (\"d\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") != nil)
                    
                    XCTAssertTrue(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "b")!.scoped(on: "c")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.scoped(on: "d")!.isEmpty)
                }
                
                do {
                    let request = A.include(b.join(c.include(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"d\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "LEFT JOIN \"d\" ON (\"d\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") != nil)
                    
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "b")!.scoped(on: "c")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.scoped(on: "d")!.isEmpty)
                }
                
                do {
                    let request = A.join(b.include(c.include(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c\".*, \"d\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "LEFT JOIN \"d\" ON (\"d\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") != nil)
                    
                    XCTAssertTrue(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.scoped(on: "d")!.isEmpty)
                }
                
                do {
                    let request = A.include(b.include(c.include(d)))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".*, \"d\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "LEFT JOIN \"d\" ON (\"d\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c") != nil)
                    XCTAssertTrue(row.scoped(on: "b")?.scoped(on: "c")?.scoped(on: "d") != nil)
                    
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "c")!.scoped(on: "d")!.isEmpty)
                }
            }
        }
    }
    
    func testAvailableVariantsWithSiblingRelations() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, a1ID REFERENCES a(id), a2ID REFERENCES a(id))")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                let a1ID = db.lastInsertedRowID
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                let a2ID = db.lastInsertedRowID
                try db.execute("INSERT INTO b (id, aID) VALUES (NULL, ?)", arguments: [a1ID])
                try db.execute("INSERT INTO c (id, a1ID, a2ID) VALUES (NULL, ?, ?)", arguments: [a1ID, a2ID])
                
                let b = ForeignRelation(to: "b", through: ["id": "aID"])
                let c1 = ForeignRelation(named: "c1", to: "c", through: ["id": "a1ID"])
                let c2 = ForeignRelation(named: "c2", to: "c", through: ["id": "a2ID"])
                
                struct A : TableMapping {
                    static func databaseTableName() -> String { return "a" }
                }
                
                do {
                    let request = A.join(b, c1, c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.join(b).join(c1, c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.join(b, c1).join(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.join(b).join(c1).join(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.join(b, c1).include(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
                
                do {
                    let request = A.join(b).join(c1).include(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
                
                do {
                    let request = A.join(b).include(c1).join(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c1\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.join(b).include(c1, c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c1\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
                
                do {
                    let request = A.join(b).include(c1).include(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"c1\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
                
                do {
                    let request = A.include(b).join(c1, c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.include(b).join(c1).join(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.include(b).join(c1).include(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c1") == nil)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
                
                do {
                    let request = A.include(b, c1).join(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c1\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.include(b).include(c1).join(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c1\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c2") == nil)
                }
                
                do {
                    let request = A.include(b, c1, c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c1\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
                
                do {
                    let request = A.include(b).include(c1, c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c1\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
                
                do {
                    let request = A.include(b, c1).include(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c1\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
                
                do {
                    let request = A.include(b).include(c1).include(c2)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c1\".*, \"c2\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" \"c1\" ON (\"c1\".\"a1ID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"c\" \"c2\" ON (\"c2\".\"a2ID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c1")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c2")!.isEmpty)
                }
            }
        }
    }
    
    func testAvailableVariantsWithDiamondRelations() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                // a <- b <- d
                // a <- c <- d
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, aID REFERENCES b(id))")
                try db.execute("CREATE TABLE d (id INTEGER PRIMARY KEY, bID REFERENCES b(id), cID REFERENCES c(id))")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                let aID = db.lastInsertedRowID
                try db.execute("INSERT INTO b (id, aID) VALUES (NULL, ?)", arguments: [aID])
                let bID = db.lastInsertedRowID
                try db.execute("INSERT INTO c (id, aID) VALUES (NULL, ?)", arguments: [aID])
                let cID = db.lastInsertedRowID
                try db.execute("INSERT INTO d (id, bID, cID) VALUES (NULL, ?, ?)", arguments: [bID, cID])
                
                let b = ForeignRelation(to: "b", through: ["id": "aID"])
                let c = ForeignRelation(to: "c", through: ["id": "aID"])
                let bd = ForeignRelation(to: "d", through: ["id": "bID"])
                let cd = ForeignRelation(to: "d", through: ["id": "cID"])
                
                struct A : TableMapping {
                    static func databaseTableName() -> String { return "a" }
                }
                
                do {
                    let request = A.join(b.join(bd), c.join(cd))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"d\" \"d0\" ON (\"d0\".\"bID\" = \"b\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"d\" \"d1\" ON (\"d1\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "c") == nil)
                }
                
                do {
                    let request = A.include(b.join(bd), c.join(cd))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"d\" \"d0\" ON (\"d0\".\"bID\" = \"b\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"d\" \"d1\" ON (\"d1\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "b")!.scoped(on: "d") == nil)
                    XCTAssertFalse(row.scoped(on: "c")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c")!.scoped(on: "d") == nil)
                }
                
                do {
                    let request = A.join(b.include(bd), c.join(cd))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"d0\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"d\" \"d0\" ON (\"d0\".\"bID\" = \"b\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"d\" \"d1\" ON (\"d1\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "d")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c") == nil)
                }
                
                do {
                    let request = A.include(b.include(bd), c.join(cd))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"d0\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"d\" \"d0\" ON (\"d0\".\"bID\" = \"b\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"d\" \"d1\" ON (\"d1\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "d")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c")!.scoped(on: "d") == nil)
                }
                
                do {
                    let request = A.join(b.join(bd), c.include(cd))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"d1\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"d\" \"d0\" ON (\"d0\".\"bID\" = \"b\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"d\" \"d1\" ON (\"d1\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") == nil)
                    XCTAssertTrue(row.scoped(on: "c")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c")!.scoped(on: "d")!.isEmpty)
                }
                
                do {
                    let request = A.include(b.join(bd), c.include(cd))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".*, \"d1\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"d\" \"d0\" ON (\"d0\".\"bID\" = \"b\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"d\" \"d1\" ON (\"d1\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "b")!.scoped(on: "d") == nil)
                    XCTAssertFalse(row.scoped(on: "c")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c")!.scoped(on: "d")!.isEmpty)
                }
                
                do {
                    let request = A.join(b.include(bd), c.include(cd))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"d0\".*, \"d1\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"d\" \"d0\" ON (\"d0\".\"bID\" = \"b\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"d\" \"d1\" ON (\"d1\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "d")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "c")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c")!.scoped(on: "d")!.isEmpty)
                }
                
                do {
                    let request = A.include(b.include(bd), c.include(cd))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"d0\".*, \"c\".*, \"d1\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"d\" \"d0\" ON (\"d0\".\"bID\" = \"b\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"aID\" = \"a\".\"id\") " +
                        "LEFT JOIN \"d\" \"d1\" ON (\"d1\".\"cID\" = \"c\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "d")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c")!.isEmpty)
                    XCTAssertFalse(row.scoped(on: "c")!.scoped(on: "d")!.isEmpty)
                }
            }
        }
    }
    
    func testRelationVariantNameAndAlias() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                try db.execute("INSERT INTO b (id, aID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                
                let bRelationUnnamed = ForeignRelation(to: "b", through: ["id": "aID"])
                let bRelationNamedAsTable = ForeignRelation(named: "b", to: "b", through: ["id": "aID"])
                let bRelationNamed = ForeignRelation(named: "bVariant", to: "b", through: ["id": "aID"])
                
                struct A : TableMapping {
                    static func databaseTableName() -> String { return "a" }
                }
                
                do {
                    let request = A.include(bRelationUnnamed)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                }
                
                do {
                    let request = A.include(bRelationUnnamed.aliased("bAlias"))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"bAlias\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" \"bAlias\" ON (\"bAlias\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                }
                
                do {
                    let request = A.include(bRelationNamedAsTable)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                }
                
                do {
                    let request = A.include(bRelationNamedAsTable.aliased("bAlias"))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"bAlias\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" \"bAlias\" ON (\"bAlias\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                }
                
                do {
                    let request = A.include(bRelationNamed)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"bVariant\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" \"bVariant\" ON (\"bVariant\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "bVariant") != nil)
                    XCTAssertFalse(row.scoped(on: "bVariant")!.isEmpty)
                }
                
                do {
                    let request = A.include(bRelationNamed.aliased("bAlias"))
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"bAlias\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" \"bAlias\" ON (\"bAlias\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "bVariant") != nil)
                    XCTAssertFalse(row.scoped(on: "bVariant")!.isEmpty)
                }
            }
        }
    }
    
//    func testRelationFilter() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inDatabase { db in
//                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, foo TEXT)")
//                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id), bar TEXT, foo TEXT)")
//                try db.execute("INSERT INTO a (id, foo) VALUES (NULL, ?)", arguments: ["foo"])
//                try db.execute("INSERT INTO b (id, aID, bar, foo) VALUES (NULL, ?, ?, ?)", arguments: [db.lastInsertedRowID, "bar", "foo"])
//                
//                let barColumn = SQLColumn("bar")
//                let bRelationUnnamed = ForeignRelation(to: "b", through: ["id": "aID"])
//                let bRelationNamedAsTable = ForeignRelation(named: "b", to: "b", through: ["id": "aID"])
//                let bRelationNamed = ForeignRelation(named: "bVariant", to: "b", through: ["id": "aID"])
//                
//                struct A : TableMapping {
//                    static func databaseTableName() -> String { return "a" }
//                }
//                
//                do {
//                    let request = A.include(bRelationUnnamed.filter { $0["foo"] == "foo" && $0[barColumn] == "bar" })
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"b\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" ON ((\"b\".\"aID\" = \"a\".\"id\") AND ((\"b\".\"foo\" = 'foo') AND (\"b\".\"bar\" = 'bar')))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "b") != nil)
//                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationUnnamed.aliased("bAlias").filter { $0["foo"] == "foo" && $0[barColumn] == "bar" })
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"bAlias\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" \"bAlias\" ON ((\"bAlias\".\"aID\" = \"a\".\"id\") AND ((\"bAlias\".\"foo\" = 'foo') AND (\"bAlias\".\"bar\" = 'bar')))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "b") != nil)
//                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationNamedAsTable.filter { $0["foo"] == "foo" && $0[barColumn] == "bar" })
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"b\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" ON ((\"b\".\"aID\" = \"a\".\"id\") AND ((\"b\".\"foo\" = 'foo') AND (\"b\".\"bar\" = 'bar')))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "b") != nil)
//                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationNamedAsTable.aliased("bAlias").filter { $0["foo"] == "foo" && $0[barColumn] == "bar" })
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"bAlias\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" \"bAlias\" ON ((\"bAlias\".\"aID\" = \"a\".\"id\") AND ((\"bAlias\".\"foo\" = 'foo') AND (\"bAlias\".\"bar\" = 'bar')))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "b") != nil)
//                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationNamed.filter { $0["foo"] == "foo" && $0[barColumn] == "bar" })
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"bVariant\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" \"bVariant\" ON ((\"bVariant\".\"aID\" = \"a\".\"id\") AND ((\"bVariant\".\"foo\" = 'foo') AND (\"bVariant\".\"bar\" = 'bar')))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "bVariant") != nil)
//                    XCTAssertFalse(row.scoped(on: "bVariant")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationNamed.aliased("bAlias").filter { $0["foo"] == "foo" && $0[barColumn] == "bar" })
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"bAlias\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" \"bAlias\" ON ((\"bAlias\".\"aID\" = \"a\".\"id\") AND ((\"bAlias\".\"foo\" = 'foo') AND (\"bAlias\".\"bar\" = 'bar')))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "bVariant") != nil)
//                    XCTAssertFalse(row.scoped(on: "bVariant")!.isEmpty)
//                }
//            }
//        }
//    }
    
//    func testRelationFilterLiteral() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inDatabase { db in
//                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, foo TEXT)")
//                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id), bar TEXT)")
//                try db.execute("INSERT INTO a (id, foo) VALUES (NULL, ?)", arguments: ["foo"])
//                try db.execute("INSERT INTO b (id, aID, bar) VALUES (NULL, ?, ?)", arguments: [db.lastInsertedRowID, "bar"])
//                
//                let bRelationUnnamed = ForeignRelation(to: "b", through: ["id": "aID"])
//                let bRelationNamedAsTable = ForeignRelation(named: "b", to: "b", through: ["id": "aID"])
//                let bRelationNamed = ForeignRelation(named: "bVariant", to: "b", through: ["id": "aID"])
//                
//                struct A : TableMapping {
//                    static func databaseTableName() -> String { return "a" }
//                }
//                
//                do {
//                    let request = A.include(bRelationUnnamed.filter(sql: "b.bar = ?", arguments: ["bar"]))
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"b\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" ON ((\"b\".\"aID\" = \"a\".\"id\") AND (b.bar = 'bar'))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "b") != nil)
//                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationUnnamed.aliased("bAlias").filter(sql: "bAlias.bar = ?", arguments: ["bar"]))
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"bAlias\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" \"bAlias\" ON ((\"bAlias\".\"aID\" = \"a\".\"id\") AND (bAlias.bar = 'bar'))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "b") != nil)
//                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationNamedAsTable.filter(sql: "b.bar = ?", arguments: ["bar"]))
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"b\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" ON ((\"b\".\"aID\" = \"a\".\"id\") AND (b.bar = 'bar'))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "b") != nil)
//                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationNamedAsTable.aliased("bAlias").filter(sql: "bAlias.bar = ?", arguments: ["bar"]))
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"bAlias\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" \"bAlias\" ON ((\"bAlias\".\"aID\" = \"a\".\"id\") AND (bAlias.bar = 'bar'))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "b") != nil)
//                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationNamed.filter(sql: "bVariant.bar = ?", arguments: ["bar"]))
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"bVariant\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" \"bVariant\" ON ((\"bVariant\".\"aID\" = \"a\".\"id\") AND (bVariant.bar = 'bar'))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "bVariant") != nil)
//                    XCTAssertFalse(row.scoped(on: "bVariant")!.isEmpty)
//                }
//                
//                do {
//                    let request = A.include(bRelationNamed.aliased("bAlias").filter(sql: "bAlias.bar = ?", arguments: ["bar"]))
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"bAlias\".* " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" \"bAlias\" ON ((\"bAlias\".\"aID\" = \"a\".\"id\") AND (bAlias.bar = 'bar'))")
//                    
//                    let row = Row.fetchOne(db, request)!
//                    XCTAssertTrue(row.scoped(on: "bVariant") != nil)
//                    XCTAssertFalse(row.scoped(on: "bVariant")!.isEmpty)
//                }
//            }
//        }
//    }
    
    func testRelationWithConflict() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inTransaction { db in
                try db.execute("PRAGMA defer_foreign_keys = ON")
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, bID REFERENCES b(id))")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("INSERT INTO a (id, bID) VALUES (?, ?)", arguments: [1, 1])
                try db.execute("INSERT INTO b (id, aID) VALUES (?, ?)", arguments: [1, 1])
                return .Commit
            }
            
            let bRelation = ForeignRelation(to: "b", through: ["id": "aID"])
            let aRelation = ForeignRelation(to: "a", through: ["id": "bID"])
            
            struct A : TableMapping {
                static func databaseTableName() -> String { return "a" }
            }
            
            dbQueue.inDatabase { db in
                let request = A.include(bRelation.include(aRelation))
                XCTAssertEqual(
                    self.sql(db, request),
                    "SELECT \"a0\".*, \"b\".*, \"a1\".* " +
                        "FROM \"a\" \"a0\" " +
                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a0\".\"id\") " +
                    "LEFT JOIN \"a\" \"a1\" ON (\"a1\".\"bID\" = \"b\".\"id\")")
                
                let row = Row.fetchOne(db, request)!
                XCTAssertTrue(row.scoped(on: "b") != nil)
                XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                XCTAssertTrue(row.scoped(on: "b")!.scoped(on: "a") != nil)
                XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "a")!.isEmpty)
            }
            
            dbQueue.inDatabase { db in
                let request = A.include(bRelation.include(aRelation.include(bRelation)))
                XCTAssertEqual(
                    self.sql(db, request),
                    "SELECT \"a0\".*, \"b0\".*, \"a1\".*, \"b1\".* " +
                        "FROM \"a\" \"a0\" " +
                        "LEFT JOIN \"b\" \"b0\" ON (\"b0\".\"aID\" = \"a0\".\"id\") " +
                        "LEFT JOIN \"a\" \"a1\" ON (\"a1\".\"bID\" = \"b0\".\"id\") " +
                    "LEFT JOIN \"b\" \"b1\" ON (\"b1\".\"aID\" = \"a1\".\"id\")")
                
                let row = Row.fetchOne(db, request)!
                XCTAssertTrue(row.scoped(on: "b") != nil)
                XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                XCTAssertTrue(row.scoped(on: "b")!.scoped(on: "a") != nil)
                XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "a")!.isEmpty)
                XCTAssertTrue(row.scoped(on: "b")!.scoped(on: "a")!.scoped(on: "b") != nil)
                XCTAssertFalse(row.scoped(on: "b")!.scoped(on: "a")!.scoped(on: "b")!.isEmpty)
            }
        }
    }
    
//    func testRelationFilterWithConflict() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inTransaction { db in
//                try db.execute("PRAGMA defer_foreign_keys = ON")
//                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, bID REFERENCES b(id), foo TEXT)")
//                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id), bar TEXT)")
//                try db.execute("INSERT INTO a (id, bID, foo) VALUES (?, ?, ?)", arguments: [1, 1, "foo"])
//                try db.execute("INSERT INTO b (id, aID, bar) VALUES (?, ?, ?)", arguments: [1, 1, "bar"])
//                return .Commit
//            }
//            
//            let bRelation = ForeignRelation(to: "b", through: ["id": "aID"])
//            let aRelation = ForeignRelation(to: "a", through: ["id": "bID"])
//            
//            struct A : TableMapping {
//                static func databaseTableName() -> String { return "a" }
//            }
//            
//            dbQueue.inDatabase { db in
//                let request = A
//                    .filter { $0["foo"] == "foo1" }
//                    .include(bRelation
//                        .filter { $0["bar"] == "bar" }
//                        .include(aRelation
//                            .filter { $0["foo"] == "foo2" }))
//                XCTAssertEqual(
//                    self.sql(db, request),
//                    "SELECT \"a0\".*, \"b\".*, \"a1\".* " +
//                        "FROM \"a\" \"a0\" " +
//                        "LEFT JOIN \"b\" ON ((\"b\".\"aID\" = \"a0\".\"id\") AND (\"b\".\"bar\" = 'bar')) " +
//                        "LEFT JOIN \"a\" \"a1\" ON ((\"a1\".\"bID\" = \"b\".\"id\") AND (\"a1\".\"foo\" = 'foo2')) " +
//                    "WHERE (\"a0\".\"foo\" = 'foo1')")
//            }
//            
//            dbQueue.inDatabase { db in
//                let request = A
//                    .filter { $0["foo"] == "foo1" }
//                    .include(bRelation
//                        .filter { $0["bar"] == "bar1" }
//                        .include(aRelation
//                            .filter { $0["foo"] == "foo2" }
//                            .include(bRelation
//                                .filter { $0["bar"] == "bar2" })))
//                XCTAssertEqual(
//                    self.sql(db, request),
//                    "SELECT \"a0\".*, \"b0\".*, \"a1\".*, \"b1\".* " +
//                        "FROM \"a\" \"a0\" " +
//                        "LEFT JOIN \"b\" \"b0\" ON ((\"b0\".\"aID\" = \"a0\".\"id\") AND (\"b0\".\"bar\" = 'bar1')) " +
//                        "LEFT JOIN \"a\" \"a1\" ON ((\"a1\".\"bID\" = \"b0\".\"id\") AND (\"a1\".\"foo\" = 'foo2')) " +
//                        "LEFT JOIN \"b\" \"b1\" ON ((\"b1\".\"aID\" = \"a1\".\"id\") AND (\"b1\".\"bar\" = 'bar2')) " +
//                    "WHERE (\"a0\".\"foo\" = 'foo1')")
//            }
//        }
//    }
    
    func testFirstLevelRequiredRelation() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                
                let bRelation = ForeignRelation(to: "b", through: ["id": "aID"])
                
                struct A : TableMapping {
                    static func databaseTableName() -> String { return "a" }
                }
                
                do {
                    let request = A.include(bRelation)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "b")!.value(named: "id") == nil)
                }
                
                do {
                    let request = A.include(required: false, bRelation)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)!
                    XCTAssertTrue(row.scoped(on: "b") != nil)
                    XCTAssertFalse(row.scoped(on: "b")!.isEmpty)
                    XCTAssertTrue(row.scoped(on: "b")!.value(named: "id") == nil)
                }
                
                do {
                    let request = A.include(required: true, bRelation)
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".* " +
                            "FROM \"a\" " +
                        "JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\")")
                    
                    let row = Row.fetchOne(db, request)
                    XCTAssertTrue(row == nil)
                }
            }
        }
    }
    
    func testTwoLevelsRequiredRelation() {
        assertNoError {
            let dbQueue = try makeDatabaseQueue()
            try dbQueue.inDatabase { db in
                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id))")
                try db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, bID REFERENCES b(id))")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                try db.execute("INSERT INTO b (id, aID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                try db.execute("INSERT INTO a (id) VALUES (NULL)")
                try db.execute("INSERT INTO b (id, aID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                try db.execute("INSERT INTO c (id, bID) VALUES (NULL, ?)", arguments: [db.lastInsertedRowID])
                
                let bRelation = ForeignRelation(to: "b", through: ["id": "aID"])
                let cRelation = ForeignRelation(to: "c", through: ["id": "bID"])
                
                struct A : TableMapping {
                    static func databaseTableName() -> String { return "a" }
                }
                
                do {
                    let request = A.include(bRelation.include(cRelation)).order(sql: "a.id, b.id, c.id")
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "ORDER BY a.id, b.id, c.id")
                    
                    let rows = Row.fetchAll(db, request)
                    XCTAssertEqual(rows.count, 3)
                }
                
                do {
                    let request = A.include(bRelation.include(required: false, cRelation)).order(sql: "a.id, b.id, c.id")
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "ORDER BY a.id, b.id, c.id")
                    
                    let rows = Row.fetchAll(db, request)
                    XCTAssertEqual(rows.count, 3)
                }
                
                do {
                    let request = A.include(bRelation.include(required: true, cRelation)).order(sql: "a.id, b.id, c.id")
                    _ = try request.prepare(db)
                    XCTFail("Expected DatabaseError")
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.code, 21) // SQLITE_MISUSE
                }
                
                do {
                    let request = A.include(required: true, bRelation.include(cRelation)).order(sql: "a.id, b.id, c.id")
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "ORDER BY a.id, b.id, c.id")
                    
                    let rows = Row.fetchAll(db, request)
                    XCTAssertEqual(rows.count, 2)
                }
                
                do {
                    let request = A.include(required: true, bRelation.include(required: false, cRelation)).order(sql: "a.id, b.id, c.id")
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "ORDER BY a.id, b.id, c.id")
                    
                    let rows = Row.fetchAll(db, request)
                    XCTAssertEqual(rows.count, 2)
                }
                
                do {
                    let request = A.include(required: true, bRelation.include(required: true, cRelation)).order(sql: "a.id, b.id, c.id")
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "ORDER BY a.id, b.id, c.id")
                    
                    let rows = Row.fetchAll(db, request)
                    XCTAssertEqual(rows.count, 1)
                }
                
                do {
                    let request = A.include(required: false, bRelation.include(cRelation)).order(sql: "a.id, b.id, c.id")
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "ORDER BY a.id, b.id, c.id")
                    
                    let rows = Row.fetchAll(db, request)
                    XCTAssertEqual(rows.count, 3)
                }
                
                do {
                    let request = A.include(required: false, bRelation.include(required: false, cRelation)).order(sql: "a.id, b.id, c.id")
                    XCTAssertEqual(
                        self.sql(db, request),
                        "SELECT \"a\".*, \"b\".*, \"c\".* " +
                            "FROM \"a\" " +
                            "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\") " +
                            "LEFT JOIN \"c\" ON (\"c\".\"bID\" = \"b\".\"id\") " +
                        "ORDER BY a.id, b.id, c.id")
                    
                    let rows = Row.fetchAll(db, request)
                    XCTAssertEqual(rows.count, 3)
                }
                
                do {
                    let request = A.include(required: false, bRelation.include(required: true, cRelation)).order(sql: "a.id, b.id, c.id")
                    _ = try request.prepare(db)
                    XCTFail("Expected DatabaseError")
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.code, 21) // SQLITE_MISUSE
                }
            }
        }
    }
    
//    func testRelationSelection() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inDatabase { db in
//                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, foo TEXT)")
//                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id), bar TEXT, foo TEXT)")
//                try db.execute("INSERT INTO a (id, foo) VALUES (NULL, ?)", arguments: ["foo"])
//                try db.execute("INSERT INTO b (id, aID, bar, foo) VALUES (NULL, ?, ?, ?)", arguments: [db.lastInsertedRowID, "bar", "foo"])
//                
//                let barColumn = SQLColumn("bar")
//                let bRelation = ForeignRelation(to: "b", through: ["id": "aID"])
//                
//                struct A : TableMapping {
//                    static func databaseTableName() -> String { return "a" }
//                }
//                
//                do {
//                    let request = A.include(bRelation.select { [$0["foo"], $0[barColumn]] })
//                    XCTAssertEqual(
//                        self.sql(db, request),
//                        "SELECT \"a\".*, \"b\".\"foo\", \"b\".\"bar\" " +
//                            "FROM \"a\" " +
//                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a\".\"id\")")
//                }
//            }
//        }
//    }
    
//    func testRelationSelectionWithConflict() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inTransaction { db in
//                try db.execute("PRAGMA defer_foreign_keys = ON")
//                try db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, bID REFERENCES b(id), foo TEXT)")
//                try db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, aID REFERENCES a(id), bar TEXT)")
//                try db.execute("INSERT INTO a (id, bID, foo) VALUES (?, ?, ?)", arguments: [1, 1, "foo"])
//                try db.execute("INSERT INTO b (id, aID, bar) VALUES (?, ?, ?)", arguments: [1, 1, "bar"])
//                return .Commit
//            }
//            
//            let bRelation = ForeignRelation(to: "b", through: ["id": "aID"])
//            let aRelation = ForeignRelation(to: "a", through: ["id": "bID"])
//            
//            struct A : TableMapping {
//                static func databaseTableName() -> String { return "a" }
//            }
//            
//            dbQueue.inDatabase { db in
//                let request = A
//                    .select { [$0["foo"]] }
//                    .include(bRelation
//                        .select { [$0["bar"]] }
//                        .include(aRelation
//                            .select { [$0["id"]] }))
//                XCTAssertEqual(
//                    self.sql(db, request),
//                    "SELECT \"a0\".\"foo\", \"b\".\"bar\", \"a1\".\"id\" " +
//                        "FROM \"a\" \"a0\" " +
//                        "LEFT JOIN \"b\" ON (\"b\".\"aID\" = \"a0\".\"id\") " +
//                    "LEFT JOIN \"a\" \"a1\" ON (\"a1\".\"bID\" = \"b\".\"id\")")
//            }
//            
//            dbQueue.inDatabase { db in
//                let request = A
//                    .select { [$0["foo"]] }
//                    .include(bRelation
//                        .select { [$0["bar"]] }
//                        .include(aRelation
//                            .select { [$0["id"]] }
//                            .include(bRelation
//                                .select { [$0["id"]] })))
//                XCTAssertEqual(
//                    self.sql(db, request),
//                    "SELECT \"a0\".\"foo\", \"b0\".\"bar\", \"a1\".\"id\", \"b1\".\"id\" " +
//                        "FROM \"a\" \"a0\" " +
//                        "LEFT JOIN \"b\" \"b0\" ON (\"b0\".\"aID\" = \"a0\".\"id\") " +
//                        "LEFT JOIN \"a\" \"a1\" ON (\"a1\".\"bID\" = \"b0\".\"id\") " +
//                    "LEFT JOIN \"b\" \"b1\" ON (\"b1\".\"aID\" = \"a1\".\"id\")")
//            }
//        }
//    }
    
//    func testDefaultRelationAliasWithInclude() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inDatabase { db in
//                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
//                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
//            }
//            
//            try dbQueue.inTransaction { db in
//                try db.execute("INSERT INTO countries (isoCode, name) VALUES (?, ?)", arguments: ["FR", "France"])
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (NULL, ?, ?)", arguments: ["Arthur", "FR"])
//                return .Commit
//            }
//            
//            let request = Person
//                .include(Person.birthCountry)
//                .filter(sql: "\(Person.birthCountry.name).isoCode = ?", arguments: ["FR"])
//            
//            XCTAssertEqual(
//                sql(dbQueue, request),
//                "SELECT \"persons\".*, \"birthCountry\".* " +
//                    "FROM \"persons\" " +
//                    "LEFT JOIN \"countries\" \"birthCountry\" ON (\"birthCountry\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\") " +
//                "WHERE (birthCountry.isoCode = 'FR')")
//            
//            dbQueue.inDatabase { db in
//                let persons = request.fetchAll(db)
//                XCTAssertEqual(persons.count, 1)
//                
//                XCTAssertEqual(persons[0].name, "Arthur")
//                XCTAssertEqual(persons[0].birthCountry!.name, "France")
//            }
//        }
//    }
    
//    func testDefaultRelationAliasWithJoin() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inDatabase { db in
//                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
//                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
//            }
//            
//            try dbQueue.inTransaction { db in
//                try db.execute("INSERT INTO countries (isoCode, name) VALUES (?, ?)", arguments: ["FR", "France"])
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (NULL, ?, ?)", arguments: ["Arthur", "FR"])
//                return .Commit
//            }
//            
//            let request = Person
//                .join(Person.birthCountry)
//                .filter(sql: "\(Person.birthCountry.name).isoCode = ?", arguments: ["FR"])
//            
//            XCTAssertEqual(
//                sql(dbQueue, request),
//                "SELECT \"persons\".* " +
//                    "FROM \"persons\" " +
//                    "LEFT JOIN \"countries\" \"birthCountry\" ON (\"birthCountry\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\") " +
//                "WHERE (birthCountry.isoCode = 'FR')")
//            
//            dbQueue.inDatabase { db in
//                let persons = request.fetchAll(db)
//                XCTAssertEqual(persons.count, 1)
//                
//                XCTAssertEqual(persons[0].name, "Arthur")
//                XCTAssertTrue(persons[0].birthCountry == nil)
//            }
//        }
//    }
    
//    func testExplicitRelationAlias() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inDatabase { db in
//                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
//                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
//            }
//            
//            try dbQueue.inTransaction { db in
//                try db.execute("INSERT INTO countries (isoCode, name) VALUES (?, ?)", arguments: ["FR", "France"])
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (NULL, ?, ?)", arguments: ["Arthur", "FR"])
//                return .Commit
//            }
//            
//            let request = Person
//                .include(Person.birthCountry.aliased("foo"))
//                .filter(sql: "foo.isoCode = ?", arguments: ["FR"])
//            XCTAssertEqual(
//                sql(dbQueue, request),
//                "SELECT \"persons\".*, \"foo\".* " +
//                    "FROM \"persons\" " +
//                    "LEFT JOIN \"countries\" \"foo\" ON (\"foo\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\") " +
//                "WHERE (foo.isoCode = 'FR')")
//            
//            dbQueue.inDatabase { db in
//                let persons = request.fetchAll(db)
//                XCTAssertEqual(persons.count, 1)
//                
//                XCTAssertEqual(persons[0].name, "Arthur")
//                XCTAssertEqual(persons[0].birthCountry!.name, "France")
//            }
//            
//            dbQueue.inDatabase { db in
//                let request = Person
//                    .include(Person.birthCountry.aliased("foo"))
//                    .filter(sql: "foo.isoCode = ?", arguments: ["US"])
//                let persons = request.fetchAll(db)
//                
//                XCTAssertEqual(persons.count, 0)
//            }
//        }
//    }
    
//    func testPersonToRuledCountryAndToBirthCountry() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inDatabase { db in
//                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
//                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, leaderID INTEGER REFERENCES persons(id))")
//            }
//            
//            try dbQueue.inTransaction { db in
//                try db.execute("PRAGMA defer_foreign_keys = ON")
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [1, "Arthur", "FR"])
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [2, "Barbara", "FR"])
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [3, "John", "US"])
//                try db.execute("INSERT INTO countries (isoCode, name, leaderID) VALUES (?, ?, ?)", arguments: ["FR", "France", 2])
//                try db.execute("INSERT INTO countries (isoCode, name, leaderID) VALUES (?, ?, ?)", arguments: ["US", "United States", 3])
//                return .Commit
//            }
//            
//            let request = Person
//                .include(Person.ruledCountry)
//                .include(Person.birthCountry)
//            
//            XCTAssertEqual(
//                sql(dbQueue, request),
//                "SELECT \"persons\".*, \"ruledCountry\".*, \"birthCountry\".* " +
//                    "FROM \"persons\" " +
//                    "LEFT JOIN \"countries\" \"ruledCountry\" ON (\"ruledCountry\".\"leaderID\" = \"persons\".\"id\") " +
//                "LEFT JOIN \"countries\" \"birthCountry\" ON (\"birthCountry\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\")")
//            
//            dbQueue.inDatabase { db in
//                // TODO: sort persons using SQL
//                let persons = request.fetchAll(db).sort { $0.id < $1.id }
//                
//                XCTAssertEqual(persons.count, 3)
//                
//                XCTAssertEqual(persons[0].name, "Arthur")
//                XCTAssertNil(persons[0].ruledCountry)
//                XCTAssertEqual(persons[0].birthCountry!.name, "France")
//                
//                XCTAssertEqual(persons[1].name, "Barbara")
//                XCTAssertEqual(persons[1].ruledCountry!.name, "France")
//                XCTAssertEqual(persons[1].birthCountry!.name, "France")
//                
//                XCTAssertEqual(persons[2].name, "John")
//                XCTAssertEqual(persons[2].ruledCountry!.name, "United States")
//                XCTAssertEqual(persons[2].birthCountry!.name, "United States")
//            }
//        }
//    }
    
//    func testPersonToRuledCountryAndToBirthCountryToLeaderToRuledCountry() {
//        assertNoError {
//            let dbQueue = try makeDatabaseQueue()
//            try dbQueue.inDatabase { db in
//                try db.execute("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birthCountryIsoCode TEXT NOT NULL REFERENCES countries(isoCode))")
//                try db.execute("CREATE TABLE countries (isoCode TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, leaderID INTEGER REFERENCES persons(id))")
//            }
//            
//            try dbQueue.inTransaction { db in
//                try db.execute("PRAGMA defer_foreign_keys = ON")
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [1, "Arthur", "FR"])
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [2, "Barbara", "FR"])
//                try db.execute("INSERT INTO persons (id, name, birthCountryIsoCode) VALUES (?, ?, ?)", arguments: [3, "John", "US"])
//                try db.execute("INSERT INTO countries (isoCode, name, leaderID) VALUES (?, ?, ?)", arguments: ["FR", "France", 2])
//                try db.execute("INSERT INTO countries (isoCode, name, leaderID) VALUES (?, ?, ?)", arguments: ["US", "United States", 3])
//                return .Commit
//            }
//            
//            let request = Person
//                .include(Person.ruledCountry
//                    .include(Country.leader))
//                .include(Person.birthCountry
//                    .include(Country.leader
//                        .include(Person.ruledCountry)))
//            
//            XCTAssertEqual(
//                sql(dbQueue, request),
//                "SELECT \"persons\".*, \"ruledCountry0\".*, \"leader0\".*, \"birthCountry\".*, \"leader1\".*, \"ruledCountry1\".* " +
//                    "FROM \"persons\" " +
//                    "LEFT JOIN \"countries\" \"ruledCountry0\" ON (\"ruledCountry0\".\"leaderID\" = \"persons\".\"id\") " +
//                    "LEFT JOIN \"persons\" \"leader0\" ON (\"leader0\".\"id\" = \"ruledCountry0\".\"leaderID\") " +
//                    "LEFT JOIN \"countries\" \"birthCountry\" ON (\"birthCountry\".\"isoCode\" = \"persons\".\"birthCountryIsoCode\") " +
//                    "LEFT JOIN \"persons\" \"leader1\" ON (\"leader1\".\"id\" = \"birthCountry\".\"leaderID\") " +
//                "LEFT JOIN \"countries\" \"ruledCountry1\" ON (\"ruledCountry1\".\"leaderID\" = \"leader1\".\"id\")")
//            
//            dbQueue.inDatabase { db in
//                // TODO: sort persons using SQL
//                let persons = request.fetchAll(db).sort { $0.id < $1.id }
//                
//                XCTAssertEqual(persons.count, 3)
//                
//                XCTAssertEqual(persons[0].name, "Arthur")
//                XCTAssertNil(persons[0].ruledCountry)
//                XCTAssertEqual(persons[0].birthCountry!.name, "France")
//                XCTAssertEqual(persons[0].birthCountry!.leader!.name, "Barbara")
//                XCTAssertEqual(persons[0].birthCountry!.leader!.ruledCountry!.name, "France")
//                
//                XCTAssertEqual(persons[1].name, "Barbara")
//                XCTAssertEqual(persons[1].ruledCountry!.name, "France")
//                XCTAssertEqual(persons[1].ruledCountry!.leader!.name, "Barbara")
//                XCTAssertEqual(persons[1].birthCountry!.name, "France")
//                XCTAssertEqual(persons[1].birthCountry!.leader!.name, "Barbara")
//                XCTAssertEqual(persons[1].birthCountry!.leader!.ruledCountry!.name, "France")
//                
//                XCTAssertEqual(persons[2].name, "John")
//                XCTAssertEqual(persons[2].ruledCountry!.name, "United States")
//                XCTAssertEqual(persons[2].ruledCountry!.leader!.name, "John")
//                XCTAssertEqual(persons[2].birthCountry!.name, "United States")
//                XCTAssertEqual(persons[2].birthCountry!.leader!.name, "John")
//                XCTAssertEqual(persons[2].birthCountry!.leader!.ruledCountry!.name, "United States")
//            }
//        }
//    }
}
