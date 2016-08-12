#if !USING_BUILTIN_SQLITE
    #if os(OSX)
        import SQLiteMacOSX
    #elseif os(iOS)
        #if (arch(i386) || arch(x86_64))
            import SQLiteiPhoneSimulator
        #else
            import SQLiteiPhoneOS
        #endif
    #endif
#endif


// MARK: - _SQLSelectQuery

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public struct _SQLSelectQuery {
    var selection: [_SQLSelectable]
    var distinct: Bool
    var source: SQLSource?
    var whereExpression: _SQLExpression?
    var groupByExpressions: [_SQLExpression]
    var orderings: [_SQLOrdering]
    var reversed: Bool
    var havingExpression: _SQLExpression?
    var limit: _SQLLimit?
    
    init(
        select selection: [_SQLSelectable],
        distinct: Bool = false,
        from source: SQLSource? = nil,
        filter whereExpression: _SQLExpression? = nil,
        groupBy groupByExpressions: [_SQLExpression] = [],
        orderBy orderings: [_SQLOrdering] = [],
        reversed: Bool = false,
        having havingExpression: _SQLExpression? = nil,
        limit: _SQLLimit? = nil)
    {
        self.selection = selection
        self.distinct = distinct
        self.source = source
        self.whereExpression = whereExpression
        self.groupByExpressions = groupByExpressions
        self.orderings = orderings
        self.reversed = reversed
        self.havingExpression = havingExpression
        self.limit = limit
    }
    
    func sql(inout arguments: StatementArguments?) -> String {
        var sql = "SELECT"
        
        if distinct {
            sql += " DISTINCT"
        }
        
        assert(!selection.isEmpty)
        sql += " " + selection.map { $0.selectionSQL(from: source, &arguments) }.joinWithSeparator(", ")
        
        if let source = source {
            sql += " FROM " + source.sourceSQL(&arguments)
        }
        
        if let whereExpression = whereExpression {
            sql += " WHERE " + whereExpression.sql(&arguments)
        }
        
        if !groupByExpressions.isEmpty {
            sql += " GROUP BY " + groupByExpressions.map { $0.sql(&arguments) }.joinWithSeparator(", ")
        }
        
        if let havingExpression = havingExpression {
            sql += " HAVING " + havingExpression.sql(&arguments)
        }
        
        var orderings = self.orderings
        if reversed {
            if orderings.isEmpty {
                // https://www.sqlite.org/lang_createtable.html#rowid
                //
                // > The rowid value can be accessed using one of the special
                // > case-independent names "rowid", "oid", or "_rowid_" in
                // > place of a column name. If a table contains a user defined
                // > column named "rowid", "oid" or "_rowid_", then that name
                // > always refers the explicitly declared column and cannot be
                // > used to retrieve the integer rowid value.
                //
                // Here we assume that _rowid_ is not a custom column.
                // TODO: support for user-defined _rowid_ column.
                // TODO: support for WITHOUT ROWID tables.
                orderings = [SQLColumn("_rowid_").desc]
            } else {
                orderings = orderings.map { $0.reversedSortDescriptor }
            }
        }
        if !orderings.isEmpty {
            sql += " ORDER BY " + orderings.map { $0.orderingSQL(&arguments) }.joinWithSeparator(", ")
        }
        
        if let limit = limit {
            sql += " LIMIT " + limit.sql
        }
        
        return sql
    }
    
    /// Returns a query that counts the number of rows matched by self.
    var countQuery: _SQLSelectQuery {
        guard groupByExpressions.isEmpty && limit == nil else {
            // SELECT ... GROUP BY ...
            // SELECT ... LIMIT ...
            return trivialCountQuery
        }
        
        guard let source = source as? SQLSourceTable else {
            // SELECT ... FROM (something which is not a table)
            return trivialCountQuery
        }
        
        assert(!selection.isEmpty)
        if selection.count == 1 {
            let selectable = self.selection[0]
            switch selectable.sqlSelectableKind {
            case .Star(source: let starSource):
                guard !distinct else {
                    return trivialCountQuery
                }
                
                if starSource !== source {
                    return trivialCountQuery
                }
                
                // SELECT tableName.* FROM tableName ...
                // ->
                // SELECT COUNT(*) FROM tableName ...
                var countQuery = unorderedQuery
                countQuery.selection = [_SQLExpression.CountAll]
                return countQuery
                
            case .Expression(let expression):
                // SELECT [DISTINCT] expr FROM tableName ...
                if distinct {
                    // SELECT DISTINCT expr FROM tableName ...
                    // ->
                    // SELECT COUNT(DISTINCT expr) FROM tableName ...
                    var countQuery = unorderedQuery
                    countQuery.distinct = false
                    countQuery.selection = [_SQLExpression.CountDistinct(expression)]
                    return countQuery
                } else {
                    // SELECT expr FROM tableName ...
                    // ->
                    // SELECT COUNT(*) FROM tableName ...
                    var countQuery = unorderedQuery
                    countQuery.selection = [_SQLExpression.CountAll]
                    return countQuery
                }
            }
        } else {
            // SELECT [DISTINCT] expr1, expr2, ... FROM tableName ...
            
            guard !distinct else {
                return trivialCountQuery
            }

            // SELECT expr1, expr2, ... FROM tableName ...
            // ->
            // SELECT COUNT(*) FROM tableName ...
            var countQuery = unorderedQuery
            countQuery.selection = [_SQLExpression.CountAll]
            return countQuery
        }
    }
    
    // SELECT COUNT(*) FROM (self)
    private var trivialCountQuery: _SQLSelectQuery {
        let source = SQLSourceQuery(query: unorderedQuery, alias: nil)
        return _SQLSelectQuery(
            select: [_SQLExpression.CountAll],
            from: source)
    }
    
    /// Remove ordering
    private var unorderedQuery: _SQLSelectQuery {
        var query = self
        query.reversed = false
        query.orderings = []
        return query
    }
}


// MARK: - SQLSource

/// TODO
public protocol SQLSource : class {
    func sourceSQL(inout arguments: StatementArguments?) -> String
}

/// TODO
public protocol SQLSourceWithIdentifier : SQLSource {
    var alias: String? { get set }
    var identifier: String { get }
    func addJoinItem(item: JoinItem) -> SQLSourceWithIdentifier
}

extension SQLSourceWithIdentifier {
    /// TODO: documentation
    public subscript(columnName: String) -> SQLColumn {
        return SQLColumn(columnName, source: self)
    }
    
    /// TODO: documentation
    public subscript(column: SQLColumn) -> SQLColumn {
        return self[column.name]
    }
}

class SQLSourceTable {
    let name: String
    var alias: String?
    
    init(name: String, alias: String?) {
        self.name = name
        self.alias = alias
    }
}

extension SQLSourceTable : SQLSource {
    func sourceSQL(inout arguments: StatementArguments?) -> String {
        if let alias = alias {
            return name.quotedDatabaseIdentifier + " AS " + alias.quotedDatabaseIdentifier
        } else {
            return name.quotedDatabaseIdentifier
        }
    }
}

extension SQLSourceTable : SQLSourceWithIdentifier {
    var identifier: String { return alias ?? name }
    
    func addJoinItem(item: JoinItem) -> SQLSourceWithIdentifier {
        return SQLSourceJoin(leftSource: self, joinItems: [item])
    }
}

class SQLSourceQuery {
    let query: _SQLSelectQuery
    var alias: String?
    
    init(query: _SQLSelectQuery, alias: String?) {
        self.query = query
        self.alias = alias
    }
}

extension SQLSourceQuery : SQLSource {
    func sourceSQL(inout arguments: StatementArguments?) -> String {
        if let alias = alias {
            return "(" + query.sql(&arguments) + ") AS " + alias.quotedDatabaseIdentifier
        } else {
            return "(" + query.sql(&arguments) + ")"
        }
    }
}

extension SQLSourceQuery : SQLSourceWithIdentifier {
    var identifier: String { return alias! }
    
    func addJoinItem(item: JoinItem) -> SQLSourceWithIdentifier {
        return SQLSourceJoin(leftSource: self, joinItems: [item])
    }
}


// MARK: - Joins

/// TODO: documentation
public struct JoinItem {
    let scopeName: String
    var required: Bool
    var selection: (SQLSourceWithIdentifier) -> [_SQLSelectable]
    let table: String
    var alias: String?
    var condition: (left: SQLSourceWithIdentifier, right: SQLSourceWithIdentifier) -> _SQLExpressible
    var rightItems: [JoinItem]
}


/// TODO: documentation
public protocol JoinItemConvertible {
    var joinItem: JoinItem { get }
}

extension JoinItem : JoinItemConvertible {
    public var joinItem: JoinItem { return self }
}

extension JoinItemConvertible {
    
    /// TODO: documentation
    @warn_unused_result
    public func include(items: JoinItemConvertible...) -> JoinItem {
        return include(required: false, items)
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func include(required required: Bool, _ items: JoinItemConvertible...) -> JoinItem {
        return include(required: required, items)
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func include(items: [JoinItemConvertible]) -> JoinItem {
        return include(required: false, items)
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func include(required required: Bool, _ items: [JoinItemConvertible]) -> JoinItem {
        var item = joinItem
        item.rightItems.appendContentsOf(items.map {
            var item = $0.joinItem
            item.required = required
            return item
            })
        return item
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func join(items: JoinItemConvertible...) -> JoinItem {
        return join(required: false, items)
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func join(required required: Bool, _ items: JoinItemConvertible...) -> JoinItem {
        return join(required: required, items)
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func join(items: [JoinItemConvertible]) -> JoinItem {
        return join(required: false, items)
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func join(required required: Bool, _ items: [JoinItemConvertible]) -> JoinItem {
        var item = joinItem
        item.rightItems.appendContentsOf(items.map {
            var item = $0.joinItem
            item.required = required
            item.selection = { _ in [] }
            return item
            })
        return item
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func aliased(alias: String) -> JoinItem {
        var item = joinItem
        item.alias = alias
        return item
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func filter(sql sql: String, arguments: StatementArguments? = nil) -> JoinItem {
        return joinItem
        //        return filter { _ in _SQLExpression.Literal("(\(sql))", arguments) }
    }
}


/// TODO
public struct ForeignRelation {
    /// TODO
    public let scopeName: String
    var alias: String?
    let table: String
    let foreignKey: [String: String]
    
    /// TODO
    public init(named scopeName: String? = nil, to table: String, through foreignKey: [String: String]) {
        self.scopeName = scopeName ?? table
        self.alias = nil
        self.table = table
        self.foreignKey = foreignKey
    }
}

extension ForeignRelation {
    /// TODO
    public func aliased(alias: String) -> ForeignRelation {
        var relation = self
        relation.alias = alias
        return relation
    }
}

extension ForeignRelation {
    /// TODO
    public func select(selection: (SQLSource) -> [_SQLSelectable]) -> ForeignRelation {
        // TODO
        return self
    }
}

extension ForeignRelation : JoinItemConvertible {
    public var joinItem: JoinItem {
        return JoinItem(
            scopeName: scopeName,
            required: false,
            selection: { [_SQLSelectionElement.Star(source: $0)] },
            table: table,
            alias: alias ?? scopeName,
            condition: { (left, right) in self.foreignKey.map { (leftColumn, rightColumn) in right[rightColumn] == left[leftColumn] }.reduce(&&) },
            rightItems: [])
    }
}

class SQLSourceJoin {
    let leftSource: SQLSourceWithIdentifier
    let joinItems: [JoinItem]
    
    init(leftSource: SQLSourceWithIdentifier, joinItems: [JoinItem]) {
        self.leftSource = leftSource
        self.joinItems = joinItems
    }
}

extension SQLSourceJoin : SQLSource {
    func sourceSQL(inout arguments: StatementArguments?) -> String {
        return "TODO"
    }
}

extension SQLSourceJoin : SQLSourceWithIdentifier {
    var alias: String? {
        get {
            return leftSource.alias
        }
        set {
            leftSource.alias = newValue
        }
    }
    
    var identifier: String {
        return leftSource.identifier
    }
    
    func addJoinItem(item: JoinItem) -> SQLSourceWithIdentifier {
        return SQLSourceJoin(leftSource: leftSource, joinItems: joinItems + [item])
    }
}


// MARK: - _SQLOrdering

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SQLOrdering {
    var reversedSortDescriptor: _SQLSortDescriptor { get }
    func orderingSQL(inout arguments: StatementArguments?) -> String
}

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public enum _SQLSortDescriptor {
    case Asc(_SQLExpression)
    case Desc(_SQLExpression)
}

extension _SQLSortDescriptor : _SQLOrdering {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var reversedSortDescriptor: _SQLSortDescriptor {
        switch self {
        case .Asc(let expression):
            return .Desc(expression)
        case .Desc(let expression):
            return .Asc(expression)
        }
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func orderingSQL(inout arguments: StatementArguments?) -> String {
        switch self {
        case .Asc(let expression):
            return expression.sql(&arguments) + " ASC"
        case .Desc(let expression):
            return expression.sql(&arguments) + " DESC"
        }
    }
}


// MARK: - _SQLLimit

struct _SQLLimit {
    let limit: Int
    let offset: Int?
    
    var sql: String {
        if let offset = offset {
            return "\(limit) OFFSET \(offset)"
        } else {
            return "\(limit)"
        }
    }
}


// MARK: - _SpecificSQLExpressible

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SpecificSQLExpressible : SQLExpressible {
    // SQLExpressible can be adopted by Swift standard types, and user
    // types, through the DatabaseValueConvertible protocol which inherits
    // from SQLExpressible.
    //
    // For example, Int adopts SQLExpressible through
    // DatabaseValueConvertible.
    //
    // _SpecificSQLExpressible, on the other side, is not adopted by any
    // Swift standard type or any user type. It is only adopted by GRDB types,
    // such as SQLColumn and _SQLExpression.
    //
    // This separation lets us define functions and operators that do not
    // spill out. The three declarations below have no chance overloading a
    // Swift-defined operator, or a user-defined operator:
    //
    // - ==(SQLExpressible, _SpecificSQLExpressible)
    // - ==(_SpecificSQLExpressible, SQLExpressible)
    // - ==(_SpecificSQLExpressible, _SpecificSQLExpressible)
}

extension _SpecificSQLExpressible where Self: _SQLOrdering {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var reversedSortDescriptor: _SQLSortDescriptor {
        return .Desc(sqlExpression)
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func orderingSQL(inout arguments: StatementArguments?) -> String {
        return sqlExpression.sql(&arguments)
    }
}

extension _SpecificSQLExpressible where Self: _SQLSelectable {
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func selectionSQL(from querySource: SQLSource?, inout _ arguments: StatementArguments?) -> String {
        return sqlExpression.sql(&arguments)
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func countedSQL(inout arguments: StatementArguments?) -> String {
        return sqlExpression.sql(&arguments)
    }
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlSelectableKind: _SQLSelectableKind {
        return .Expression(expression: sqlExpression)
    }
}

extension _SpecificSQLExpressible {
    
    /// Returns a value that can be used as an argument to FetchRequest.order()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var asc: _SQLSortDescriptor {
        return .Asc(sqlExpression)
    }
    
    /// Returns a value that can be used as an argument to FetchRequest.order()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var desc: _SQLSortDescriptor {
        return .Desc(sqlExpression)
    }
    
    /// Returns a value that can be used as an argument to FetchRequest.select()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func aliased(alias: String) -> _SQLSelectable {
        return _SQLSelectionElement.Expression(expression: sqlExpression, alias: alias)
    }
}


/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public indirect enum _SQLExpression {
    /// For example: `name || 'rrr' AS pirateName`
    case Literal(String, StatementArguments?)
    
    /// For example: `1` or `'foo'`
    case Value(DatabaseValue)
    
    /// For example: `name`, `table.name`
    case Identifier(identifier: String, source: SQLSourceWithIdentifier?)
    
    /// For example: `name = 'foo' COLLATE NOCASE`
    case Collate(_SQLExpression, String)
    
    /// For example: `NOT condition`
    case Not(_SQLExpression)
    
    /// For example: `name = 'foo'`
    case Equal(_SQLExpression, _SQLExpression)
    
    /// For example: `name <> 'foo'`
    case NotEqual(_SQLExpression, _SQLExpression)
    
    /// For example: `name IS NULL`
    case Is(_SQLExpression, _SQLExpression)
    
    /// For example: `name IS NOT NULL`
    case IsNot(_SQLExpression, _SQLExpression)
    
    /// For example: `-value`
    case PrefixOperator(String, _SQLExpression)
    
    /// For example: `age + 1`
    case InfixOperator(String, _SQLExpression, _SQLExpression)
    
    /// For example: `id IN (1,2,3)`
    case In([_SQLExpression], _SQLExpression)
    
    /// For example `id IN (SELECT ...)`
    case InSubQuery(_SQLSelectQuery, _SQLExpression)
    
    /// For example `EXISTS (SELECT ...)`
    case Exists(_SQLSelectQuery)
    
    /// For example: `age BETWEEN 1 AND 2`
    case Between(value: _SQLExpression, min: _SQLExpression, max: _SQLExpression)
    
    /// For example: `LOWER(name)`
    case Function(String, [_SQLExpression])
    
    /// For example: `COUNT(name)`
    case Count(_SQLExpression)
    
    /// For example: `COUNT(DISTINCT name)`
    case CountDistinct(_SQLExpression)
    
    /// For example: `COUNT(*)`
    case CountAll
    
    ///
    func sql(inout arguments: StatementArguments?) -> String {
        switch self {
        case .Literal(let sql, let literalArguments):
            if let literalArguments = literalArguments {
                guard arguments != nil else {
                    fatalError("Not implemented")
                }
                arguments!.values.appendContentsOf(literalArguments.values)
                for (name, value) in literalArguments.namedValues {
                    guard arguments!.namedValues[name] == nil else {
                        fatalError("argument \(String(reflecting: name)) can't be reused")
                    }
                    arguments!.namedValues[name] = value
                }
            }
            return sql
            
        case .Value(let value):
            if arguments == nil {
                return value.sqlLiteral
            } else {
                arguments!.values.append(value)
                return "?"
            }
            
        case .Identifier(let identifier, let source):
            if let source = source {
                return source.identifier.quotedDatabaseIdentifier + "." + identifier.quotedDatabaseIdentifier
            } else {
                return identifier.quotedDatabaseIdentifier
            }
            
        case .Collate(let expression, let collation):
            let sql = expression.sql(&arguments)
            let chars = sql.characters
            if chars.last! == ")" {
                return String(chars.prefixUpTo(chars.endIndex.predecessor())) + " COLLATE " + collation + ")"
            } else {
                return sql + " COLLATE " + collation
            }
            
        case .Not(let condition):
            switch condition {
            case .Not(let expression):
                return expression.sql(&arguments)
                
            case .In(let expressions, let expression):
                if expressions.isEmpty {
                    return "1"
                } else {
                    return "(" + expression.sql(&arguments) + " NOT IN (" + (expressions.map { $0.sql(&arguments) } as [String]).joinWithSeparator(", ") + "))"
                }
                
            case .InSubQuery(let subQuery, let expression):
                return "(" + expression.sql(&arguments) + " NOT IN (" + subQuery.sql(&arguments)  + "))"
                
            case .Exists(let subQuery):
                return "(NOT EXISTS (" + subQuery.sql(&arguments)  + "))"
                
            case .Equal(let lhs, let rhs):
                return _SQLExpression.NotEqual(lhs, rhs).sql(&arguments)
                
            case .NotEqual(let lhs, let rhs):
                return _SQLExpression.Equal(lhs, rhs).sql(&arguments)
                
            case .Is(let lhs, let rhs):
                return _SQLExpression.IsNot(lhs, rhs).sql(&arguments)
                
            case .IsNot(let lhs, let rhs):
                return _SQLExpression.Is(lhs, rhs).sql(&arguments)
                
            default:
                return "(NOT " + condition.sql(&arguments) + ")"
            }
            
        case .Equal(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .Value(DatabaseValue.Null)):
                // Swiftism!
                // Turn `filter(a == nil)` into `a IS NULL` since the intention is obviously to check for NULL. `a = NULL` would evaluate to NULL.
                return "(" + lhs.sql(&arguments) + " IS NULL)"
            case (.Value(DatabaseValue.Null), let rhs):
                // Swiftism!
                return "(" + rhs.sql(&arguments) + " IS NULL)"
            default:
                return "(" + lhs.sql(&arguments) + " = " + rhs.sql(&arguments) + ")"
            }
            
        case .NotEqual(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .Value(DatabaseValue.Null)):
                // Swiftism!
                // Turn `filter(a != nil)` into `a IS NOT NULL` since the intention is obviously to check for NULL. `a <> NULL` would evaluate to NULL.
                return "(" + lhs.sql(&arguments) + " IS NOT NULL)"
            case (.Value(DatabaseValue.Null), let rhs):
                // Swiftism!
                return "(" + rhs.sql(&arguments) + " IS NOT NULL)"
            default:
                return "(" + lhs.sql(&arguments) + " <> " + rhs.sql(&arguments) + ")"
            }
            
        case .Is(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .Value(let rhs)) where rhs.isNull:
                return "(" + lhs.sql(&arguments) + " IS NULL)"
            case (.Value(let lhs), let rhs) where lhs.isNull:
                return "(" + rhs.sql(&arguments) + " IS NULL)"
            default:
                return "(" + lhs.sql(&arguments) + " IS " + rhs.sql(&arguments) + ")"
            }
            
        case .IsNot(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .Value(let rhs)) where rhs.isNull:
                return "(" + lhs.sql(&arguments) + " IS NOT NULL)"
            case (.Value(let lhs), let rhs) where lhs.isNull:
                return "(" + rhs.sql(&arguments) + " IS NOT NULL)"
            default:
                return "(" + lhs.sql(&arguments) + " IS NOT " + rhs.sql(&arguments) + ")"
            }
            
        case .PrefixOperator(let SQLOperator, let value):
            return SQLOperator + value.sql(&arguments)
            
        case .InfixOperator(let SQLOperator, let lhs, let rhs):
            return "(" + lhs.sql(&arguments) + " \(SQLOperator) " + rhs.sql(&arguments) + ")"
            
        case .In(let expressions, let expression):
            guard !expressions.isEmpty else {
                return "0"
            }
            return "(" + expression.sql(&arguments) + " IN (" + (expressions.map { $0.sql(&arguments) } as [String]).joinWithSeparator(", ")  + "))"
        
        case .InSubQuery(let subQuery, let expression):
            return "(" + expression.sql(&arguments) + " IN (" + subQuery.sql(&arguments)  + "))"
            
        case .Exists(let subQuery):
            return "(EXISTS (" + subQuery.sql(&arguments)  + "))"
            
        case .Between(value: let value, min: let min, max: let max):
            return "(" + value.sql(&arguments) + " BETWEEN " + min.sql(&arguments) + " AND " + max.sql(&arguments) + ")"
            
        case .Function(let functionName, let functionArguments):
            return functionName + "(" + (functionArguments.map { $0.sql(&arguments) } as [String]).joinWithSeparator(", ")  + ")"
            
        case .Count(let counted):
            return "COUNT(" + counted.countedSQL(&arguments) + ")"
            
        case .CountDistinct(let expression):
            return "COUNT(DISTINCT " + expression.sql(&arguments) + ")"
            
        case .CountAll:
            return "COUNT(*)"
        }
    }
}

extension _SQLExpression : _SpecificSQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return self
    }
}

extension _SQLExpression : _SQLSelectable {}
extension _SQLExpression : _SQLOrdering {}


// MARK: - _SQLSelectable

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SQLSelectable {
    func selectionSQL(from querySource: SQLSource?, inout _ arguments: StatementArguments?) -> String
    func countedSQL(inout arguments: StatementArguments?) -> String
    var sqlSelectableKind: _SQLSelectableKind { get }
}

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public enum _SQLSelectableKind {
    case Star(source: SQLSourceWithIdentifier)
    case Expression(expression: _SQLExpression)
}

enum _SQLSelectionElement {
    case Star(source: SQLSourceWithIdentifier)
    case Expression(expression: _SQLExpression, alias: String)
}

extension _SQLSelectionElement : _SQLSelectable {
    
    func selectionSQL(from querySource: SQLSource?, inout _ arguments: StatementArguments?) -> String {
        switch self {
        case .Star(let starSource):
            if starSource === querySource {
                return "*"
            } else {
                return starSource.identifier.quotedDatabaseIdentifier + ".*"
            }
        case .Expression(expression: let expression, alias: let alias):
            return expression.sql(&arguments) + " AS " + alias.quotedDatabaseIdentifier
        }
    }
    
    func countedSQL(inout arguments: StatementArguments?) -> String {
        switch self {
        case .Star:
            fatalError("Not implemented")
        case .Expression(expression: let expression, alias: _):
            return expression.sql(&arguments)
        }
    }
    
    var sqlSelectableKind: _SQLSelectableKind {
        switch self {
        case .Star(let source):
            return .Star(source: source)
        case .Expression(expression: let expression, alias: _):
            return .Expression(expression: expression)
        }
    }
}


// MARK: - SQLColumn

/// A column in the database
///
/// See https://github.com/groue/GRDB.swift#the-query-interface
public struct SQLColumn {
    let source: SQLSourceWithIdentifier?
    
    /// The name of the column
    public let name: String
    
    /// Initializes a column given its name.
    public init(_ name: String) {
        self.name = name
        self.source = nil
    }
    
    init(_ name: String, source: SQLSourceWithIdentifier?) {
        self.name = name
        self.source = source
    }
}

extension SQLColumn : _SpecificSQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return .Identifier(identifier: name, source: source)
    }
}

extension SQLColumn : _SQLSelectable {}
extension SQLColumn : _SQLOrdering {}
