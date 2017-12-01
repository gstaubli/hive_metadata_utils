import collection._

def getDbs() : Array[String] = {
    spark.sql("show databases").collect.map(r => r.getString(0))
}

def getTablesInDB(db : String) : Array[String] = {
    spark.sql("show tables in " + db).collect.map(r => r.getString(0))
}

def getColumnsInTable(fq_table : String) : Array[String] = {
    var columns : Array[String] = new Array[String](0)
    try {
        val columns = spark.sql("show columns in " + fq_table).collect.map(r => r.getString(0))
    }
    catch {
        case e : Throwable => println("Error (" + e + "=> " + fq_table)
    }
    finally {
        return columns
    }
    columns
}

def getDbTables() : mutable.Map[String, Array[String]] = {
    val dbTables = mutable.Map[String, Array[String]]()
    val dbs = getDbs()
    dbs.foreach(db => {
        val tables = getTablesInDB(db)
        dbTables.put(db, tables)
    })
    dbTables
}

def findTablesByName(regexStr : String) : Array[String] = {
    val dbTables = getDbTables()
    val matchedDbTables = mutable.Map[String, Array[String]]()
    dbTables.foreach {
        case (db, tables) => {
            val matchedTables = mutable.ArrayBuffer[String]()
            tables.foreach(
                table => {
                    if(table.matches(regexStr))
                    {
                        matchedTables.append(table)
                    }
                }
            )
            matchedDbTables.put(db, matchedTables.toArray)
        }
    }
    matchedDbTables.flatMap{
        case (db, tables) => {
            tables.map(t => db + "." + t)
        }
    }.toArray
}

def findTablesByColumnName(regexStr : String) : Array[String] = {
    val allTables = findTablesByName(".*")
    val matchingTables = mutable.ArrayBuffer[String]()
    allTables.par.foreach(fq_table => {
        val table_cols = getColumnsInTable(fq_table)
        var has_matching_col = false
        table_cols.foreach(tc => {
            if(tc.matches(regexStr)) {
                has_matching_col = true
            }
        })
        matchingTables.append(fq_table)
    })
    matchingTables.toArray
}
