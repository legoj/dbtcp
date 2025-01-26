using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace jstools
{
    class dbtcp
    {
        public static string _APPNAME = "dbtcp";
        static void Main(string[] args)
        {
            string cxml;
            if (args.Length < 1)
            {
                cxml = AppDomain.CurrentDomain.BaseDirectory + dbtcp._APPNAME + ".xml";
                Console.WriteLine("XMLConfigFile was not specified. Setting default path:");
                Console.WriteLine(cxml);
            }
            else
            {
                cxml = args[0];
            }

            if (!File.Exists(cxml)){
                Console.WriteLine("The specified an XML configuration file path does not exist!");
                Console.WriteLine("Param: " + cxml);
                return;
            }
            
            DBTCopy dbc = new DBTCopy(cxml);
            dbc.RunTasks();
        }
        static void PrintUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("\t" + _APPNAME + " [pathToXMLConfigurationFile]");            
        }
    }
    internal class DBTCopy
    {
        Config conf = null;
        public DBTCopy(string xmlPath)
        {
            JUtil.LogInfo(this.GetType(), "instantiated with param: " + xmlPath);
            this.conf = new Config(xmlPath);            
        }
        public void RunTasks()
        {
            JUtil.LogInfo(this.GetType(), "RunTasks invoked!");
            foreach (string tid in this.conf.TaskIds)
            {
                TaskInfo tsk = this.conf.GetTaskInfo(tid);
                RunTask(tsk);
            }
        }
        private void RunTask(TaskInfo tskInfo)
        {
            JUtil.LogInfo(this.GetType(), "RunTask.Id: " + tskInfo.Id + " Desc: " + tskInfo.Description);
            string sdStr = this.conf.GetDBString(tskInfo.SourceDB);
            DBExport sdb = new DBExport(sdStr);
            string ddStr = this.conf.GetDBString(tskInfo.DestinationDB);            
            DBImport ddb = new DBImport(ddStr);
            JUtil.LogInfo(this.GetType(), "RunTask.SrcBD: " + tskInfo.SourceDB + " DstDB: " + tskInfo.DestinationDB);
            foreach (string tn in tskInfo.TableIds)
            {
                JUtil.LogInfo(this.GetType(), "RunTask.Table: " + tn);
                TableInfo tbl = this.conf.GetTableInfo(tn);                
                SqlCommand sqc = new SqlCommand(tbl.SourceSelect, sdb.GetConnection());
                DataTable dt = sdb.GetTableData(sqc);
                dt.TableName = tbl.NewName;

                if (ddb.DBTableExists(tbl.NewName)) {
                    JUtil.LogInfo(this.GetType(), "RunTask.TableExists: " + tbl.NewName);
                    if (tskInfo.RequiresBackup(tn))
                    {
                        JUtil.LogInfo(this.GetType(), "RunTask.Backup: Renaming table to " + "TDBDBDB" );
                        //tbd rename existing table and create new
                        ddb.DBTableTruncate(tbl.NewName);
                    }
                    else
                    {
                        JUtil.LogInfo(this.GetType(), "RunTask.Truncate: Clearing existing data");
                        ddb.DBTableTruncate(tbl.NewName);
                    }
                }
                else
                {
                    JUtil.LogInfo(this.GetType(), "RunTask.CreateTable: " + tbl.NewName + " on DestDB");
                    DataTable sc = sdb.GetTableSchema(sqc);
                    string s = sdb.GenerateTableCreateSQL(tbl.NewName, sc, tbl.SelectFields);
                    sc.Dispose();                    
                    sc = null;
                    JUtil.LogInfo(this.GetType(), "RunTask.SchemaTable: disposed.");
                    ddb.DBExecNonQuery(s);
                }
                ddb.DBBulkInsert(dt);
                dt.Dispose();
                dt = null;
                JUtil.LogInfo(this.GetType(), "RunTask.DataTable: disposed.");
            }
        }

        
    }
    internal class DBExport
    {
        static string _COLNAME = "ColumnName";
        static string _TYPNAME = "DataTypeName";
        static string _LENNAME = "ColumnSize";
        static string _ADBNULL = "AllowDBNull";
        static string _NUMPREC = "NumericPrecision";
        static string _NUMSCAL = "NumericScale";
        static string _DATTYP = "DataType";
        static string _VARTYPE = "var";
        static string _CHRTYPE = "char";
        static string _BINTYPE = "binary";

        private SqlConnection sqlConn = null;
        private string connString = null;

        public DBExport(string connectionString)
        {
            JUtil.LogInfo(this.GetType(), "Instantiated with param: " + connectionString);
            this.connString = connectionString;
        }
        public SqlConnection GetConnection()
        {
            if (this.sqlConn == null)
            {
                this.sqlConn = new SqlConnection(this.connString);
                this.sqlConn.Open();
                JUtil.LogInfo(this.GetType(), "GetConnection.State: " + this.sqlConn.State);
            }
            return this.sqlConn;
        }
        public void CloseConnection()
        {
            if (this.sqlConn != null) { this.sqlConn.Close(); }
            this.sqlConn = null;
        }

        public DataTable GetTableSchema(string sqlSelect)
        {
            return GetTableSchema(new SqlCommand(sqlSelect, this.GetConnection()));
        }

        public DataTable GetTableSchema(SqlCommand sqlCmd)
        {
            JUtil.LogInfo(this.GetType(), "GetTableSchema.SqlCmd: " + sqlCmd.CommandText);
            SqlDataReader reader = sqlCmd.ExecuteReader(CommandBehavior.SchemaOnly);
            DataTable sTable = reader.GetSchemaTable();
            JUtil.LogInfo(this.GetType(), "GetTableSchema.RowCount: " + sTable.Rows.Count);
            if (!reader.IsClosed) reader.Close();
            return sTable;
        }
        public DataTable GetTableData(SqlCommand sqlCmd)
        {
            JUtil.LogInfo(this.GetType(), "GetTableData.SqlCmd: " + sqlCmd.CommandText);
            DataTable tabData = new DataTable();
            SqlDataAdapter cda = new SqlDataAdapter(sqlCmd);

            DateTime startDT = DateTime.Now;
            JUtil.LogInfo(this.GetType(), "GetTableData.Started: " + startDT);
            cda.Fill(tabData);
            DateTime endDT = DateTime.Now;
            JUtil.LogInfo(this.GetType(), "GetTableData.Completed: " + endDT);
            TimeSpan diff = endDT - startDT;
            JUtil.LogInfo(this.GetType(), "GetTableData.TimeTaken: " + diff.Seconds);
            JUtil.LogInfo(this.GetType(), "GetTableData.RowCount: " + tabData.Rows.Count);
            cda.Dispose();
            return tabData;
        }
        public string GenerateTableCreateSQL(string tableName, DataTable schemaTable)
        {
            return GenerateTableCreateSQL(tableName, schemaTable, null);
        }
        public string GenerateTableCreateSQL(string tableName, DataTable schemaTable, List<string> selectedColumns)
        {
            JUtil.LogInfo(this.GetType(), "GenerateTableCreateSQL.TableName: " + tableName );
            string sql = "CREATE TABLE [" + tableName + "] (";
            foreach (DataRow colInfo in schemaTable.Rows)
            {
                string fieldName = colInfo.Field<string>(_COLNAME);
                if (selectedColumns == null || selectedColumns.Contains(fieldName))
                {
                    string fieldType = colInfo.Field<string>(_TYPNAME);
                    bool bAllowNull = colInfo.Field<bool>(_ADBNULL);
                    int fieldLength = colInfo.Field<int>(_LENNAME);
                    Type dataType = colInfo.Field<Type>(_DATTYP);
                    Int16 numPrecision = colInfo.Field<Int16>(_NUMPREC);
                    string fLength = fieldLength >= 8000 ? "max" : fieldLength.ToString();
                    string col = "[" + fieldName + "] [" + fieldType + "]";
                    if (fieldType.ToLower().Contains(_VARTYPE)) col = col + "(" + fLength + ") ";
                    if (!bAllowNull) col = col + " NOT";
                    col = col + " NULL, ";
                    sql = sql + col;
                }
            }
            sql = sql.Remove(sql.Length - 2);
            sql = sql + " )";
            JUtil.LogInfo(this.GetType(), "GenerateTableCreateSQL.RetString: " + sql);
            return sql;
        }
    }
    internal class DBImport
    {
        private SqlConnection sqlConn = null;
        private string connString = null;

        public DBImport(string connectionString)
        {
            this.connString = connectionString;
            JUtil.LogInfo(this.GetType(), "Instantiated with param: " + connectionString);
        }
        public SqlConnection GetConnection()
        {
            if (this.sqlConn == null)
            {
                this.sqlConn = new SqlConnection(this.connString);
                this.sqlConn.Open();
                JUtil.LogInfo(this.GetType(), "GetConnection.State: " + this.sqlConn.State);
            }
            return this.sqlConn;
        }
        public void CloseConnection()
        {
            if (this.sqlConn != null) { this.sqlConn.Close(); }
            this.sqlConn = null;
        }
        public bool DBTableExists(string tableName)
        {
            string sqlStr = @"IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='" + tableName + "') SELECT 1 ELSE SELECT 0";
            SqlCommand sqlCmd = new SqlCommand(sqlStr, this.GetConnection());
            int x = Convert.ToInt32(sqlCmd.ExecuteScalar());
            JUtil.LogInfo(this.GetType(), "DBTableExists.TabExist: " + x);
            return x == 1;
        }
        public int DBExecNonQuery(string sqlString)
        {
            JUtil.LogInfo(this.GetType(), "DBExecNonQuery.SQL: " + sqlString);
            SqlCommand sqlCmd = new SqlCommand(sqlString, this.GetConnection());
            return sqlCmd.ExecuteNonQuery();
        }
        public int DBTableCreate(string tableName, string colAttr)
        {            
            string sqlStr = "CREATE TABLE " + tableName + " (" + colAttr + ")";
            JUtil.LogInfo(this.GetType(), "DBTableCreate.SQL: " + sqlStr);
            SqlCommand sqlCmd = new SqlCommand(sqlStr, this.GetConnection());
            return sqlCmd.ExecuteNonQuery();
        }
        public int DBTableTruncate(string tableName)
        {
            string sqlStr = "TRUNCATE TABLE " + tableName;
            JUtil.LogInfo(this.GetType(), "DBTableTruncate.SQL: " + sqlStr);
            SqlCommand sqlCmd = new SqlCommand(sqlStr, this.GetConnection());
            return sqlCmd.ExecuteNonQuery();
        }
        public int DBTableRename(string tableName, string newName)
        {
            JUtil.LogInfo(this.GetType(), "DBTableRename.OldName: " + tableName + " NewName:"+ newName);
            return DBExecNonQuery("sp_rename '" + tableName + "', '" + newName + "'");
        }

        public bool DBBulkInsert(DataTable table)
        {
            JUtil.LogInfo(this.GetType(), "DBBulkInsert.TableName: " + table.TableName + " RowCount:" + table.Rows.Count);
            SqlBulkCopy bc = new SqlBulkCopy(this.GetConnection());
            try
            {
                DateTime startDT = DateTime.Now;
                JUtil.LogInfo(this.GetType(), "DBBulkInsert.Started: " + startDT);
                bc.BulkCopyTimeout = 500; //500 seconds
                bc.DestinationTableName = table.TableName;
                bc.WriteToServer(table);
                DateTime endDT = DateTime.Now;
                JUtil.LogInfo(this.GetType(), "DBBulkInsert.Completed: " + endDT);
                TimeSpan diff = endDT - startDT;
                JUtil.LogInfo(this.GetType(), "DBBulkInsert.TimeTaken: " + diff.Seconds + " secs");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine("SQLException: " + e);
                JUtil.LogError(this.GetType(), "DBBulkInsert.Exception: " + e.Message);
                if (e.Message.Contains("Received an invalid column length from the bcp client for colid"))
                {

                    string pattern = @"\d+";
                    Match match = Regex.Match(e.Message.ToString(), pattern);
                    var index = Convert.ToInt32(match.Value) - 1;

                    FieldInfo fi = typeof(SqlBulkCopy).GetField("_sortedColumnMappings", BindingFlags.NonPublic | BindingFlags.Instance);
                    var sortedColumns = fi.GetValue(bc);
                    var items = (Object[])sortedColumns.GetType().GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(sortedColumns);

                    FieldInfo itemdata = items[index].GetType().GetField("_metadata", BindingFlags.NonPublic | BindingFlags.Instance);
                    var metadata = itemdata.GetValue(items[index]);

                    var column = metadata.GetType().GetField("column", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);
                    var length = metadata.GetType().GetField("length", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);
                    JUtil.LogError(this.GetType(), "DBBulkInsert.ExcDetail: " + String.Format("Column: {0} contains data with a length greater than: {1}", column, length));
                }
                return false;
            }
            finally
            {
                bc.Close();
            }
            return true;
        }

    }

    //logger class - singleton
    internal class JUtil
    {
        static string _INFO = "[info]";
        static string _ERROR = "[error]";
        private static JUtil _instance = null;
        static JUtil Logger {   get  {  if (_instance == null) _instance = new JUtil(); return _instance; }   }
        //public static void Log(string fmtdString, params object[] args) { JUtil.Logger.Write(fmtdString, args); }
        public static void LogInfo( Type callerType, string remarks ) { JUtil.Logger.Write("{0}\t{1}\t{2}\t{3}",DateTime.Now, _INFO, callerType.ToString(), remarks); }
        public static void LogError(Type callerType, string remarks) { JUtil.Logger.Write("{0}\t{1}\t{2}\t{3}", DateTime.Now, _ERROR, callerType.ToString(), remarks); }

        private string logFilePath = null;
        private TextWriter logWriter = null;
        public bool PrintToConsole { get; set; }
        JUtil(string logFP =null)
        {
            this.PrintToConsole = true; //default
            this.logFilePath = GenFilePath(logFP);
            this.logWriter = new StreamWriter(this.logFilePath);
        }

        void Write(string fmtdString, params object[] args)
        {
            this.logWriter.WriteLine(fmtdString, args);
            if (PrintToConsole) Console.WriteLine(fmtdString, args);
            this.logWriter.Flush();
        }

        private static string GenFilePath(string prefix)
        {
            string p = AppDomain.CurrentDomain.BaseDirectory + dbtcp._APPNAME + "_";
            if (!String.IsNullOrEmpty(prefix)) p = p + prefix + "_";
            return p + DateTime.Now.ToString("yyyyMMdd_hhmmss") + ".log";      
        }

        static void InitLogger(string logName)
        {
            if (_instance == null) _instance = new JUtil(logName);
        }
        
    }

    //xml config parser
    internal class Config
    {
        public const string _DB = "db";
        public const string _F = "f";
        public const string _TABLE = "table";
        public const string _TASK = "task";
        public const string _SELECT = "select";
        public const string _WHERE = "where";
        public const string ATTID = "id";
        public const string ATTDESC = "desc";
        public const string ATTSRC = "src";
        public const string ATTDST = "dst";
        public const string ATTBACKUP = "backup";
        public const string ATTDISTINCT = "distinct";

        private string configFilePath = null;
        private Dictionary<string, string> dbs = null;
        private Dictionary<string, TableInfo> tabs = null;
        private Dictionary<string, TaskInfo> tsks = null;
        public Config(string xmlFilePath)
        {
            this.configFilePath = xmlFilePath;
            this.ParseXML();            
        }
        private void ParseXML()
        {
            XmlDocument oXml = new XmlDocument();
            oXml.Load(this.configFilePath);
            XmlNode mNode = oXml.DocumentElement;
            if (dbtcp._APPNAME.Equals(mNode.Name))
            {
                this.dbs = new Dictionary<string, string>();
                this.tabs = new Dictionary<string, TableInfo>();
                this.tsks = new Dictionary<string, TaskInfo>();

                foreach (XmlNode xNode in mNode.ChildNodes)
                {
                    string x = xNode.Name;
                    string name = xNode.Attributes[ATTID].Value;
                    if (_DB.Equals(x))
                    {
                        string val = xNode.InnerText;
                        this.dbs.Add(name, val);
                    }
                    else if (_TABLE.Equals(x))
                    {
                        string stab = xNode.Attributes[ATTSRC].Value;
                        string dtab = stab;
                        XmlAttribute dstAtt = xNode.Attributes[ATTDST];
                        if (dstAtt != null) dtab = dstAtt.Value;

                        TableInfo tabInfo = new TableInfo(name, stab, dtab);

                        XmlAttribute disAtt = xNode.Attributes[ATTDISTINCT];
                        if (disAtt != null) tabInfo.IsDistinct = disAtt.Value.Equals("1", StringComparison.CurrentCultureIgnoreCase);

                        if (xNode.HasChildNodes)
                        {
                            foreach (XmlNode tNode in xNode.ChildNodes)
                            {
                                string cx = tNode.Name;
                                if (_SELECT.Equals(cx))
                                {
                                    foreach (XmlNode fNode in tNode.ChildNodes)
                                    {
                                        string fn = fNode.Name;
                                        if (_F.Equals(fn))
                                        {
                                            string fldName = fNode.Attributes[ATTID].Value;
                                            tabInfo.AddSelectField(fldName);
                                        }
                                    }
                                }
                                else if (_WHERE.Equals(cx))
                                {
                                    tabInfo.Where = tNode.InnerText;
                                }
                            }
                        }
                        this.tabs.Add(tabInfo.Id, tabInfo);
                    }
                    else if (_TASK.Equals(x))
                    {
                        string ssrc = xNode.Attributes[ATTSRC].Value;
                        string sdes = xNode.Attributes[ATTDESC].Value;
                        string sdst = xNode.Attributes[ATTDST].Value;

                        TaskInfo tskInfo = new TaskInfo(name, sdes, ssrc,sdst);
                        foreach (XmlNode tNode in xNode.ChildNodes)
                        {
                            string cx = tNode.Name;
                            if (_TABLE.Equals(cx))
                            {
                                string stab = tNode.Attributes[ATTID].Value;
                                XmlAttribute buAtt = tNode.Attributes[ATTBACKUP];
                                bool bBak = buAtt != null;
                                tskInfo.AddTable(stab, bBak);
                            }
                        }
                        this.tsks.Add(tskInfo.Id, tskInfo);
                    }
                }
            }
            oXml = null;
        }
        public IEnumerable<string> TaskIds { get { return this.tsks.Keys; } }
        public TaskInfo GetTaskInfo(string taskId)
        {
            return this.tsks[taskId];
        }
        public TableInfo GetTableInfo(string tableId)
        {
            return this.tabs[tableId];
        }
        public string GetDBString(string dbid)
        {
            return this.dbs[dbid];
        }
        public void DumpTables()
        {
            foreach(TableInfo t in this.tabs.Values)
            {
                Console.WriteLine("table.Id={0}, SrcName={1}, DstName={2}", t.Id, t.Name, t.NewName);
                Console.WriteLine("select.Str: {0}", t.SourceSelect);
            }
        }
        public void DumpTasks()
        {
            foreach (TaskInfo t in this.tsks.Values)
            {
                Console.WriteLine("task id={0}, desc={1}, srcdb={2}, destdb={3}", t.Id, t.Description, t.SourceDB, t.DestinationDB);
                foreach (string i in t.TableIds)
                {
                    Console.WriteLine("table id={0}, backup={1}", i, t.RequiresBackup(i));
                }

            }
        }
    }
    internal class TableInfo
    {
        private List<string> selectFields = null;
        private string tableId = null;
        private string tableSrc = null;
        private string tableDst = null;

        public TableInfo(string id, string srcName, string dstName)
        {
            this.tableId = id;
            this.tableSrc = srcName;
            this.tableDst = dstName;
        }

        public string Id { get { return this.tableId; } }
        public string Name { get { return this.tableSrc; } }
        public string NewName { get { return this.tableDst; } }
        public string Where { get; set;}
        public bool IsDistinct { get; set; }
        public List<string> SelectFields { get { return this.selectFields; } } 
        public void AddSelectField(string fieldName)
        {
            if (this.selectFields == null)
                this.selectFields = new List<string>();
            if(!this.selectFields.Contains(fieldName))
                this.selectFields.Add(fieldName);
        }
        public string SourceSelect
        {
            get
            {
                StringBuilder sb = new StringBuilder("select ");
                if (this.IsDistinct) sb.Append(" distinct ");
                if (this.selectFields == null)
                    sb.Append("*");
                else
                    sb.Append(String.Join(",", this.selectFields.ToArray()));
                sb.Append(" from ").Append(this.tableSrc);
                if (this.Where != null)
                    sb.Append(" where ").Append(this.Where);
                return sb.ToString();
            }
        }
    }

    internal class TaskInfo
    {
        private string taskId = null;
        private string taskDesc = null;
        private string srcDB = null;
        private string dstDB = null;
        private Dictionary<string, bool> tblLst = null;
        
        public TaskInfo(string id, string desc, string src, string dst)
        {
            this.taskId = id;
            this.taskDesc = desc;
            this.srcDB = src;
            this.dstDB = dst;
            this.tblLst = new Dictionary<string, bool>();
        }
        public string SourceDB { get { return this.srcDB; } }
        public string DestinationDB { get { return this.dstDB; } }
        public string Id { get { return this.taskId; } }
        public string Description { get { return this.taskDesc; } }
        public void AddTable(string tblid, bool bBackup)
        {
            if (!this.tblLst.ContainsKey(tblid)) this.tblLst.Add(tblid, bBackup);
        }
        public IEnumerable<string> TableIds { get { return this.tblLst.Keys; } }
        public int TableCount {  get { return this.tblLst.Count; } }
        public bool RequiresBackup(string tbNam)
        {
            return this.tblLst[tbNam];
        }

    }
}
    

