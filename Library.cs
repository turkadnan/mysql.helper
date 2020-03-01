using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Linq;
using System.Reflection;

namespace mysql.helper
{
    public class Library
    {
        public static DataSet ExecuteDataset(string ConnectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            try
            {
                MySqlConnection mysqlConn = new MySqlConnection(ConnectionString);
                MySqlDataAdapter Da = null;
                DataSet Ds = new DataSet();
                Da = new MySqlDataAdapter(commandText, mysqlConn);

                if (commandType == CommandType.Text)
                {
                    Da.SelectCommand.CommandType = CommandType.Text;
                }
                if (commandType == CommandType.StoredProcedure)
                {
                    Da.SelectCommand.CommandType = CommandType.StoredProcedure;
                }
                if (commandType == CommandType.TableDirect)
                {
                    Da.SelectCommand.CommandType = CommandType.TableDirect;
                }


                foreach (var item in commandParameters)
                {
                    Da.SelectCommand.Parameters.AddWithValue(item.ParameterName, item.Value);
                }

                Da.Fill(Ds);

                return Ds;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static DataSet ExecuteDataset(string ConnectionString, CommandType commandType, string commandText)
        {
            try
            {

                MySqlConnection mysqlConn = new MySqlConnection(ConnectionString);
                MySqlDataAdapter Da = null;
                DataSet Ds = new DataSet();

                Da = new MySqlDataAdapter(commandText, mysqlConn);

                if (commandType == CommandType.Text)
                {
                    Da.SelectCommand.CommandType = CommandType.Text;
                }
                if (commandType == CommandType.StoredProcedure)
                {
                    Da.SelectCommand.CommandType = CommandType.StoredProcedure;
                }
                if (commandType == CommandType.TableDirect)
                {
                    Da.SelectCommand.CommandType = CommandType.TableDirect;
                }

                Da.Fill(Ds);
                return Ds;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static int ExecuteNonQuery(string ConnectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {

            MySqlConnection mysqlConn = new MySqlConnection(ConnectionString);
            MySqlCommand cmd = new MySqlCommand(commandText, mysqlConn);

            try
            {
                if (commandType == CommandType.Text)
                {
                    cmd.CommandType = CommandType.Text;
                }
                if (commandType == CommandType.StoredProcedure)
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                }
                if (commandType == CommandType.TableDirect)
                {
                    cmd.CommandType = CommandType.TableDirect;
                }

                foreach (var item in commandParameters)
                {
                    cmd.Parameters.AddWithValue(item.ParameterName, item.Value);
                }

                if (mysqlConn.State == ConnectionState.Closed) mysqlConn.Open();

                return cmd.ExecuteNonQuery();

            }
            catch (Exception)
            {
                return -1;
            }
            finally
            {
                mysqlConn.Close();
            }
        }

        public static int ExecuteNonQuery(string ConnectionString, CommandType commandType, string commandText)
        {

            MySqlConnection mysqlConn = new MySqlConnection(ConnectionString);
            MySqlCommand cmd = new MySqlCommand(commandText, mysqlConn);

            try
            {
                if (commandType == CommandType.Text)
                {
                    cmd.CommandType = CommandType.Text;
                }
                if (commandType == CommandType.StoredProcedure)
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                }
                if (commandType == CommandType.TableDirect)
                {
                    cmd.CommandType = CommandType.TableDirect;
                }

                if (mysqlConn.State == ConnectionState.Closed) mysqlConn.Open();

                return cmd.ExecuteNonQuery();

            }
            catch (Exception)
            {
                return -1;
            }
            finally
            {
                mysqlConn.Close();
            }
        }

        public static object ExecuteScalar(string ConnectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {

            MySqlConnection mysqlConn = new MySqlConnection(ConnectionString);
            MySqlCommand cmd = new MySqlCommand(commandText, mysqlConn);

            try
            {
                if (commandType == CommandType.Text)
                {
                    cmd.CommandType = CommandType.Text;
                }
                if (commandType == CommandType.StoredProcedure)
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                }
                if (commandType == CommandType.TableDirect)
                {
                    cmd.CommandType = CommandType.TableDirect;
                }

                foreach (var item in commandParameters)
                {
                    cmd.Parameters.AddWithValue(item.ParameterName, item.Value);
                }

                if (mysqlConn.State == ConnectionState.Closed) mysqlConn.Open();

                return cmd.ExecuteScalar();

            }
            catch (Exception)
            {
                return null;
            }
            finally
            {
                mysqlConn.Close();
            }
        }

        public static object ExecuteScalar(string ConnectionString, CommandType commandType, string commandText)
        {

            MySqlConnection mysqlConn = new MySqlConnection(ConnectionString);
            MySqlCommand cmd = new MySqlCommand(commandText, mysqlConn);

            try
            {
                if (commandType == CommandType.Text)
                {
                    cmd.CommandType = CommandType.Text;
                }
                if (commandType == CommandType.StoredProcedure)
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                }
                if (commandType == CommandType.TableDirect)
                {
                    cmd.CommandType = CommandType.TableDirect;
                }

                if (mysqlConn.State == ConnectionState.Closed) mysqlConn.Open();

                return cmd.ExecuteScalar();

            }
            catch (Exception)
            {
                return null;
            }
            finally
            {
                mysqlConn.Close();
            }
        }

        public static string CreateInsertQuery<T>(string TableName, T Fields, params string[] Extract)
        {
            string tmpQuery = "";
            string fields = "";
            string parameters = "";

            PropertyInfo[] propertyInfos;
            propertyInfos = typeof(T).GetProperties();

            foreach (PropertyInfo item in propertyInfos)
            {
                if (!Extract.Contains(item.Name))
                {
                    fields += item.Name + ",";

                    parameters += "?" + item.Name + ",";

                }
            }
            if (fields.Length > 0)
            {
                fields = fields.TrimEnd(',');
                parameters = parameters.TrimEnd(',');
            }

            tmpQuery = "INSERT INTO " + TableName + "(" + fields + ")VALUES(" + parameters + ")";

            return tmpQuery;
        }

        public static string CreateUpdateQuery<T>(string TableName, T Fields, CriteriaClass[] WhereCriteria, params string[] Extract)
        {
            string tmpQuery = "";
            string fieldsANDParameters = "";
            string whereCriteria = "";

            PropertyInfo[] propertyInfos;
            propertyInfos = typeof(T).GetProperties();

            foreach (PropertyInfo item in propertyInfos)
            {
                if (!Extract.Contains(item.Name))
                {
                    fieldsANDParameters += item.Name + "=?" + item.Name + ",";
                }
            }
            if (fieldsANDParameters.Length > 0)
            {
                fieldsANDParameters = fieldsANDParameters.TrimEnd(',');
            }

            if (WhereCriteria != null)
            {
                foreach (var item in WhereCriteria)
                {
                    whereCriteria += item.Key + " " + item.Operator + " " + item.Value + " AND ";
                }

                if (whereCriteria.Length > 0)
                {
                    whereCriteria = " WHERE " + whereCriteria;
                    char[] charDelete = new char[] { ' ', 'A', 'N', 'D', ' ' };
                    whereCriteria = whereCriteria.TrimEnd(charDelete);
                }

            }

            tmpQuery = "UPDATE " + TableName + " SET " + fieldsANDParameters + " " + whereCriteria;

            return tmpQuery;
        }

        public static string CreateDeleteQuery(string TableName, CriteriaClass[] WhereCriteria)
        {
            string tmpQuery = "";
            string whereCriteria = "";

            if (WhereCriteria != null)
            {
                foreach (var item in WhereCriteria)
                {
                    whereCriteria += item.Key + " " + item.Operator + " " + item.Value + " AND ";
                }

                if (whereCriteria.Length > 0)
                {
                    whereCriteria = " WHERE " + whereCriteria;
                    char[] charDelete = new char[] { ' ', 'A', 'N', 'D', ' ' };
                    whereCriteria = whereCriteria.TrimEnd(charDelete);
                }

                tmpQuery = "DELETE FROM " + TableName + " " + whereCriteria;
            }

            return tmpQuery;
        }

        public static string CreateBasicSelectQuery<T>(string TableName, T Fields, CriteriaClass[] WhereCriteria, string OrderBy, params string[] Extract)
        {
            string tmpQuery = "";
            string fields = "";
            string whereCriteria = "";
            string orderBy = "";

            PropertyInfo[] propertyInfos;
            propertyInfos = typeof(T).GetProperties();

            foreach (PropertyInfo item in propertyInfos)
            {
                if (Extract != null && Extract.Length > 0)
                {
                    if (!Extract.Contains(item.Name))
                    {
                        fields += item.Name + ",";
                    }
                }
                else
                {
                    fields += item.Name + ",";
                }

            }

            if (fields.Length > 0)
            {
                fields = fields.TrimEnd(',');
            }

            if (WhereCriteria != null)
            {
                foreach (var item in WhereCriteria)
                {
                    whereCriteria += item.Key + " " + item.Operator + " " + item.Value + " AND ";
                }

                if (whereCriteria.Length > 0)
                {
                    whereCriteria = " WHERE " + whereCriteria;
                    char[] charDelete = new char[] { ' ', 'A', 'N', 'D', ' ' };
                    whereCriteria = whereCriteria.TrimEnd(charDelete);
                }

            }

            if (OrderBy != null && OrderBy.Length > 0)
            {
                orderBy = "Order By " + OrderBy;
            }

            tmpQuery = "select " + fields + " from " + TableName + " " + whereCriteria + " " + orderBy;

            return tmpQuery;
        }

    }
    public class CriteriaClass
    {

        public CriteriaClass()
        { }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="_key">FieldName</param>
        /// <param name="_operator">=,>= ...</param>
        /// <param name="_value">Field Value</param>
        public CriteriaClass(string _key, string _operator, string _value)
        {
            Key = _key;
            Operator = _operator;
            Value = _value;
        }

        #region Properties
        public string Key { get; set; }
        public string Operator { get; set; }
        public string Value { get; set; }
        #endregion
    }
}
