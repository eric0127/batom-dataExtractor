using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;
using XmlRpc;
using log4net;
using log4net.Config;

namespace TeslaDataExtractor
{
    class Program
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(Program));
        private static string odooUrl = "http://192.168.10.143:8069/xmlrpc/2", db = "odoo-batom", pass = "batom", user = "admin";
        private static string applicableModels = "('110438')";
        private static string rootFolder = @"C:\ICM802\";
        private static OleDbConnection conn = null;
        private static XmlRpcClient client;
        private static XmlRpcResponse responseLogin;
        
        static void Main(string[] args)
        {
            XmlConfigurator.Configure(new System.IO.FileInfo("./log4net.config"));
            logger.Info("TeslaDataExtractor started");
            if (Connect())
            {
                string timestampCondition = "";
                string lastProcessedTime = GetLastProcessedTime();
                if (lastProcessedTime != "")
                    timestampCondition = "`日期時間` > #" + lastProcessedTime + "# and ";
                ExtractData("select * from `檢測資料` where " +
                    timestampCondition +
                    "`加工機種名稱` in " + applicableModels);
            }
            Disconnect();
        }

        private static bool Connect()
        {
            bool rc = false;
            try
            {
                var DBPath = rootFolder + "ICM802.mdb";

                conn = new OleDbConnection("Provider=Microsoft.Jet.OleDb.4.0;"
                    + "Data Source=" + DBPath);
                conn.Open();
                
                client = new XmlRpcClient();
                client.Url = odooUrl;
                client.Path = "common";           

                // LOGIN

                XmlRpcRequest requestLogin = new XmlRpcRequest("authenticate");
                requestLogin.AddParams(db, user, pass, XmlRpcParameter.EmptyStruct());

                responseLogin = client.Execute(requestLogin);

                if (responseLogin.IsFault())
                {
                    logger.Error("無法連線到 odoo 資料庫");
                }
                else
                {
                    rc = true;
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message, ex);
            }
            
            return rc;
        }

        private static void Disconnect()
        {
            if (conn != null)
            {
                conn.Close();
                conn = null;
            }
        }

        private static void Insert(string sql)
        {
            try
            {
                using (OleDbCommand cmd = new OleDbCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
            }
        }

        private static void ExtractData(string sql)
        {
            try
            {
                using (DataTable dt = new DataTable())
                {
                    using (OleDbDataAdapter adapter = new OleDbDataAdapter(sql, conn))
                    {
                        string lastProcessedTime = "";
                        adapter.Fill(dt);
                        foreach (DataRow row in dt.Rows)
                        {
                            lastProcessedTime = AddToOdoo(row.ItemArray);
                        }
                        if (lastProcessedTime != "")
                            SetLastProcessedTime(lastProcessedTime);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message, ex);
            }
        }

        private static void Update(string sql)
        {
            try
            {
                using (OleDbCommand cmd = new OleDbCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message, ex);
            }
        }

        private static void Delete(string sql)
        {
            try
            {
                using (OleDbCommand cmd = new OleDbCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message, ex);
            }
        }

        private static string GetLastProcessedTime()
        {
            string lastProcessedTime = "";
            try
            {
                using (DataTable dt = new DataTable())
                {
                    string sql = "select setting_value from setting where setting_name = 'last_processed_time'";
                    using (OleDbDataAdapter adapter = new OleDbDataAdapter(sql, conn))
                    {
                        adapter.Fill(dt);
                        if (dt.Rows.Count > 0)
                        {
                            lastProcessedTime = (string)dt.Rows[0].ItemArray[0];
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message, ex);
            }
            
            return lastProcessedTime;
        }

        private static void SetLastProcessedTime(string lastProcessedTime)
        {
            try
            {
                using (DataTable dt = new DataTable())
                {
                    string sql = "select setting_value from setting where setting_name = 'last_processed_time'";
                    using (OleDbDataAdapter adapter = new OleDbDataAdapter(sql, conn))
                    {
                        adapter.Fill(dt);
                        if (dt.Rows.Count > 0)
                        {
                            Update("update setting set setting_value = '" + lastProcessedTime + "' " +
                                "where setting_name = 'last_processed_time'");
                        }
                        else
                        {
                            Insert("insert into setting (setting_name, setting_value) values " +
                                "('last_processed_time', '" + lastProcessedTime + "')");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message, ex);
            }
        }
        
        private static string AddToOdoo(object[] pressValues)
        {
            string timestampStr = "";
            try
            {
                string gear_qr = ((string)pressValues[0]).Trim();
                string pinion_qr = ((string)pressValues[1]).Trim();
                string product_model = (string)pressValues[2];
                DateTime timestamp = (DateTime)pressValues[3];
                timestampStr = timestamp.ToString();
                float press_distance = float.Parse((string)pressValues[4]);
                float press_duration = float.Parse((string)pressValues[5]);
                float compressor_pressure = float.Parse((string)pressValues[6]);
                float final_pressure = float.Parse((string)pressValues[7]);
                float pos1_pressure = float.Parse((string)pressValues[8]);
                float pos2_pressure = float.Parse((string)pressValues[9]);
                float pos3_pressure = float.Parse((string)pressValues[10]);
                float pos4_pressure = float.Parse((string)pressValues[11]);
                float pos5_pressure = float.Parse((string)pressValues[12]);
                string result = (string)pressValues[13];
                string ng_reason = (string)pressValues[14];
                string chartFileName = GetChartFile(timestamp);
                if (chartFileName != "")
                {
                    try
                    {
                        string destDir = rootFolder + @"110438";
                        if (!Directory.Exists(destDir))
                            Directory.CreateDirectory(destDir);
                        File.Copy(rootFolder + @"JPG\" + chartFileName, destDir + @"\" + chartFileName);
                    }
                    catch (Exception e1)
                    {
                        logger.Error(e1.Message, e1);
                    }
                }
                logger.Info("QR1: " + gear_qr);
                logger.Info("QR2: " + pinion_qr);
                logger.Info("Time: " + timestamp.ToString());
                logger.Info("chart: " + chartFileName);
                
                if (gear_qr == "" && pinion_qr == "")
                {
                    logger.Warn("No QR code");
                }
                else
                {
                    client.Path = "object";

                    List<object> domain = new List<object>();
                    if (gear_qr != "")
                        domain.Add(XmlRpcParameter.AsArray("gear_qr", "=", gear_qr));
                    if (pinion_qr != "")
                        domain.Add(XmlRpcParameter.AsArray("pinion_qr", "=", pinion_qr));
                    
                    if (domain.Count == 2)
                    {
                        domain.Insert(0, "|");
                    }
                    XmlRpcRequest requestSearch = new XmlRpcRequest("execute_kw");
                    requestSearch.AddParams(db, responseLogin.GetInt(), pass, "batom.tesla.qrcode", "search_read", 
                        XmlRpcParameter.AsArray(
                            domain
                        ),
                        XmlRpcParameter.AsStruct(
                            XmlRpcParameter.AsMember("fields", XmlRpcParameter.AsArray("gear_qr", "pinion_qr"))
                            // ,XmlRpcParameter.AsMember("limit", 2)
                        )
                    );                      

                    XmlRpcResponse responseSearch = client.Execute(requestSearch);

                    if (responseSearch.IsFault())
                    {
                        logger.Error(responseSearch.GetFaultString());
                    }
                    else if (!responseSearch.IsArray())
                    {
                        logger.Error("查詢 odoo 資料庫異常");
                    }
                    else
                    {
                        int qr_id = -1; // used as the flag (ok: qr_id > 0)
                        List<Object> valueList = responseSearch.GetArray();
                        if (valueList.Count == 0)
                        {
                            Dictionary<string, object> values = new Dictionary<string, object>();
                            if (gear_qr != "")
                                values["gear_qr"] = gear_qr;
                            if (pinion_qr != "")
                                values["pinion_qr"] = pinion_qr;
                            XmlRpcRequest requestCreate = new XmlRpcRequest("execute_kw");
                            requestCreate.AddParams(db, responseLogin.GetInt(), pass, "batom.tesla.qrcode", "create", 
                                XmlRpcParameter.AsArray(values)
                            );                      

                            XmlRpcResponse responseCreate = client.Execute(requestCreate);
                            if (responseCreate.IsFault())
                            {
                                logger.Error(responseCreate.GetFaultString());
                            }
                            else
                            {
                                qr_id = responseCreate.GetInt();
                            }
                        }
                        else
                        {
                            string db_gear_qr = "";
                            string db_pinion_qr = "";
                            foreach (Dictionary<string, object> valueDictionary in valueList)
                            {
                                foreach (KeyValuePair<string, object> kvp in valueDictionary)
                                {
                                    if (kvp.Key == "id")
                                        qr_id = (int)kvp.Value;
                                    else if (kvp.Key == "gear_qr" && kvp.Value is string)
                                        db_gear_qr = (string)kvp.Value;
                                    else if (kvp.Key == "pinion_qr" && kvp.Value is string)
                                        db_pinion_qr = (string)kvp.Value;
                                }
                            }
                            
                            if ((gear_qr == "" || gear_qr == db_gear_qr) && 
                                (pinion_qr == "" || pinion_qr == db_pinion_qr))
                            {
                                // existing qr record, do nothing
                            }
                            else if (ValueConflict(gear_qr, db_gear_qr) ||
                                ValueConflict(pinion_qr, db_pinion_qr))
                            {
                                logger.Error("與資料庫中下列 QR code 組合衝突，無法儲存：\n" +
                                    "軸：　" + db_pinion_qr + "\n" +
                                    "餅：　" + db_gear_qr
                                );
                            }
                            else
                            {
                                Dictionary<string, object> values = new Dictionary<string, object>();
                                if (gear_qr != "")
                                    values["gear_qr"] = gear_qr;
                                if (pinion_qr != "")
                                    values["pinion_qr"] = pinion_qr;
                                XmlRpcRequest requestWrite = new XmlRpcRequest("execute_kw");
                                requestWrite.AddParams(db, responseLogin.GetInt(), pass, "batom.tesla.qrcode", "write",
                                    XmlRpcParameter.AsArray(XmlRpcParameter.AsArray(qr_id), values)
                                );                      

                                XmlRpcResponse responseWrite = client.Execute(requestWrite);

                                if (responseWrite.IsFault())
                                {
                                    logger.Error(responseWrite.GetFaultString());
                                    qr_id = -1;
                                }
                            }
                        }
                        
                        if (qr_id > 0)
                        {
                            domain = new List<object>();
                            domain.Add(XmlRpcParameter.AsArray("timestamp", "=", timestampStr));
                            requestSearch = new XmlRpcRequest("execute_kw");
                            requestSearch.AddParams(db, responseLogin.GetInt(), pass, "batom.tesla.press_info", "search_read", 
                                XmlRpcParameter.AsArray(
                                    domain
                                ),
                                XmlRpcParameter.AsStruct(
                                    XmlRpcParameter.AsMember("fields", XmlRpcParameter.AsArray("timestamp"))
                                    // ,XmlRpcParameter.AsMember("limit", 2)
                                )
                            );                      

                            responseSearch = client.Execute(requestSearch);

                            if (responseSearch.IsFault())
                            {
                                logger.Error(responseSearch.GetFaultString());
                            }
                            else if (!responseSearch.IsArray())
                            {
                                logger.Error("查詢 odoo 資料庫異常");
                            }
                            else
                            {
                                valueList = responseSearch.GetArray();
                                if (valueList.Count > 0)
                                {
                                    logger.Warn("壓配資料已存在");
                                }
                                else
                                {
                                    Dictionary<string, object> values = new Dictionary<string, object>();
                                    values["qr_id"] = qr_id;
                                    values["product_model"] = product_model;
                                    values["timestamp"] = timestampStr;
                                    values["press_distance"] = press_distance;
                                    values["press_duration"] = press_duration;
                                    values["compressor_pressure"] = compressor_pressure;
                                    values["final_pressure"] = final_pressure;
                                    values["pos1_pressure"] = pos1_pressure;
                                    values["pos2_pressure"] = pos2_pressure;
                                    values["pos3_pressure"] = pos3_pressure;
                                    values["pos4_pressure"] = pos4_pressure;
                                    values["pos5_pressure"] = pos5_pressure;
                                    values["result"] = result;
                                    values["ng_reason"] = ng_reason;
                                    values["chart_file_name"] = chartFileName;
                                    if (chartFileName != "")
                                    {
                                        try
                                        {
                                            values["chart"] = Convert.ToBase64String(File.ReadAllBytes(rootFolder + @"JPG\" + chartFileName));
                                        }
                                        catch (Exception e2)
                                        {
                                            logger.Error(e2.Message, e2);
                                        }
                                    }
                                    else
                                        values["chart"] = false;
                                    XmlRpcRequest requestCreate = new XmlRpcRequest("execute_kw");
                                    requestCreate.AddParams(db, responseLogin.GetInt(), pass, "batom.tesla.press_info", "create", 
                                        XmlRpcParameter.AsArray(values)
                                    );                      

                                    XmlRpcResponse responseCreate = client.Execute(requestCreate);
                                    if (responseCreate.IsFault())
                                    {
                                        logger.Error(responseCreate.GetFaultString());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message, ex);
            }
            
            return timestampStr;
        }
        
        private static bool ValueConflict(string value, string dbValue)
        {
            return (value != dbValue && value != "" && dbValue != "");
        }
        
        private static string GetChartFile(DateTime timestamp)
        {
            string[] fileEntries = Directory.GetFiles(rootFolder + "JPG");
            foreach (string filePath in fileEntries)
            {
                try
                {
                    string fileName = Path.GetFileName(filePath);
                    DateTime fileTimestamp = new DateTime(
                        int.Parse(fileName.Substring(0, 4)),
                        int.Parse(fileName.Substring(4, 2)),
                        int.Parse(fileName.Substring(6, 2)),
                        int.Parse(fileName.Substring(8, 2)),
                        int.Parse(fileName.Substring(10, 2)),
                        int.Parse(fileName.Substring(12, 2)));
                    TimeSpan ts = timestamp - fileTimestamp;
                    if (0 <= ts.TotalSeconds && ts.TotalSeconds <= 2)
                        return fileName;
                }
                catch (Exception ex)
                {
                    logger.Error(ex.Message, ex);
                }
            }
            
            return "";
        }
    }
}
