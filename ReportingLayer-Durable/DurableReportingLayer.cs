using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ReportingLayer_Durable
{
    public static class DurableReportingLayer
    {
        [FunctionName("DurableReportingLayer")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
        {

            var data = context.GetInput<dynamic>();    

            await context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                                new
                                                {
                                                    caption = $"drop and recreate AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}",
                                                    data.connectionString,
                                                    queryString = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}')
                                                                        BEGIN                                                                                
                                                                            DROP TABLE AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}                                                                            
                                                                        END
                                                                        CREATE TABLE AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}
                                                                            ([_POBId] [nvarchar](4000) NULL,
	                                                                         [_POBName] [nvarchar](4000) NULL) 

                                                                        CREATE INDEX IX_AF_TEMP_App_CAMSchedule_Closed_Ideal
	                                                                        ON AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId} (_POBId)
	                                                                        INCLUDE (_POBName)"
                                                });
            context.SetCustomStatus($"drop and recreate AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}.. success");
            var insert_into_app_CAMSchedule_Closed_Ideal = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                                            new
                                                            {
                                                                caption = $"Insert into AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}",
                                                                data.connectionString,
                                                                queryString = $@"INSERT INTO AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId} 
                                                                                 SELECT DISTINCT PerfOblId, PerfOblName
                                                                                    FROM 
                                                                                 App_CAMPOData d
                                                                                 JOIN App_CAMGroups g on d.GroupId = g.GroupId
                                                                                 WHERE g.ModelId = '{data.modelId}'
                                                                                 Order by PerfOblId;"
                                                            });

            await insert_into_app_CAMSchedule_Closed_Ideal;
            context.SetCustomStatus($"Insert into App_CAMSchedule_Closed_Ideal_{data.modelId}..success");

            return insert_into_app_CAMSchedule_Closed_Ideal.Result.ToString();
            /*
            var app_CAMSchedule_Closed_Ideal = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery", 
                                                new { 
                                                    caption = "App_CAMSchedule_Closed_Ideal",
                                                    data.connectionString, 
                                                    queryString = $@"DROP TABLE App_CAMSchedule_Closed_Ideal_{data.modelId};
                                                                     INSERT INTO App_CAMSchedule_Closed_Ideal_{data.modelId} 
                                                                     SELECT DISTINCT PerfOblId, PerfOblName
                                                                        FROM 
                                                                     App_CAMPOData d
                                                                     JOIN App_CAMGroups g on d.GroupId = g.GroupId
                                                                     WHERE g.ModelId = '{data.modelId}'
                                                                     Order by PerfOblId;" });
            
            var appSource_Data_Rule = context.CallActivityAsync<List<IDataRecord>>("DurableReportingLayer_GetDataTable",
                                                new
                                                {
                                                    tableName = "AppSource_Data_Rule",
                                                    data.connectionString,
                                                    queryString = $@"SELECT DISTINCT
                                                                     d.PerfOblId,
                                                                     c.GroupId,
                                                                     c.GroupName,
                                                                     e.RuleId,
                                                                     e.RuleName
                                                                 FROM
                                                                 App_CAMPOSource b
                                                                 INNER JOIN App_CAMGroups c on b.GroupId = c.GroupId
                                                                 INNER JOIN App_CAMPOData d on  b.GroupId = d.GroupId and b.PerfOblId = d.PerfOblId
                                                                 INNER JOIN App_CAMRules e on d.RuleId = e.RuleId
                                                                 WHERE c.ModelId = '{data.modelId}'"
                                                });

            var app_CAMSchedule = context.CallActivityAsync<List<IDataRecord>>("DurableReportingLayer_GetDataTable",
                                                new
                                                {
                                                    tableName = "App_CAMSchedule",
                                                    data.connectionString,
                                                    queryString = $@"SELECT 
	                                                                    0 as Ideal,
	                                                                    s.PerfOblId,
	                                                                    s.PerfOblName,
	                                                                    convert(int, s.EntryType) as EntryType,
	                                                                    convert(int, s.TrueUp) as TrueUp,
	                                                                    d.name as TimeName,
	                                                                    s.Amount,
	                                                                    s.ModelId
                                                                    FROM App_CAMSchedule_{data.modelId}  s
	                                                                JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                                                                WHERE ISNULL(s.amount, 0) <> 0"
                                                });

            var app_CAMSchedule_Closed = context.CallActivityAsync<List<IDataRecord>>("DurableReportingLayer_GetDataTable",
                                                new
                                                {
                                                    tableName = "App_CAMSchedule_Closed",
                                                    data.connectionString,
                                                    queryString = $@"SELECT 
	                                                                    0 as Ideal,
	                                                                    s.PerfOblId,
	                                                                    s.PerfOblName,
	                                                                    convert(int, s.EntryType) as EntryType,
	                                                                    convert(int, s.TrueUp) as TrueUp,
	                                                                    d.name as TimeName,
	                                                                    s.Amount,
	                                                                    s.ModelId
                                                                    FROM App_CAMSchedule_Closed_{data.modelId}  s
	                                                                JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                                                                WHERE ISNULL(s.amount, 0) <> 0"
                                                });

            var app_CAMSchedule_Ideal = context.CallActivityAsync<List<IDataRecord>>("DurableReportingLayer_GetDataTable",
                                                new
                                                {
                                                    tableName = "App_CAMSchedule_Closed",
                                                    data.connectionString,
                                                    queryString = $@"SELECT 
	                                                                1 as Ideal,
	                                                                s.PerfOblId,
	                                                                s.PerfOblName,
	                                                                convert(int, s.EntryType) as EntryType,
	                                                                0 as TrueUp,
	                                                                d.name as TimeName,
	                                                                s.Amount,
	                                                                s.ModelId
                                                                FROM App_CAMSchedule_Ideal_{data.modelId}  s
	                                                            JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                                                            WHERE ISNULL(s.amount, 0) <> 0"
                                                });
           
            context.SetCustomStatus("awaiting all tasks");
            try
            {
                //await Task.WhenAll(new Task[] { app_CAMSchedule_Closed_Ideal, appSource_Data_Rule, app_CAMSchedule, app_CAMSchedule_Closed, app_CAMSchedule_Ideal });
                await app_CAMSchedule_Closed_Ideal;
                context.SetCustomStatus("finished gathering datasets");
                //var finalTable = app_CAMSchedule.Result.Concat(app_CAMSchedule_Closed.Result).Concat(app_CAMSchedule_Ideal.Result);
                //return finalTable.Count().ToString();
                return app_CAMSchedule_Closed_Ideal.Result.ToString();
            }
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                context.SetCustomStatus("Exception occured");
                return "function errored out";
            }
             */

        }

        [FunctionName("DurableReportingLayer_ExecuteQuery")]
        public static int ExecuteQuery([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var data = context.GetInput<dynamic>();

            using (SqlConnection conn = new SqlConnection((string)data.connectionString))
            {
                using (SqlCommand cmd = new SqlCommand((string)data.queryString, conn))
                {
                    cmd.CommandTimeout = 60 * 30; // 30 minutes
                    cmd.CommandType = CommandType.Text;

                    if (data.parameters != null)
                        cmd.Parameters.AddRange((SqlParameter[])data.parameters);

                    //var adapter = new SqlDataAdapter(cmd);
                    //adapter.Fill(dt);
                    conn.Open();
                    var ret = cmd.ExecuteNonQuery();
                    conn.Close();
                    log.LogInformation($"successfully executed {(string)data.caption}");
                    return ret;
                }
            }
        }

        [FunctionName("DurableReportingLayer_GetDataTable")]
        public static List<DataRow> GetDataTable([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var data = context.GetInput<dynamic>();

            DataTable dt = new DataTable((string)data.tableName);

            log.LogInformation($"attempting to get datatable {(string)data.tableName}");

            using (SqlConnection conn = new SqlConnection((string)data.connectionString))
            {
                using (SqlCommand cmd = new SqlCommand((string)data.queryString, conn))
                {
                    cmd.CommandTimeout = 60 * 30; // 30 minutes
                    cmd.CommandType = CommandType.Text;

                    if (data.parameters != null)
                        cmd.Parameters.AddRange((SqlParameter[])data.parameters);

                    //var adapter = new SqlDataAdapter(cmd);
                    //adapter.Fill(dt);
                    var ret = new List<DataRow>();
                    conn.Open();
                    var reader = cmd.ExecuteReader();
                    IReadOnlyCollection<DbColumn> schema = null;
                    var dtSchema = new DataTable();
                    while (reader.Read())
                    {
                        if (schema == null)
                        {
                            schema = reader.GetColumnSchema();
                            foreach (var col in schema)
                            {
                                dtSchema.Columns.Add(col.ColumnName, col.DataType);
                            }
                        }
                        var dr = dtSchema.NewRow();
                        reader.GetValues(dr.ItemArray);
                        ret.Add(dr);
                    }
                    conn.Close();
                    log.LogInformation($"acquired datatable {(string)data.tableName}");
                    return ret;
                }
            }
        }

        [FunctionName("DurableReportingLayer_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {
            string requestBody = await req.Content.ReadAsStringAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            if (data == null)
                return new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest) { Content = new StringContent("request format is invalid") };
            //declare a predicate for checking if the dynamic has a given property
            Func<object, string, bool> hasProperty = (jsonObject, prop) => ((JObject)jsonObject).Property(prop) != null;

            if (!hasProperty(data, "connectionString"))
                return new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest) { Content = new StringContent("Connection string is missing from post body") };

            if (!hasProperty(data, "modelId"))
                return new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest) { Content = new StringContent("Model Id is missing from post body") };

            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("DurableReportingLayer", data);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}