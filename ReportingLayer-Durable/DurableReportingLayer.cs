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
        const int COMMAND_TIMEOUT = 60 * 60 * 5; // sql command timeout in seconds set to 5 hours

        [FunctionName("DurableReportingLayer")]
        public static async Task<long> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            var data = context.GetInput<dynamic>();

            var sizeOfPerfOblId =  context.CallActivityAsync<int>("DurableReportingLayer_ExecuteScalar",
                                    new
                                    {
                                        caption = $"get size of PerfOblId column",
                                        data.connectionString,
                                        queryString = $@"SELECT MAX(LEN(PerfOblId))
                                                        FROM 
                                                        App_CAMPOData d
                                                        JOIN App_CAMGroups g on d.GroupId = g.GroupId
                                                        WHERE g.ModelId = '{data.modelId}';"
                                    });
            var sizeOfPerfOblName = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteScalar",
                                   new
                                   {
                                       caption = $"get size of PerfOblName column",
                                       data.connectionString,
                                       queryString = $@"SELECT MAX(LEN(PerfOblName))
                                                        FROM 
                                                        App_CAMPOData d
                                                        JOIN App_CAMGroups g on d.GroupId = g.GroupId
                                                        WHERE g.ModelId = '{data.modelId}';"
                                   });
            var modelNames = context.CallActivityAsync<DataTable>("DurableReportingLayer_GetDataTable",
                                   new
                                   {
                                       tableName = $"ModelNames",
                                       data.connectionString,
                                       queryString = $@"SELECT DISTINCT ModelName
	                                                    FROM Models M
	                                                    Join App_CAMGroups G on M.ModelId = G.POSourceModelId
	                                                    WHERE G.ModelId = '{data.modelId}';"
                                   });
            await Task.WhenAll(sizeOfPerfOblId, sizeOfPerfOblName, modelNames);



            await context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                               new
                                               {
                                                   caption = $"drop and recreate AF_TEMP_AppSource_Data_Rule_{data.modelId}",
                                                   data.connectionString,
                                                   queryString = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AF_TEMP_AppSource_Data_Rule_{data.modelId}')
                                                                        BEGIN                                                                                
                                                                            DROP TABLE AF_TEMP_AppSource_Data_Rule_{data.modelId};                                                                            
                                                                        END
                                                                        CREATE TABLE AF_TEMP_AppSource_Data_Rule_{data.modelId}
                                                                            (   PerfOblId NVARCHAR({sizeOfPerfOblId.Result}),
	                                                                            GroupId Int,
	                                                                            GroupName NVARCHAR(256),  
	                                                                            RuleId Int,
	                                                                            RuleName NVARCHAR(256)); 

                                                                        CREATE NONCLUSTERED INDEX [IX_AF_TEMP_AppSource_Data_Rule_{data.modelId}]
                                                                        ON [dbo].[AF_TEMP_AppSource_Data_Rule_{data.modelId}] ([PerfOblId], [RuleID], [GroupName], [RuleName])
                                                                        INCLUDE ([GroupId]);"
                                               });

            //context.SetCustomStatus($"drop and recreate AF_TEMP_AppSource_Data_Rule_{data.modelId} table definition.. success");

            string appSource_Data_Rule_Insert_Qry = $@"INSERT INTO AF_TEMP_AppSource_Data_Rule_{data.modelId}
                                                       SELECT DISTINCT * FROM (";

            foreach(DataRow r in modelNames.Result.Rows)
            {
                appSource_Data_Rule_Insert_Qry += $@"
                                                SELECT  
				                                    b.PerfOblId_Dest,
				                                    c.GroupId,
				                                    c.GroupName,
				                                    e.RuleId,
				                                    e.RuleName
			                                    FROM App_CAM_Trans_Data_{data.modelId}_{r["ModelName"]} b
			                                    JOIN App_CAMGroups c ON b.GroupId = c.GroupId
			                                    JOIN App_CAMRules e ON b.RuleId = e.RuleId
			                                    WHERE c.ModelId = '{data.modelId}'";
                if (modelNames.Result.Rows.IndexOf(r) != modelNames.Result.Rows.Count && modelNames.Result.Rows.Count > 1)
                    appSource_Data_Rule_Insert_Qry += $"{Environment.NewLine} UNION ALL";
            }
            appSource_Data_Rule_Insert_Qry += $"{Environment.NewLine} ) t";

            var schedule_Closed_Ideal =  context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                                new
                                                {
                                                    caption = $"drop and recreate AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}",
                                                    data.connectionString,
                                                    queryString = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}')
                                                                        BEGIN                                                                                
                                                                            DROP TABLE AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId};                                                                            
                                                                        END
                                                                        CREATE TABLE AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}
                                                                            ([_POBId] [nvarchar]({sizeOfPerfOblId.Result}) NULL,
	                                                                         [_POBName] [nvarchar]({sizeOfPerfOblName.Result}) NULL); 

                                                                        CREATE INDEX IX_AF_TEMP_App_CAMSchedule_Closed_Ideal
	                                                                        ON AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId} (_POBId)
	                                                                        INCLUDE (_POBName);

                                                                        INSERT INTO AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId} 
                                                                        SELECT DISTINCT PerfOblId, PerfOblName
                                                                        FROM 
                                                                        App_CAMPOData d
                                                                        JOIN App_CAMGroups g on d.GroupId = g.GroupId
                                                                        WHERE g.ModelId = '{data.modelId}'
                                                                        Order by PerfOblId;"
                                                });

            var appSource_Data_Rule = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                                new
                                                {
                                                    caption = $"load AF_TEMP_AppSource_Data_Rule_{data.modelId}",
                                                    data.connectionString,
                                                    queryString = appSource_Data_Rule_Insert_Qry
                                                });

            await Task.WhenAll(schedule_Closed_Ideal, appSource_Data_Rule);
            //context.SetCustomStatus($"drop and recreate AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId}.. success");


            var app_CAMSchedule = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                                new
                                                {
                                                    caption = $"drop and recreate AF_TEMP_App_CAMSchedule_{data.modelId}",
                                                    data.connectionString,
                                                    queryString = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AF_TEMP_App_CAMSchedule_{data.modelId}')
                                                                        BEGIN                                                                                
                                                                            DROP TABLE AF_TEMP_App_CAMSchedule_{data.modelId};                                                                            
                                                                        END

                                                                        SELECT 
	                                                                        0 as Ideal,
	                                                                        s.PerfOblId,
	                                                                        s.PerfOblName,
	                                                                        convert(int, s.EntryType) as EntryType,
	                                                                        convert(int, s.TrueUp) as TrueUp,
	                                                                        d.name as TimeName,
	                                                                        s.Amount,
	                                                                        s.ModelId,
	                                                                        a.GroupId,
	                                                                        a.GroupName,
	                                                                        a.RuleId,
	                                                                        a.RuleName
                                                                        Into AF_TEMP_App_CAMSchedule_{data.modelId} FROM App_CAMSchedule_{data.modelId}  s
	                                                                        JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                                                                        LEFT JOIN AF_TEMP_AppSource_Data_Rule_{data.modelId} a on a.PerfOblId = s.PerfOblId 
	                                                                        WHERE ISNULL(s.amount, 0) <> 0
	                                                                        AND a.RuleID IS NOT NULL 
	                                                                        AND a.GroupName IS NOT NULL 
	                                                                        AND a.RuleName IS NOT NULL"
                                                });
            var app_CAMSchedule_Closed = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                                new
                                                {
                                                    caption = $"drop and recreate AF_TEMP_App_CAMSchedule_Closed_{data.modelId}",
                                                    data.connectionString,
                                                    queryString = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AF_TEMP_App_CAMSchedule_Closed_{data.modelId}')
                                                                        BEGIN                                                                                
                                                                            DROP TABLE AF_TEMP_App_CAMSchedule_Closed_{data.modelId};                                                                            
                                                                        END

                                                                        SELECT 
	                                                                        0 as Ideal,
	                                                                        s.PerfOblId,
	                                                                        s.PerfOblName,
	                                                                        convert(int, s.EntryType) as EntryType,
	                                                                        convert(int, s.TrueUp) as TrueUp,
	                                                                        d.name as TimeName,
	                                                                        s.Amount,
	                                                                        s.ModelId,
	                                                                        a.GroupId,
	                                                                        a.GroupName,
	                                                                        a.RuleId,
	                                                                        a.RuleName
                                                                        Into AF_TEMP_App_CAMSchedule_Closed_{data.modelId} FROM App_CAMSchedule_Closed_{data.modelId}  s
	                                                                        JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                                                                        LEFT JOIN AF_TEMP_AppSource_Data_Rule_{data.modelId} a on a.PerfOblId = s.PerfOblId 
	                                                                        WHERE ISNULL(s.amount, 0) <> 0
	                                                                        AND a.RuleID IS NOT NULL 
	                                                                        AND a.GroupName IS NOT NULL 
	                                                                        AND a.RuleName IS NOT NULL"
                                                });

            var app_CAMSchedule_Ideal = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                                new
                                                {
                                                    caption = $"drop and recreate AF_TEMP_App_CAMSchedule_Ideal_{data.modelId}",
                                                    data.connectionString,
                                                    queryString = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AF_TEMP_App_CAMSchedule_Ideal_{data.modelId}')
                                                                        BEGIN                                                                                
                                                                            DROP TABLE AF_TEMP_App_CAMSchedule_Ideal_{data.modelId};                                                                            
                                                                        END

                                                                        SELECT 
	                                                                        1 as Ideal,
	                                                                        s.PerfOblId,
	                                                                        s.PerfOblName,
	                                                                        convert(int, s.EntryType) as EntryType,
	                                                                        0 as TrueUp,
	                                                                        d.name as TimeName,
	                                                                        s.Amount,
	                                                                        s.ModelId,
	                                                                        a.GroupId,
	                                                                        a.GroupName,
	                                                                        a.RuleId,
	                                                                        a.RuleName
                                                                        Into AF_TEMP_App_CAMSchedule_Ideal_{data.modelId} FROM App_CAMSchedule_Ideal_{data.modelId}  s
	                                                                        JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                                                                        LEFT JOIN AF_TEMP_AppSource_Data_Rule_{data.modelId} a on a.PerfOblId = s.PerfOblId 
	                                                                        WHERE ISNULL(s.amount, 0) <> 0
	                                                                        AND a.RuleID IS NOT NULL 
	                                                                        AND a.GroupName IS NOT NULL 
	                                                                        AND a.RuleName IS NOT NULL"
                                                });

            await Task.WhenAll(app_CAMSchedule, app_CAMSchedule_Closed, app_CAMSchedule_Ideal);

            await context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
                                                new
                                                {
                                                    caption = $"drop and recreate AF_TEMP_UNION_{data.modelId}",
                                                    data.connectionString,
                                                    queryString = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AF_TEMP_UNION_{data.modelId}')
                                                                        BEGIN                                                                                
                                                                            DROP TABLE AF_TEMP_UNION_{data.modelId};                                                                            
                                                                        END
                                                                        Select * INTO AF_TEMP_UNION_{data.modelId} from (
                                                                            Select * from AF_TEMP_App_CAMSchedule_{data.modelId}
				                                                                UNION ALL
			                                                                Select * from AF_TEMP_App_CAMSchedule_Closed_{data.modelId}
				                                                                UNION ALL
			                                                                Select * from AF_TEMP_App_CAMSchedule_Ideal_{data.modelId}
                                                                        ) t
                                                                        "
                                                });

            return (sw.ElapsedMilliseconds) ;
        }

        [FunctionName("DurableReportingLayer_ExecuteScalar")]
        public static object ExecuteScalar([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var data = context.GetInput<dynamic>();

            using (SqlConnection conn = new SqlConnection((string)data.connectionString))
            {
                using (SqlCommand cmd = new SqlCommand((string)data.queryString, conn))
                {
                    cmd.CommandTimeout = COMMAND_TIMEOUT;
                    cmd.CommandType = CommandType.Text;

                    if (data.parameters != null)
                        cmd.Parameters.AddRange((SqlParameter[])data.parameters);

                    //var adapter = new SqlDataAdapter(cmd);
                    //adapter.Fill(dt);
                    conn.Open();
                    var ret = cmd.ExecuteScalar();
                    conn.Close();
                    log.LogInformation($"successfully executed {(string)data.caption}");
                    return ret;
                }
            }
        }

        [FunctionName("DurableReportingLayer_ExecuteQuery")]
        public static int ExecuteQuery([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var data = context.GetInput<dynamic>();

            using (SqlConnection conn = new SqlConnection((string)data.connectionString))
            {
                using (SqlCommand cmd = new SqlCommand((string)data.queryString, conn))
                {
                    cmd.CommandTimeout = COMMAND_TIMEOUT;
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
        public static DataTable GetDataTable([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var data = context.GetInput<dynamic>();

            DataTable dt = new DataTable((string)data.tableName);

            log.LogInformation($"attempting to get datatable {(string)data.tableName}");

            using (SqlConnection conn = new SqlConnection((string)data.connectionString))
            {
                using (SqlCommand cmd = new SqlCommand((string)data.queryString, conn))
                {
                    cmd.CommandTimeout = COMMAND_TIMEOUT;
                    cmd.CommandType = CommandType.Text;

                    if (data.parameters != null)
                        cmd.Parameters.AddRange((SqlParameter[])data.parameters);

                    var adapter = new SqlDataAdapter(cmd);
                    adapter.Fill(dt);
                    return dt;
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