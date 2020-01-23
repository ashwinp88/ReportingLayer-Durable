using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
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
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {

            var data = context.GetInput<dynamic>();    
            
            var app_CAMSchedule_Closed_Ideal = context.CallActivityAsync<DataTable>("DurableReportingLayer_GetDataTable", 
                                                new { 
                                                    tableName = "App_CAMSchedule_Closed_Ideal",
                                                    data.connectionString, 
                                                    queryString = $@"SELECT DISTINCT PerfOblId, PerfOblName
                                                                        FROM 
                                                                    App_CAMPOData d
                                                                    JOIN App_CAMGroups g on d.GroupId = g.GroupId
                                                                    WHERE g.ModelId = '{data.modelId}'
                                                                    Order by PerfOblId" });

            var appSource_Data_Rule = context.CallActivityAsync<DataTable>("DurableReportingLayer_GetDataTable",
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

            var app_CAMSchedule = context.CallActivityAsync<DataTable>("DurableReportingLayer_GetDataTable",
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

            var app_CAMSchedule_Closed = context.CallActivityAsync<DataTable>("DurableReportingLayer_GetDataTable",
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

            var app_CAMSchedule_Ideal = context.CallActivityAsync<DataTable>("DurableReportingLayer_GetDataTable",
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
            await Task.WhenAll(new Task[] { app_CAMSchedule_Closed_Ideal, appSource_Data_Rule, app_CAMSchedule, app_CAMSchedule_Closed, app_CAMSchedule_Ideal });
            context.SetCustomStatus("finished gathering datasets");
            var finalTable = app_CAMSchedule.Result;
            finalTable.Merge(app_CAMSchedule_Closed.Result);
            finalTable.Merge(app_CAMSchedule_Ideal.Result);
            
            return finalTable.Rows.Count.ToString();
        }

        [FunctionName("DurableReportingLayer_GetDataTable")]
        public static DataTable GetDataTable([ActivityTrigger] DurableActivityContext context, ILogger log)
        {
            var data = context.GetInput<dynamic>();

            DataTable dt = new DataTable((string)data.tableName);
            using (SqlConnection conn = new SqlConnection((string)data.connectionString))
            {
                using (SqlCommand cmd = new SqlCommand((string)data.queryString, conn))
                {
                    cmd.CommandTimeout = 60 * 30; // 30 minutes
                    cmd.CommandType = CommandType.Text;

                    if (data.parameters != null)
                        cmd.Parameters.AddRange((SqlParameter[])data.parameters);

                    var adapter = new SqlDataAdapter(cmd);
                    adapter.Fill(dt);
                }
            }
            return dt;
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