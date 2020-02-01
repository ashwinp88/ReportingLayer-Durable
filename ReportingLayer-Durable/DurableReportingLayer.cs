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
			var data = context.GetInput<dynamic>();

			var sizeOfPerfOblId =  context.CallActivityAsync<int>("DurableReportingLayer_ExecuteScalar",
									new {  caption = $"get size of PerfOblId column",
										   data.connectionString,
										   queryString = $@"SELECT MAX(LEN(PerfOblId))
															FROM 
															App_CAMPOData d
															JOIN App_CAMGroups g on d.GroupId = g.GroupId
															WHERE g.ModelId = '{data.modelId}';" });
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
			var createAndInsertStrings = context.CallActivityAsync<DataTable>("DurableReportingLayer_GetDataTable",
										   new
										   {
											   tableName = $"Strings",
											   data.connectionString,
											   queryString = $@"SELECT Stuff((SELECT ', ' + '[' + fieldname + ']' 
																			  FROM   app_camreportingfields 
																			  WHERE  modelid = '{data.modelId}' 
																			  ORDER  BY fieldname 
																			  FOR xml path ('')), 1, 1, '') AS [Insert], 
																	   Stuff((SELECT ', ' + '[' + fieldname + '] NVARCHAR(256)' 
																			  FROM   app_camreportingfields 
																			  WHERE  modelid = '{data.modelId}'
																			  ORDER  BY fieldname 
																			  FOR xml path ('')), 1, 1, '') AS [Create], 
																	   Stuff((SELECT ', ' + '[' + membername + ']' 
																			  FROM   dbo.app_camperiodmanagement a 
																					 INNER JOIN hierpc_time b 
																							 ON a.timeid = b.memberid 
																			  WHERE  modelid = '{data.modelId}'
																			  FOR xml path ('')), 1, 1, '') AS CalendarInsert, 
																	   Stuff((SELECT ', ' + '[' + membername + '] FLOAT' 
																			  FROM   dbo.app_camperiodmanagement a 
																					 INNER JOIN hierpc_time b 
																							 ON a.timeid = b.memberid 
																			  WHERE  modelid = '{data.modelId}'
																			  FOR xml path ('')), 1, 1, '') AS CalendarCreate, 
																	   Stuff((SELECT ', ' + ' SUM(ISNULL([' + membername 
																					 + '], 0)) as [' + membername + ']' 
																			  FROM   dbo.app_camperiodmanagement a 
																					 INNER JOIN hierpc_time b 
																							 ON a.timeid = b.memberid 
																			  WHERE  modelid = '{data.modelId}'
																			  FOR xml path ('')), 1, 1, '') AS SumCalendar ;"
										   });

			var modelNames = context.CallActivityAsync<DataTable>("DurableReportingLayer_GetDataTable",
								new
								{
									tableName = $"ModelNames",
									data.connectionString,
									queryString = $@";WITH cte 
													 AS (SELECT DISTINCT 
														' MAX(CAST(' + CASE WHEN SDE.fieldvalue = -1 THEN 'Null' WHEN 
														SDE.fieldvalue = 1 
														THEN dbo.App_fnc_cam_parse_rule(COALESCE(NULLIF(SDE.fieldname, 
														   ''), 
														SDE.fieldname_alias)) ELSE COALESCE(NULLIF(SDE.fieldname, ''), 
														SDE.fieldname_alias) END + 
														' as VARCHAR(256))) [' 
														+ SDE.fieldname_alias + ']' FormattedColumn, 
														CASE 
														  WHEN G.poidtype > 0 THEN 
														  dbo.App_fnc_cam_parse_rule(RA.rulename) 
														  ELSE G.poid 
														END                         PerfOblId, 
														mo.modelname 
														 FROM   app_cammodels M 
																JOIN app_camgroups G 
																  ON G.modelid = M.modelid 
																JOIN app_camsourcedata SD 
																  ON SD.groupid = G.groupid 
																JOIN app_camsourcedetails SDE 
																  ON SDE.sourceid = SD.sourceid 
																JOIN models mo 
																  ON mo.modelid = SD.sourcemodelid 
																LEFT JOIN app_rmabsolute RA 
																	   ON G.poid = RA.rulename 
														 WHERE  M.modelid = '{data.modelId}') 
												SELECT t2.modelname, 
													   FormattedColumns = Stuff((SELECT ',' + formattedcolumn 
																				 FROM   cte t1 
																				 WHERE  t1.modelname = t2.modelname 
																				 FOR xml path ('')), 1, 1, ''), 
													   t2.perfoblid 
												FROM   cte t2 
												GROUP  BY t2.modelname, 
														  t2.perfoblid ;"});

			var additionalColumns = context.CallActivityAsync<string>("DurableReportingLayer_ExecuteScalar",
									new
									{
										caption = $"get additional columns",
										data.connectionString,
										queryString = $@"SELECT stuff((Select DISTINCT ', [' + 	
																SDE.FieldName_Alias + ']'
														FROM App_CAMModels M
															INNER JOIN App_CAMGroups G
																ON G.ModelId = M.ModelId
															INNER JOIN App_CAMSourceData SD
																ON SD.GroupId = G.GroupId
															INNER JOIN App_CAMSourceDetails SDE
																ON SDE.SourceId = SD.SourceId
															INNER JOIN Models mo
																ON mo.ModelId = SD.SourceModelId
														WHERE M.ModelId = '{data.modelId}' for xml path ('')), 1, 1, '');"
									});

			await Task.WhenAll(sizeOfPerfOblId, sizeOfPerfOblName, modelNames, createAndInsertStrings, additionalColumns);



			var create_AppSource_Data_Rule =  context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
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

			var create_ReportingLayer_Qry = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'App_CAMAmortizationScheduleReportingFilter_{data.modelId}')
												BEGIN                                                                                
													DROP TABLE App_CAMAmortizationScheduleReportingFilter_{data.modelId};                                                                            
												END
												CREATE TABLE [dbo].[App_CAMAmortizationScheduleReportingFilter_{data.modelId}]		
												(	
													[Rownum] [int] IDENTITY(1,1) NOT NULL,
													PerfOblId Nvarchar({sizeOfPerfOblId.Result}),
													PerfOblName Nvarchar({sizeOfPerfOblName.Result}), 
													GroupName Nvarchar(256), 
													GroupId INT, 
													RuleName Nvarchar(256), 
													RuleId INT, 
													Ideal INT, 
													EntryType INT, 
													TrueUp INT, 
													{createAndInsertStrings.Result.Rows[0]["CalendarCreate"]},
													{createAndInsertStrings.Result.Rows[0]["Create"]}	
												)
												CREATE CLUSTERED INDEX IX on [dbo].[App_CAMAmortizationScheduleReportingFilter_{data.modelId}]	(Rownum) 
												CREATE NONCLUSTERED INDEX  IX_NC on [dbo].[App_CAMAmortizationScheduleReportingFilter_{data.modelId}] (";
			if (sizeOfPerfOblId.Result <= 1700)
				create_ReportingLayer_Qry += $"{Environment.NewLine} PerfOblId, ";
			if (sizeOfPerfOblName.Result <= 1700)
				create_ReportingLayer_Qry += $"{Environment.NewLine} PerfOblName, ";
			create_ReportingLayer_Qry += $@"GroupName, RuleName, Ideal, EntryType, TrueUp, {additionalColumns.Result})
											Include ({createAndInsertStrings.Result.Rows[0]["CalendarInsert"]}) ";
			var create_ReportingLayer = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
											   new
											   {
												   caption = $"drop and recreate App_CAMAmortizationScheduleReportingFilter_{data.modelId}",
												   data.connectionString,
												   queryString = create_ReportingLayer_Qry
											   });

			//context.SetCustomStatus($"drop and recreate AF_TEMP_AppSource_Data_Rule_{data.modelId} table definition.. success");

			string appSource_Data_Rule_Insert_Qry = $@"INSERT INTO AF_TEMP_AppSource_Data_Rule_{data.modelId}
													   SELECT DISTINCT * FROM (";
			string join_Qry = " LEFT JOIN (";

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
				join_Qry += $@" SELECT DISTINCT {r["PerfOblId"]} PerfOblId, {r["FormattedColumns"]}
								FROM App_CAM_Trans_Data_{data.modelId}_{r["ModelName"]}
								JOIN AF_TEMP_App_CAMSchedule_Closed_Ideal_{data.modelId} i ON i._POBId = {r["PerfOblId"]}
								GROUP BY {r["PerfOblId"]} ";
				if (modelNames.Result.Rows.IndexOf(r) != modelNames.Result.Rows.Count && modelNames.Result.Rows.Count > 1)
				{
					appSource_Data_Rule_Insert_Qry += $"{Environment.NewLine} UNION ALL";
					join_Qry += $"{Environment.NewLine} UNION ALL";
				}
			}
			join_Qry += $"{Environment.NewLine} )y ON x.PerfOblId = y.PerfOblId";
			appSource_Data_Rule_Insert_Qry += $"{Environment.NewLine} ) t";

			await Task.WhenAll(create_AppSource_Data_Rule, create_ReportingLayer);

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

			var load_AppSource_Data_Rule = context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
												new
												{
													caption = $"load AF_TEMP_AppSource_Data_Rule_{data.modelId}",
													data.connectionString,
													queryString = appSource_Data_Rule_Insert_Qry
												});

			await Task.WhenAll(schedule_Closed_Ideal, load_AppSource_Data_Rule);
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
													caption = $"load [App_CAMAmortizationScheduleReportingFilter_{data.modelId}]",
													data.connectionString,
													queryString = $@"	INSERT INTO App_CAMAmortizationScheduleReportingFilter_{data.modelId} 
																		( 
																			PerfOblId,
																			PerfOblName ,
																			GroupId, 
																			GroupName, 
																			RuleName, 
																			RuleId, 
																			Ideal , 
																			EntryType, 
																			TrueUp, 
																			{createAndInsertStrings.Result.Rows[0]["CalendarInsert"]},
																			{createAndInsertStrings.Result.Rows[0]["Insert"]}
																		)
																		SELECT 
																		x.PerfOblId,
																		PerfOblName, 
																		GroupId, 
																		GroupName, 
																		RuleName, 
																		RuleId,
																		Ideal, 
																		EntryType,
																		TrueUp,
																		{createAndInsertStrings.Result.Rows[0]["SumCalendar"]},
																		{createAndInsertStrings.Result.Rows[0]["Insert"]}
																		FROM
																		(
																			SELECT
																				PerfOblId, 
																				PerfOblName,GroupId, GroupName, RuleName, RuleId, 
																				Ideal, 
																				EntryType,
																				TrueUp, {createAndInsertStrings.Result.Rows[0]["SumCalendar"]}
																			FROM
																				(
																			Select * from AF_TEMP_App_CAMSchedule_{data.modelId}
																				UNION ALL
																			Select * from AF_TEMP_App_CAMSchedule_Closed_{data.modelId}
																				UNION ALL
																			Select * from AF_TEMP_App_CAMSchedule_Ideal_{data.modelId}
																				) a
																			PIVOT
																			(
																				SUM(Amount) FOR TimeName IN ({createAndInsertStrings.Result.Rows[0]["CalendarInsert"]})
																			) p
																			GROUP BY PerfOblId, PerfOblName,GroupId, GroupName, RuleName, RuleId, Ideal, TrueUp, EntryType
																			) x	
																		{join_Qry}
																		GROUP BY  x.PerfOblId,PerfOblName, GroupId, GroupName, RuleName, RuleId,
																		Ideal , EntryType ,TrueUp, {createAndInsertStrings.Result.Rows[0]["Insert"]}
																		ORDER BY x.PerfOblId,PerfOblName	
																		"
												});

			await context.CallActivityAsync<int>("DurableReportingLayer_ExecuteQuery",
												new
												{
													caption = $"drop and recreate App_CAMAmortizationScheduleReportingFilterColumns_{data.modelId}",
													data.connectionString,
													queryString = $@" IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'App_CAMAmortizationScheduleReportingFilterColumns_{data.modelId}')
																		BEGIN                                                                                
																			DROP TABLE App_CAMAmortizationScheduleReportingFilterColumns_{data.modelId};                                                                            
																		END
																		CREATE TABLE App_CAMAmortizationScheduleReportingFilterColumns_{data.modelId}
																		(
																			ColumnName nvarchar(256),
																			ColumnValues nvarchar(Max)
																		)

																		Insert into [dbo].[App_CAMAmortizationScheduleReportingFilterColumns_{data.modelId}]
																		VALUES ('Portfolio', Stuff((Select distinct ',' + GroupName from [App_CAMAmortizationScheduleReportingFilter_{data.modelId}] for xml path ('')), 1, 1, '')),
																				('Treatment', Stuff((Select distinct ',' + RuleName from [App_CAMAmortizationScheduleReportingFilter_{data.modelId}]	for xml path ('')), 1, 1, '')),
																				('EntryType', 
																				Stuff((Select distinct ',' +  case when Ideal = 0 then '' else 'Non-Adj ' end + 
																						case when EntryType = 1 then 'Expense' else 'Capitalization' end +
																						case when TrueUp = 1 then ' True Up' else '' end  
																								from [App_CAMAmortizationScheduleReportingFilter_{data.modelId}] for xml path ('')), 1, 1, ''))"
												});

			return 1 ;
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