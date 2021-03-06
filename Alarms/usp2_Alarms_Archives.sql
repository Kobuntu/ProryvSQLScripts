if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Alarms_Archives')
          and type in ('P','PC'))
   drop procedure usp2_Alarms_Archives
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2018
--
-- Описание:
--
--		Выборка аварий текущих или архивных
--
-- ======================================================================================
create proc [dbo].[usp2_Alarms_Archives]

	@User_ID varchar(22),
	@StartDateTime datetime,
	@FinishDateTime datetime,
	@Confirmed bit,
	@tis varchar(max) = null
as

begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Фильтр по ТИ

declare @isTiFilter bit;

select Items as TI_ID
into #tis
from dbo.usf2_Utils_Split(@tis, ',')

if (@tis is not null AND len(@tis) > 0) begin
	if exists(select top 1 1 from #tis) set @isTiFilter = 1;
	else set @isTiFilter = 0;
end else set @isTiFilter = 0;

SELECT 

--Alarms_Archive
[t1].[Alarm_ID], [t1].[EventDateTime], 
[t1].[AlarmSeverity], [t1].[AlarmMessage], [t1].[AlarmDescription], [t1].[AlarmMessageShort],  
[t1].[AlarmDateTime], 

[t2].[StringName] AS [WorkFlowActivityName], [t3].[StringName] AS [SettingName], [t4].[UserFullName] AS [UserName], 
[t5].[TI_ID] AS [TI_ID], [t10].[Slave61968System_ID] AS [Slave61968System_ID], [t6].[Formula_UN] AS [Formula_UN], [t7].[BalancePS_UN] AS [BalancePS_UN], 
[t8].[PS_ID] AS [Balance_PS_ID], [t9].[PS_ID] AS [PS_ID], [t11].[StringName] AS [Master61968_Name], [t12].[BalanceFreeHierarchy_UN] AS [BalanceFreeHierarchy_UN]

--Info_Balance_FreeHierarchy_Objects
,[t15].[FreeHierItem_ID] as FreeHierarchyObjects_FreeHierItem_ID
,[t15].[HierLev1_ID] as FreeHierarchyObjects_HierLev1_ID
,[t15].[HierLev2_ID] as FreeHierarchyObjects_HierLev2_ID
,[t15].[HierLev3_ID] as FreeHierarchyObjects_HierLev3_ID
,[t15].[PS_ID] AS FreeHierarchyObjects_PS_ID

--Alarms_Archive_Confirm_Status
, [t16].AlarmConfirmStatusCategory_ID
, [t16].Comment

FROM [dbo].[Alarms_Archive] AS [t1]
--INNER JOIN [dbo].[Alarms_Current] AS [t0] ON [t0].[Alarm_ID] = [t1].[Alarm_ID]
INNER JOIN [dbo].[Workflow_Activity_List] AS [t2] ON [t1].[WorkflowActivity_ID] = [t2].[WorkflowActivity_ID]
INNER JOIN [dbo].[Alarms_Settings] AS [t3] ON [t1].[AlarmSetting_ID] = [t3].[AlarmSetting_ID]
INNER JOIN [dbo].[Expl_Users] AS [t4] ON [t1].[User_ID] = [t4].[User_ID]
LEFT OUTER JOIN [dbo].[Alarms_Archive_To_TI] AS [t5] ON [t1].[Alarm_ID] = [t5].[Alarm_ID]
LEFT OUTER JOIN [dbo].[Alarms_Archive_To_Formula] AS [t6] ON [t1].[Alarm_ID] = [t6].[Alarm_ID]
LEFT OUTER JOIN [dbo].[Alarms_Archive_To_Balance_PS] AS [t7] ON [t1].[Alarm_ID] = [t7].[Alarm_ID]
LEFT OUTER JOIN [dbo].[Info_Balance_PS_List_2] AS [t8] ON [t7].[BalancePS_UN] = [t8].[BalancePS_UN]
LEFT OUTER JOIN [dbo].[Alarms_Archive_To_PS] AS [t9] ON [t1].[Alarm_ID] = [t9].[Alarm_ID]
LEFT OUTER JOIN [dbo].[Alarms_Archive_To_Master61968_SlaveSystems] AS [t10] ON [t1].[Alarm_ID] = [t10].[Alarm_ID]
LEFT OUTER JOIN [dbo].[Master61968_SlaveSystems] AS [t11] ON [t10].[Slave61968System_ID] = [t11].[Slave61968System_ID]
LEFT OUTER JOIN [dbo].[Alarms_Archive_To_Balance_FreeHierarchy] AS [t12] ON [t1].[Alarm_ID] = [t12].[Alarm_ID]
LEFT OUTER JOIN [dbo].[Info_Balance_FreeHierarchy_List] AS [t13] ON [t12].[BalanceFreeHierarchy_UN] = [t13].[BalanceFreeHierarchy_UN]
LEFT OUTER JOIN [dbo].[Info_Balance_FreeHierarchy_Objects] AS [t15] ON [t13].[BalanceFreeHierarchyObject_UN] = [t15].[BalanceFreeHierarchyObject_UN]
outer apply 
(
	select top 1 s.AlarmConfirmStatusCategory_ID, s.Comment from [dbo].[Alarms_Archive_Confirm_Status] s 
	where s.Alarm_ID = [t1].Alarm_ID 
	order by [ConfirmStatusDateTime] desc
)  [t16]

--LEFT OUTER JOIN (
--    SELECT 1 AS [test], [t14].[BalanceFreeHierarchyObject_UN], [t14].[FreeHierItem_ID], [t14].[HierLev1_ID], [t14].[HierLev2_ID], [t14].[HierLev3_ID], [t14].[PS_ID]
--    FROM [dbo].[Info_Balance_FreeHierarchy_Objects] AS [t14]
--    ) AS [t15] ON [t13].[BalanceFreeHierarchyObject_UN] = [t15].[BalanceFreeHierarchyObject_UN]
WHERE (@User_ID is null OR len(@User_ID)=0 OR [t1].[User_ID] = @User_ID) AND Confirmed = @Confirmed 
	AND ([t1].[EventDateTime] between @StartDateTime and @FinishDateTime)
	AND (@isTiFilter = 0 OR ([t5].TI_ID) in (select TI_ID from #tis))
ORDER BY [t1].[EventDateTime] DESC

drop table #tis

end
go
   grant EXECUTE on usp2_Alarms_Archives to [UserCalcService]
go