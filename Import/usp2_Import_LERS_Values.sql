--переименоввываем.. в usp2_Import_InegralValues
IF OBJECT_ID('usp2_Import_LERS_Values', 'P') IS NOT NULL
    DROP PROCEDURE usp2_Import_LERS_Values;
GO



if(exists (select top 1 1 from systypes where name like 'ImportedValueTableType_LERS'))
	DROP Type ImportedValueTableType_LERS
GO

CREATE TYPE ImportedValueTableType_LERS AS TABLE 
( 
	Code nvarchar(200) NULL,
	UAServer_ID [dbo].UASERVER_ID_TYPE  NULL,
	UANodeID [dbo].UAEXPANDEDNODEID_TYPE  NULL,
	[EventDateTime] smallDatetime  NULL, 	
	Data float NULL
)
GO

 

grant EXECUTE on TYPE::ImportedValueTableType_LERS to [UserCalcService]
go
grant EXECUTE on TYPE::ImportedValueTableType_LERS to [UserDeclarator]
go
grant EXECUTE on TYPE::ImportedValueTableType_LERS to [UserImportService]
go
grant EXECUTE on TYPE::ImportedValueTableType_LERS to [UserExportService]
go


CREATE PROCEDURE usp2_Import_LERS_Values
@UserID nvarchar(200),
@ImportedTable ImportedValueTableType_LERS readonly
AS 
BEGIN

--считаем что данные передаются в местном времени а хранятся в UTC, поэтмоу приводим к UTC
declare @HourOffcet int
select @HourOffcet=datediff(hour,getdate(),GETUTCDATE())

--сюда данные передаются в Вт/Вар



set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
set dateformat dmy

declare @ImportedTable_temp ImportedValueTableType_LERS 

insert into @ImportedTable_temp
	( 
	Code ,
	UAServer_ID, 
	UANodeID , 
	[EventDateTime] , 
	Data  
	)
select distinct  
	Code ,
	temp.UAServer_ID, 
	temp.UANodeID , 
	[EventDateTime]= case when temp.[EventDateTime] is null then null else dateadd(hour, @HourOffcet,temp.[EventDateTime])end ,
 	Data  
from @ImportedTable temp
left join UA_Nodes on UA_nodes.UAServer_ID= temp.UAServer_ID and UA_Nodes.UANodeID= temp.UANodeID


declare @tempresult table 
( 
ResultStatus bigint,
UAServer_ID UASERVER_ID_TYPE,
UANode_ID UANODE_ID_TYPE,
UANodeID UAEXPANDEDNODEID_TYPE,

SourceTimeStamp [datetime]  NULL,
Data float NULL,
ExistsData float NULL, 
Code nvarchar(200) NULL,

MeterNotDefined  bit,
DateNotDefined bit,
TimeNotDefined bit,
ChannelTypeNotDefined bit,
DataSourceNotDefined bit,
DataNotDefined bit,
TINotDefined bit ,
DupplicateDateTimeExists bit,
DataHaveBeenUpdated bit, 
DataHaveBeenInserted bit)

insert into @tempresult
select distinct  
ResultStatus=0,
UAServer_ID =UA_Nodes.UAServer_ID,
UANode_ID= UA_Nodes.UANode_ID,
UANodeID= UA_Nodes.UANodeID,

temp.[EventDateTime] , 
Data=Data,
ArchCalc.ExistsData, 
temp.Code,

--статусы пока оставялем такими же
MeterNotDefined = 0,
DateNotDefined = (case when temp.[EventDateTime] is null then 1 else 0 end),
TimeNotDefined = (case when temp.[EventDateTime] is null then 1 else 0 end),
ChannelTypeNotDefined = 0,
DataSourceNotDefined = 0,
DataNotDefined = (case when temp.Data is null or temp.Data<0 then 1 else 0 end),
TINotDefined = (case when   UA_Nodes.UANodeID is null then 1 else 0 end),
DupplicateDateTimeExists=0,
DataHaveBeenUpdated =0, 
DataHaveBeenInserted =0
from @ImportedTable_temp temp  
left join UA_Nodes 
		on UA_nodes.UAServer_ID= temp.UAServer_ID and UA_Nodes.UANodeID= temp.UANodeID
outer apply 
(
	
	select top 1 ExistsData = archVirtual.Value 
	from
		UA_Data_Archive_Float_1 archVirtual  
	where 
		archVirtual.UANode_ID= UA_Nodes.UANode_ID 
		and archVirtual.SourceTimeStamp=temp.[EventDateTime]
 
)
ArchCalc

--обновляем общий статус
update @tempresult set ResultStatus=
	MeterNotDefined*POWER(2,0)
	|DateNotDefined*POWER(2,1)
	|TimeNotDefined*POWER(2,2)
	|ChannelTypeNotDefined*POWER(2,3)
	|DataSourceNotDefined*POWER(2,4)
	|DataNotDefined*POWER(2,5)
	|TINotDefined*POWER(2,6)

 
--добавлять/обновлять будем данные только со статусом 0 

declare @sql nvarchar(max)



--merge делаем только на строки без ошибок (статус = 0)
merge into 
UA_Data_Archive_Float_1 
as Target 
	using (select * from @tempresult where ResultStatus =0 and UANode_ID is not null)
as Source 
	on Target.UANode_ID= Source.UANode_ID and Target.SourceTimeStamp= Source.SourceTimeStamp
WHEN MATCHED  and  isnull(Value,0) <>isnull(Source.Data,0) THEN
	UPDATE set Value= Source.Data, DispatchDateTime = getUTCdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (UANode_ID,
			SourceTimeStamp,
			ServerTimeStamp,
			SourcePicoseconds,ServerPicoseconds,
			Value,
			ArrayFormat,
			ArrayValue,
			StatusCode,
			DispatchDateTime,
			ConfirmDateTime,
			ConfirmUser_ID,
			ConfirmCode
			)
	values (Source.UANode_ID,	Source.SourceTimeStamp,	Source.SourceTimeStamp,
	0,	0,		
	Source.Data,	NULL,	NULL,	0,	getUTCdate(),	NULL,	NULL,	NULL);


--в конце проставляем статусы строкам, которые обрабатывались: изменили или добавили
update @tempresult set DataHaveBeenUpdated= 1 , ResultStatus=ResultStatus|1*Power(2,8)
from @tempresult
where ResultStatus=0 and ExistsData is not null

update @tempresult set DataHaveBeenInserted= 1 , ResultStatus=ResultStatus|1*Power(2,9)
from @tempresult
where ResultStatus=0 and ExistsData is null

declare @result  ImportedValueTableType_LERS 

insert into @result 
	( 
	Code, UANodeID	, EventDateTime, Data)
select distinct 
	 	
	Code,	  UANodeID,SourceTimeStamp,Data
from @tempresult
order by Code

select * from @result

--здесь можно писать в таблицу с датами время изменения (если были изменения)
 
END
GO

grant EXECUTE on usp2_Import_LERS_Values to [UserCalcService]
go
grant EXECUTE on usp2_Import_LERS_Values to [UserDeclarator]
go
grant EXECUTE on usp2_Import_LERS_Values to [UserImportService]
go
grant EXECUTE on usp2_Import_LERS_Values to [UserExportService]
go
