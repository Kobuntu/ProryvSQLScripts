

--переименоввываем.. в usp2_Import_InegralValues
IF OBJECT_ID('usp2_ImportedInegralValues', 'P') IS NOT NULL
    DROP PROCEDURE usp2_ImportedInegralValues;
GO


IF OBJECT_ID('usp2_Import_InegralValues', 'P') IS NOT NULL
    DROP PROCEDURE usp2_Import_InegralValues;
GO


if(exists (select top 1 1 from systypes where name like 'ImportedInegralValueTableType'))
	DROP Type ImportedInegralValueTableType
GO

CREATE TYPE ImportedInegralValueTableType AS TABLE 
(
	RowNumber int  not null,
	ResultStatus bigint  not null,
	Code nvarchar(200) NULL,
	Meter_ID [dbo].[Meter_ID_TYPE]  NULL,
	[TI_ID] [dbo].[TI_ID_TYPE]  NULL,
	[EventDateTime] [datetime]  NULL,
	[ChannelType] [dbo].[TI_CHANNEL_TYPE]  NULL,	
	[DataSourceType] [tinyint]  NULL,
	Data float NULL
)
GO

grant EXECUTE on TYPE::ImportedInegralValueTableType to [UserCalcService]
go
grant EXECUTE on TYPE::ImportedInegralValueTableType to [UserDeclarator]
go
grant EXECUTE on TYPE::ImportedInegralValueTableType to [UserImportService]
go
grant EXECUTE on TYPE::ImportedInegralValueTableType to [UserExportService]
go


CREATE PROCEDURE usp2_Import_InegralValues
@UserID nvarchar(200),
@ImportedTable ImportedInegralValueTableType readonly
AS 
BEGIN

--сюда данные передаются в Вт/Вар

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
set dateformat dmy

declare @ImportedTable_temp ImportedInegralValueTableType 

insert into
@ImportedTable_temp
	(RowNumber ,
	ResultStatus ,
	Code ,
	Meter_ID ,
	[TI_ID] ,
	[EventDateTime] ,
	[ChannelType] ,
	[DataSourceType] ,
	Data 
	)
select distinct temp.RowNumber ,
	temp.ResultStatus ,
	temp.Code ,
	temp.Meter_ID ,
	temp.[TI_ID] ,
	temp.[EventDateTime] ,
	[ChannelType] = case when  temp.[ChannelType]  is not null 
										and ( isnull(AIATSCode,1)=1 or 	isnull(AOATSCode,2)=2 or  isnull(RIATSCode,3)=3 or isnull(ROATSCode,4) =4) 
									then temp.[ChannelType]
								  else
									case 
										when temp.[ChannelType] =1 then 2
										when temp.[ChannelType] =2 then 1
										when temp.[ChannelType] =3 then 4
										when temp.[ChannelType] =4 then 3
										when temp.[ChannelType] =11 then 12
										when temp.[ChannelType] =12 then 11
										when temp.[ChannelType] =13 then 14
										when temp.[ChannelType] =14 then 13
										when temp.[ChannelType] =21 then 22
										when temp.[ChannelType] =22 then 21
										when temp.[ChannelType] =23 then 24
										when temp.[ChannelType] =24 then 23
										when temp.[ChannelType] =31 then 32
										when temp.[ChannelType] =32 then 31
										when temp.[ChannelType] =33 then 34
										when temp.[ChannelType] =34 then 33
										when temp.[ChannelType] =41 then 42
										when temp.[ChannelType] =42 then 41
										when temp.[ChannelType] =43 then 44
										when temp.[ChannelType] =44 then 43
										when temp.[ChannelType] =51 then 52
										when temp.[ChannelType] =52 then 51
										when temp.[ChannelType] =53 then 54
										when temp.[ChannelType] =54 then 53
										when temp.[ChannelType] =61 then 62
										when temp.[ChannelType] =62 then 61
										when temp.[ChannelType] =63 then 64
										when temp.[ChannelType] =64 then 63
										when temp.[ChannelType] =71 then 72
										when temp.[ChannelType] =72 then 71
										when temp.[ChannelType] =73 then 74
										when temp.[ChannelType] =74 then 73
										when temp.[ChannelType] =81 then 82
										when temp.[ChannelType] =82 then 81
										when temp.[ChannelType] =83 then 84
										when temp.[ChannelType] =84 then 83
										when temp.[ChannelType] =91 then 92
										when temp.[ChannelType] =92 then 91
										when temp.[ChannelType] =93 then 94
										when temp.[ChannelType] =94 then 93
										else null end
								  end,
	temp.[DataSourceType] ,
	temp.Data 
from @ImportedTable temp
left join Hard_Meters 
	on Hard_Meters.MeterSerialNumber = temp.Code 
		and Hard_Meters.MeterSerialNumber is not  null
		and Hard_Meters.MeterSerialNumber <>''
--надо проверить как будет если кривые периоды 
left join Info_Meters_TO_TI 
	on Hard_Meters.Meter_ID = Info_Meters_TO_TI.Meter_ID
		and Info_Meters_TO_TI.StartDateTime<=temp.[EventDateTime] 
	and isnull(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=temp.[EventDateTime] 
left join Info_TI ti on ti.TI_ID = Info_Meters_TO_TI.TI_ID


declare @tempresult table 
(
RowNumber int  not null,
ResultStatus bigint  not null,
Code nvarchar(200) NULL,
Meter_ID [dbo].[Meter_ID_TYPE]  NULL,
[TI_ID] [dbo].[TI_ID_TYPE]  NULL,
[TIType] tinyint  NULL,
IsTIReverce bit null,
[EventDateTime] [datetime]  NULL,
[ChannelType] [dbo].[TI_CHANNEL_TYPE]  NULL,	
[DataSourceType] tinyint  NULL,
DataSource_ID int  NULL,
Data float NULL,
ExistsIntegralsData float NULL,

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
temp.RowNumber,
temp.ResultStatus,	
temp.Code,
Hard_Meters.Meter_ID, 
Info_Meters_TO_TI.[TI_ID],
ti.[TIType],
IsTIReverce= case when isnull(AIATSCode,1)=1 or 	isnull(AOATSCode,2)=2 or  isnull(RIATSCode,3)=3 or isnull(ROATSCode,4) =4 then 0 else 1 end,
temp.[EventDateTime], 
temp.[ChannelType], 
temp.[DataSourceType],
Expl_DataSource_List.DataSource_ID,
Data=Data,
ArchCalcBit_Integrals.ExistsIntegralsData,

MeterNotDefined = (case when Hard_Meters.Meter_ID is null then 1 else 0 end),
DateNotDefined = (case when temp.[EventDateTime] is null then 1 else 0 end),
TimeNotDefined = (case when temp.[EventDateTime] is null then 1 else 0 end),
ChannelTypeNotDefined = (case when temp.[ChannelType] is null then 1 else 0 end),
DataSourceNotDefined = (case when temp.[DataSourceType] is null then 1 else 0 end),
DataNotDefined = (case when temp.Data is null or temp.Data<0 then 1 else 0 end),
TINotDefined = (case when Info_Meters_TO_TI.[TI_ID] is null  then 1 else 0 end),

DupplicateDateTimeExists=0,
DataHaveBeenUpdated =0, 
DataHaveBeenInserted =0


from @ImportedTable_temp temp
left join Hard_Meters 
	on Hard_Meters.MeterSerialNumber = temp.Code 
		and Hard_Meters.MeterSerialNumber is not  null
		and Hard_Meters.MeterSerialNumber <>''
--надо проверить как будет если кривые периоды 
left join Info_Meters_TO_TI 
	on Hard_Meters.Meter_ID = Info_Meters_TO_TI.Meter_ID
		and Info_Meters_TO_TI.StartDateTime<=temp.[EventDateTime] 
	and isnull(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=temp.[EventDateTime] 
left join Info_TI ti on ti.TI_ID = Info_Meters_TO_TI.TI_ID
left join Expl_DataSource_List on Expl_DataSource_List.DataSourceType= temp.DataSourceType
outer apply 
(
	--isnull(Integrals_Virtual.ManualEnterData,Integrals_Virtual.Data) 
	select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_1 Integrals_Virtual  
	where 
		ti.TIType=11
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
	select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_2 Integrals_Virtual  
	where		
		ti.TIType=12
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_3 Integrals_Virtual  
	where 				
		ti.TIType=13
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		 ArchCalcBit_Integrals_Virtual_4 Integrals_Virtual  
	where 		 				
		ti.TIType=14
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_5 Integrals_Virtual
	where 
		ti.TIType=15
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_6 Integrals_Virtual 
	where 
		ti.TIType=16
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_7 Integrals_Virtual  
	where 
		ti.TIType=17
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_8 Integrals_Virtual 
	where 
		ti.TIType=18
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_9 Integrals_Virtual 
	where 
		ti.TIType=19
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
		ArchCalcBit_Integrals_Virtual_10 Integrals_Virtual 
	where 
		ti.TIType=20
		and	Integrals_Virtual.TI_ID= ti.TI_ID 
		and Integrals_Virtual.EventDateTime=temp.EventDateTime
		and Integrals_Virtual.ChannelType=temp.ChannelType
		and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_11 Integrals_Virtual 
	where 
	ti.TIType=21
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_12 Integrals_Virtual  
	where 
	ti.TIType=22
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_13 Integrals_Virtual  
	where 
	ti.TIType=23
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_14 Integrals_Virtual  
	where 
	ti.TIType=24
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_15 Integrals_Virtual  
	where 
	ti.TIType=25
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_16 Integrals_Virtual  
	where 
	ti.TIType=26
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_17 Integrals_Virtual  
	where 
	ti.TIType=27
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_18 Integrals_Virtual 
	where 
	ti.TIType=28
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_19 Integrals_Virtual  
	where 
	ti.TIType=29
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalcBit_Integrals_Virtual_20 Integrals_Virtual  
	where 
	ti.TIType=30
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID
union
--остальные
select top 1 ExistsIntegralsData = Integrals_Virtual.Data 
	from
	ArchCalc_Integrals_Virtual Integrals_Virtual  
	where 
	ti.TIType<11
	and	Integrals_Virtual.TI_ID= ti.TI_ID 
	and Integrals_Virtual.EventDateTime=temp.EventDateTime
	and Integrals_Virtual.ChannelType=temp.ChannelType
	and Integrals_Virtual.DataSource_ID=Expl_DataSource_List.DataSource_ID		 
)
ArchCalcBit_Integrals

--обновляем общий статус
update @tempresult set ResultStatus=
	MeterNotDefined*POWER(2,0)
	|DateNotDefined*POWER(2,1)
	|TimeNotDefined*POWER(2,2)
	|ChannelTypeNotDefined*POWER(2,3)
	|DataSourceNotDefined*POWER(2,4)
	|DataNotDefined*POWER(2,5)
	|TINotDefined*POWER(2,6)

update @tempresult set DupplicateDateTimeExists= 1 , ResultStatus=ResultStatus|1*Power(2,7)
--select * 
from @tempresult temp1
where 
exists
 (select * from @tempresult temp2 
where 
--все нужные даныне есть
temp1.RowNumber is not null
and temp2.RowNumber is not null
and isnull(temp1.ResultStatus,1)=0
and isnull(temp2.ResultStatus,1)=0
--и номера строк разные
and temp1.RowNumber<>temp2.RowNumber
and temp1.TI_ID=temp2.TI_ID
and temp1.EventDateTime=temp2.EventDateTime
and temp1.ChannelType=temp2.ChannelType
and temp1.DataSource_ID= temp2.DataSource_ID
)

--добавлять/обновлять будем данные только со статусом 0 

declare @sql nvarchar(max)



--merge делаем только на строки без ошибок (статус = 0)
merge into 
ArchCalcBit_Integrals_Virtual_1 
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=11)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_2
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=12)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_3
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=13)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_4
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=14)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_5
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=15)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_6
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=16)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_7
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=17)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_8
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=18)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_9
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=19)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_10
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=20)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_11
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=21)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_12
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=22)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_13
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=23)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_14
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=24)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_15
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=25)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);


merge into 
ArchCalcBit_Integrals_Virtual_16
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=26)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_17
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=27)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

	
merge into 
ArchCalcBit_Integrals_Virtual_18
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=28)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_19
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=29)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);

merge into 
ArchCalcBit_Integrals_Virtual_20
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType=30)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0);


	merge into 
ArchCalc_Integrals_Virtual
as Target 
	using (select * from @tempresult where ResultStatus =0 and TI_ID is not null and TIType<11)
as Source 
	on Target.TI_ID= Source.TI_ID and Target.EventDateTime= Source.EventDateTime and Target.ChannelType= Source.ChannelType and Target.DataSource_ID= Source.DataSource_ID
WHEN MATCHED THEN
	UPDATE set Data= Source.Data, DispatchDateTime = getdate()
WHEN NOT MATCHED BY TARGET THEN
	insert (TI_ID	,EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData	,IntegralType,	DispatchDateTime,	Status,	IsUsedForFillHalfHours,	CUS_ID)
	values (Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSource_ID,	Source.Data, NULL,	0,	 getdate(),0, 	NULL,	0);
	

--в конце проставляем статусы строкам, которые обрабатывались: изменили или добавили
update @tempresult set DataHaveBeenUpdated= 1 , ResultStatus=ResultStatus|1*Power(2,8)
from @tempresult
where ResultStatus=0 and ExistsIntegralsData is not null

update @tempresult set DataHaveBeenInserted= 1 , ResultStatus=ResultStatus|1*Power(2,9)
from @tempresult
where ResultStatus=0 and ExistsIntegralsData is null

declare @result  ImportedInegralValueTableType 

insert into @result 
	(RowNumber,	ResultStatus,	Code,	
	Meter_ID,	TI_ID,	EventDateTime,	ChannelType,	
	DataSourceType,	Data)
select distinct 
	RowNumber,	ResultStatus,	Code,	
	Meter_ID,	TI_ID,	EventDateTime,	ChannelType,	
	DataSourceType,	Data	
from @tempresult
order by RowNumber

select * from @result

--select * from @tempresult
--здесь можно будет потом в журнал записать информацию об изменениях
--можно сгруппировать по датам  и написать для такой то ТИ менялись/доабвлялись данные за период...

--на выходе нормальными будут считаться статусы 256 и 512 (добавлено или обновлено без ошибок)

END
GO

grant EXECUTE on usp2_Import_InegralValues to [UserCalcService]
go
grant EXECUTE on usp2_Import_InegralValues to [UserDeclarator]
go
grant EXECUTE on usp2_Import_InegralValues to [UserImportService]
go
grant EXECUTE on usp2_Import_InegralValues to [UserExportService]
go

