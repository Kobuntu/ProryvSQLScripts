if exists (select 1
          from sysobjects
          where  id = object_id('usp2_XMLExportConfigContent_Merge')
          and type in ('P','PC'))
   drop procedure usp2_XMLExportConfigContent_Merge
go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'Expl_XML_Export_ConfigContent_TableType' AND ss.name = N'dbo')
   drop TYPE [Expl_XML_Export_ConfigContent_TableType]
go

Create TYPE [dbo].[Expl_XML_Export_ConfigContent_TableType] 
AS TABLE
(
ActionName varchar(200),
XMLExportConfig_ID XMLEXPORTCONFIG_ID_TYPE not null,
ATSArea_ID ATS_AREA_ID_TYPE not null,
NewATSArea_ID ATS_AREA_ID_TYPE not null,
TI_ID	TI_ID_TYPE  null,
Section_ID SECTION_ID_TYPE null,
ATSCode nvarchar(100) not null,
DataSourceType	tinyint,
AIAllowExport bit not null,
AICode nvarchar(10) not null,
AOAllowExport bit not null,
AOCode nvarchar(10) not null,
RIAllowExport bit not null,
RICode nvarchar(10) not null,
ROAllowExport bit not null,
ROCode nvarchar(10) not null,
UseAlternativeAlgorithmVersion bit not null,
AlternativeObjectName nvarchar(400)  null
)
GO

grant EXECUTE on TYPE::Expl_XML_Export_ConfigContent_TableType to [UserCalcService]
go
grant EXECUTE on TYPE::Expl_XML_Export_ConfigContent_TableType to [UserDeclarator]
go
grant EXECUTE on TYPE::Expl_XML_Export_ConfigContent_TableType to [UserExportService]
go


CREATE PROCEDURE [dbo].usp2_XMLExportConfigContent_Merge
				 @User_ID varchar(200), 
				 @InsertTIAsExists bit,
				 @IsInsert bit,
				 @SourceTable [Expl_XML_Export_ConfigContent_TableType] READONLY
AS BEGIN
SET NOCOUNT ON
SET QUOTED_IDENTIFIER, ANSI_NULLS, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, ANSI_PADDING ON
SET NUMERIC_ROUNDABORT OFF
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
  


declare  @startDate datetime
set @startDate=GETDATE()


--не будем добавлять ТИ с настройками по умолчанию, в которых не указана Area
--КОД АТС есть там где указана АРеа..



--Sourcetable
declare @MergeSourceTable [Expl_XML_Export_ConfigContent_TableType]
insert into @MergeSourceTable
select distinct 
ActionType='',
XMLExportConfig_ID=temp.XMLExportConfig_ID, 

ATSArea_ID =isnull(
			case 
				when @InsertTIAsExists=0 and @IsInsert=1  then isnull(temp.NewATSArea_ID, '')		-- при добавлении всегда NewATSArea_ID
				when @InsertTIAsExists=1 and @IsInsert=1  then 			-- при добавлении по умолч - берем из ТИ/ТП/Сечения или новую если не указано
						case
							 when  Info_TI.TI_ID is not null then  isnull(Info_TI.ATSArea_ID, '')
							 when  Info_Section_List.Section_ID is not null then  isnull(Info_Section_List.ATSArea_ID, '')
						else  '' end						
				when @InsertTIAsExists=0 and @IsInsert=0  then isnull(temp.ATSArea_ID, '')			--при изменении ATSArea_ID берем
			else temp.ATSArea_ID end, ''),
			
NewATSArea_ID = isnull(
			case 
				when @InsertTIAsExists=0 and @IsInsert=1  then isnull(temp.NewATSArea_ID, '')		-- при добавлении всегда NewATSArea_ID
				when @InsertTIAsExists=1 and @IsInsert=1  then -- при добавлении по умолч - берем из ТИ или новую если в ТИ нет
						case
							 when  Info_TI.TI_ID is not null then  isnull(Info_TI.ATSArea_ID, '')
							 when  Info_Section_List.Section_ID is not null then  isnull(Info_Section_List.ATSArea_ID, '')
						else  '' end
				when @InsertTIAsExists=0 and @IsInsert=0  then isnull(temp.NewATSArea_ID, '')		--при изменении NewATSArea_ID берем
			else temp.NewATSArea_ID end, ''),
			
TI_ID=temp.TI_ID,
Section_ID=temp.Section_ID,

ATSCode=isnull(case when @InsertTIAsExists=1 then 
						case
							 when  Info_TI.TI_ID is not null then  isnull(Info_TI.TIATSCode, '') 
							 when  Info_Section_List.Section_ID is not null then  isnull(Info_Section_List.ATSSectionCode, '')
						else  '' end										
					when @InsertTIAsExists=0 then 
								case 
									when isnull(temp.ATSCode ,'')='' then 
														case
															 when  Info_TI.TI_ID is not null then  isnull(Info_TI.TIATSCode, '') 
															 when  Info_Section_List.Section_ID is not null then  isnull(Info_Section_List.ATSSectionCode, '')
														else  '' end  
									else temp.ATSCode end
					else temp.ATSCode end,''),


DataSourceType		=	temp.DataSourceType,

AIAllowExport = case 
					when Info_TI.TI_ID is not null and @InsertTIAsExists=1 and Info_TI.XMLAIATSCode is null then 0
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLAIATSCode is not null  then 1
					when @InsertTIAsExists=0 then temp.AIAllowExport
					else temp.AIAllowExport end,			
AICode  = isnull(case 
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1  then Info_TI.XMLAIATSCode					
					when @InsertTIAsExists=0 then temp.AICode
					else temp.AICode end,'01'),		
AOAllowExport = case 
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLAOATSCode is null then 0
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLAOATSCode is not null  then 1
					when @InsertTIAsExists=0 then temp.AOAllowExport
					else temp.AOAllowExport end,			
AOCode  = isnull(case 
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1  then Info_TI.XMLAOATSCode					
					when @InsertTIAsExists=0 then temp.AOCode
					else temp.AOCode end,'02'),	
RIAllowExport = case 
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLRIATSCode is null then 0
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLRIATSCode is not null  then 1
					when @InsertTIAsExists=0 then temp.RIAllowExport
					else temp.RIAllowExport end,			
RICode  =  isnull(case 
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1  then Info_TI.XMLRIATSCode					
					when @InsertTIAsExists=0 then temp.RICode
					else temp.RICode end,'03'),	
							
ROAllowExport = case 
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLROATSCode is null then 0
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLROATSCode is not null  then 1
					when @InsertTIAsExists=0 then temp.ROAllowExport
					else temp.ROAllowExport end,			
ROCode  =  isnull(case 
					when Info_TI.TI_ID is not null and  @InsertTIAsExists=1  then Info_TI.XMLROATSCode					
					when @InsertTIAsExists=0 then temp.ROCode
					else temp.ROCode end	,'04'),
UseAlternativeAlgorithmVersion=temp.UseAlternativeAlgorithmVersion	,
AlternativeObjectName =temp.AlternativeObjectName
														 
from @SourceTable temp 
	Outer apply (select top 1 * from  Info_TI where temp.TI_ID=Info_TI.TI_ID) as Info_TI
	Outer apply (select top 1 * from  Info_Section_List where temp.Section_ID=Info_Section_List.Section_ID) as Info_Section_List



--альтернативные
if @InsertTIAsExists=1
begin

	insert into @MergeSourceTable

	select res.* 
	from
	(
	select distinct 
	ActionType='',
	XMLExportConfig_ID=temp.XMLExportConfig_ID, 

	ATSArea_ID =isnull(
				case 
					when @InsertTIAsExists=0 and @IsInsert=1  then isnull(temp.NewATSArea_ID, '')		-- при добавлении всегда NewATSArea_ID
					when @InsertTIAsExists=1 and @IsInsert=1  then 			-- при добавлении по умолч - берем из ТИ/ТП/Сечения или новую если не указано
							case
								 when  Info_TI.TI_ID is not null then  isnull(Info_TI.ATSArea_ID, '')
								 when  Info_Section_List.Section_ID is not null then  isnull(Info_Section_List.ATSArea_ID2, '')
							else  '' end						
					when @InsertTIAsExists=0 and @IsInsert=0  then isnull(temp.ATSArea_ID, '')			--при изменении ATSArea_ID берем
				else temp.ATSArea_ID end, ''),
			
	NewATSArea_ID = isnull(
				case 
					when @InsertTIAsExists=0 and @IsInsert=1  then isnull(temp.NewATSArea_ID, '')		-- при добавлении всегда NewATSArea_ID
					when @InsertTIAsExists=1 and @IsInsert=1  then -- при добавлении по умолч - берем из ТИ или новую если в ТИ нет
							case
								 when  Info_TI.TI_ID is not null then  isnull(Info_TI.ATSArea_ID, '')
								 when  Info_Section_List.Section_ID is not null then  isnull(Info_Section_List.ATSArea_ID2, '')
							else  '' end
					when @InsertTIAsExists=0 and @IsInsert=0  then isnull(temp.NewATSArea_ID, '')		--при изменении NewATSArea_ID берем
				else temp.NewATSArea_ID end, ''),
			
	TI_ID=temp.TI_ID,
	Section_ID=temp.Section_ID,

	ATSCode=isnull(case when @InsertTIAsExists=1 then 
							case
								 when  Info_TI.TI_ID is not null then  isnull(Info_TI.TIATSCode, '')
								 when  Info_Section_List.Section_ID is not null then  isnull(Info_Section_List.ATSSectionCode, '')
							else  '' end										
						when @InsertTIAsExists=0 then 
									case 
										when isnull(temp.ATSCode ,'')='' then 
															case
																 when  Info_TI.TI_ID is not null then  isnull(Info_TI.TIATSCode, '')
																 when  Info_Section_List.Section_ID is not null then  isnull(Info_Section_List.ATSSectionCode, '')
															else  '' end  
										else temp.ATSCode end
						else temp.ATSCode end,''),

	DataSourceType		=	temp.DataSourceType,

	AIAllowExport = case 
						when Info_TI.TI_ID is not null and @InsertTIAsExists=1 and Info_TI.XMLAIATSCode is null then 0
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLAIATSCode is not null  then 1
						when @InsertTIAsExists=0 then temp.AIAllowExport
						else temp.AIAllowExport end,			
	AICode  = isnull(case 
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1  then Info_TI.XMLAIATSCode					
						when @InsertTIAsExists=0 then temp.AICode
						else temp.AICode end,'01'),		
	AOAllowExport = case 
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLAOATSCode is null then 0
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLAOATSCode is not null  then 1
						when @InsertTIAsExists=0 then temp.AOAllowExport
						else temp.AOAllowExport end,			
	AOCode  = isnull(case 
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1  then Info_TI.XMLAOATSCode					
						when @InsertTIAsExists=0 then temp.AOCode
						else temp.AOCode end,'02'),	
	RIAllowExport = case 
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLRIATSCode is null then 0
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLRIATSCode is not null  then 1
						when @InsertTIAsExists=0 then temp.RIAllowExport
						else temp.RIAllowExport end,			
	RICode  =  isnull(case 
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1  then Info_TI.XMLRIATSCode					
						when @InsertTIAsExists=0 then temp.RICode
						else temp.RICode end,'03'),	
							
	ROAllowExport = case 
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLROATSCode is null then 0
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1 and Info_TI.XMLROATSCode is not null  then 1
						when @InsertTIAsExists=0 then temp.ROAllowExport
						else temp.ROAllowExport end,			
	ROCode  =  isnull(case 
						when Info_TI.TI_ID is not null and  @InsertTIAsExists=1  then Info_TI.XMLROATSCode					
						when @InsertTIAsExists=0 then temp.ROCode
						else temp.ROCode end	,'04')	,
	UseAlternativeAlgorithmVersion= temp.UseAlternativeAlgorithmVersion,
	AlternativeObjectName =temp.AlternativeObjectName
	from @SourceTable temp 
		Outer apply (select top 1 * from  Info_TI where temp.TI_ID=Info_TI.TI_ID) as Info_TI
	Outer apply (select top 1 * from  Info_Section_List where temp.Section_ID=Info_Section_List.Section_ID) as Info_Section_List

	)
	as res
	where 
	not exists (select top 1 1 from @MergeSourceTable temp2 where temp2.XMLExportConfig_ID=res.XMLExportConfig_ID and temp2.ATSArea_ID=res.ATSArea_ID and temp2.NewATSArea_ID=res.NewATSArea_ID and temp2.TI_ID=res.TI_ID)


end
	
	
if (@IsInsert=0 and  exists (select 1 from @MergeSourceTable where ISNULL(ATSArea_ID,'') like '' or ISNULL(NewATSArea_ID,'') like '' ))
begin
 RAISERROR ('Укажите АТС территорию',16,1);
 return;
end

if (@IsInsert=1 and  exists (select 1 from @MergeSourceTable where ISNULL(NewATSArea_ID,'') like '' ))
begin
 declare @srt varchar(max)=''
	
	select  @srt=@srt+tempMsg.ObjName from 
	(
	select distinct top 50 ObjName='ТИ "' +vw_Dict_Hierarchy.PSName+' / '+vw_Dict_Hierarchy.TIName+'"'+char(10) 
	from @MergeSourceTable temp join vw_Dict_Hierarchy on temp.TI_ID= vw_Dict_Hierarchy.TI_ID
	 where ISNULL(temp.NewATSArea_ID,'') like ''
	and temp.TI_ID is not null
	union
		select distinct top 50 ObjName='Сечение "' +Info_Section_List.SectionName+'"  (id='+convert(varchar,Info_Section_List.Section_ID)+')'+char(10) 
	from @MergeSourceTable temp join Info_Section_List on temp.Section_ID= Info_Section_List.Section_ID
	 where ISNULL(temp.NewATSArea_ID,'') like ''
	and temp.Section_ID is not null
	)
	as tempMsg
	 
	set @srt='Не указана АТС территория для объектов: '+char(10)+@srt
	RAISERROR (@srt,16,1);

	--выходим
	return;

  
	delete from @MergeSourceTable where ISNULL(NewATSArea_ID,'') like ''
	-- return;
end

--Добавляем автоматом ОВ в @MergeSourceTable
--если не добавлены вручную
--отдельно в каждую Area (если змещаемая ТИ в 2х территориях то и ОВ в двух)
declare @config_ID int
set @config_ID= (select top 1 XMLExportConfig_ID from @MergeSourceTable)

 

INSERT into @MergeSourceTable 
(
XMLExportConfig_ID ,
ATSArea_ID ,
NewATSArea_ID,
TI_ID	,
ATSCode ,
DataSourceType	,
AIAllowExport ,
AICode ,
AOAllowExport ,
AOCode ,
RIAllowExport ,
RICode ,
ROAllowExport ,
ROCode  ,
UseAlternativeAlgorithmVersion,	
AlternativeObjectName 
)
SELECT 
DISTINCT
XMLExportConfig_ID ,
temp.NewATSArea_ID ,
temp.NewATSArea_ID,
Hard_OV_List.TI_ID,
ti.TIATSCode,
DataSourceType=null, -- маловероятно но у ТИ b ОВ могут быть разные источники, но ставим по приоритету..
AIAllowExport =1,
AICode='01' ,
AOAllowExport =1,
AOCode ='02',
RIAllowExport =1,
RICode='03' ,
ROAllowExport =1,
ROCode ='04' ,
UseAlternativeAlgorithmVersion	=0		,
AlternativeObjectName= ti.TIName
FROM 
Hard_OV_List
join Hard_OV_Positions_List on Hard_OV_List.OV_ID= Hard_OV_Positions_List.OV_ID
--выбираем терриории
CROSS APPLY (SELECT distinct XMLExportConfig_ID, ATSArea_ID, NewATSArea_ID FROM @MergeSourceTable t where t.TI_ID is not null and  t.TI_ID=Hard_OV_Positions_List.TI_ID) temp
--выбираем код АТС ОВ 
CROSS APPLY (SELECT distinct TIATSCode, TIName FROM Info_TI where Info_TI.TI_ID=Hard_OV_List.TI_ID ) ti
WHERE
--ищем ОВ только для ТИ, для сечений не надо
--ОВ еще нет в конфигурации
 not exists (select top 1 1 from Expl_XML_Export_ConfigContent 
				where Expl_XML_Export_ConfigContent.TI_ID =Hard_OV_List.TI_ID
				and Expl_XML_Export_ConfigContent.XMLExportConfig_ID= temp.XMLExportConfig_ID
				and Expl_XML_Export_ConfigContent.ATSArea_ID=isnull(temp.NewATSArea_ID ,temp.ATSArea_ID)			)
--и ОВ нет в списке добавляемых ТИ
 and not exists  (select top 1 1 from @MergeSourceTable temp1 
					where temp1.TI_ID =Hard_OV_List.TI_ID
					and temp1.XMLExportConfig_ID= temp.XMLExportConfig_ID
					and isnull(temp.NewATSArea_ID ,temp.ATSArea_ID)=isnull(temp.NewATSArea_ID ,temp.ATSArea_ID))

--если остались пустые
update @MergeSourceTable
set ATSArea_ID= NewATSArea_ID
where ISNULL(ATSArea_ID,'') like ''


update @MergeSourceTable
set
DataSourceType	= case when DataSourceType=255 then null else DataSourceType end,
AIAllowExport= convert(bit,AIAllowExport) ,
AICode = right('00'+isnull(convert(varchar,AICode),'01'),2),
AOAllowExport = convert(bit,AOAllowExport),
AOCode = right('00'+isnull(convert(varchar,AOCode),'02'),2),
RIAllowExport = convert(bit,RIAllowExport),
RICode =right('00'+isnull(convert(varchar,RICode),'03'),2),
ROAllowExport = convert(bit,ROAllowExport),
ROCode = right('00'+isnull(convert(varchar,ROCode),'04'),2)



--таблица с результатом
declare @result [Expl_XML_Export_ConfigContent_TableType]

delete from @result

print 'Замена обоих записей при изменении ATSArea_ID'
insert into @result
(
ActionName,
	XMLExportConfig_ID ,
	ATSArea_ID ,
	NewATSArea_ID,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	UseAlternativeAlgorithmVersion	,
	AlternativeObjectName
	)
select distinct 'replace',XMLExportConfig_ID ,
	ATSArea_ID ,
	NewATSArea_ID,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode  ,
	temp.UseAlternativeAlgorithmVersion	,
	temp.AlternativeObjectName
	from @MergeSourceTable temp
where 
temp.NewATSArea_ID<>temp.ATSArea_ID
and @IsInsert= 0
--старая АИС должна быть в БД
and exists 
(select top 1 1 from Expl_XML_Export_ConfigContent where XMLExportConfig_ID=temp.XMLExportConfig_ID  and ATSArea_ID= temp.ATSArea_ID
 and 
	(
		(TI_ID=temp.TI_ID and temp.TI_ID is not null and TI_ID is not null)
		or
		(Section_ID=temp.Section_ID and temp.Section_ID is not null and Section_ID is not null)
	)  
)


--ЗАМЕНА  удаляем обе записи  старую и новую (так как новая тоже могла быть)
delete del
from Expl_XML_Export_ConfigContent del
join  @result temp
on
del.XMLExportConfig_ID=temp.XMLExportConfig_ID 
and (del.ATSArea_ID= temp.ATSArea_ID or del.ATSArea_ID= temp.NewATSArea_ID)
and 
	(
		(del.TI_ID=temp.TI_ID and temp.TI_ID is not null and del.TI_ID is not null)
		or
		(del.Section_ID=temp.Section_ID and temp.Section_ID is not null and del.Section_ID is not null)
	) 
and temp.NewATSArea_ID<>temp.ATSArea_ID
and ISNULL (temp.ActionName,'') like 'replace'


--для замен добавляем новую запись
insert into Expl_XML_Export_ConfigContent
(	XMLExportConfig_ID ,
	ATSArea_ID ,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	UseAlternativeAlgorithmVersion,
	AlternativeObjectName
	)
select 
distinct 
	XMLExportConfig_ID ,
	NewATSArea_ID ,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	temp.UseAlternativeAlgorithmVersion,
	temp.AlternativeObjectName
from @result temp
where 
temp.NewATSArea_ID<>temp.ATSArea_ID
and ISNULL (temp.ActionName,'') like 'replace'
and not exists 
(select top 1 1 from Expl_XML_Export_ConfigContent where XMLExportConfig_ID=temp.XMLExportConfig_ID  and ATSArea_ID= temp.NewATSArea_ID 
 and 
	(
		(TI_ID=temp.TI_ID and temp.TI_ID is not null and TI_ID is not null)
		or
		(Section_ID=temp.Section_ID and temp.Section_ID is not null and Section_ID is not null)
	) 
)


--ДОБАВЛЕНИЕ отсутствующих (по NewATSArea_ID)'
insert into @result
(
ActionName,
	XMLExportConfig_ID ,
	ATSArea_ID ,
	NewATSArea_ID,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	UseAlternativeAlgorithmVersion,
	AlternativeObjectName
	)
select distinct 'Insert',
XMLExportConfig_ID ,
	ATSArea_ID ,
	NewATSArea_ID,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	temp.UseAlternativeAlgorithmVersion,
	temp.AlternativeObjectName
from @MergeSourceTable temp
where 
not exists 
(
select top 1 1 from Expl_XML_Export_ConfigContent 
where 
XMLExportConfig_ID=temp.XMLExportConfig_ID 
and (ATSArea_ID= temp.NewATSArea_ID)
and 
	(
		(TI_ID=temp.TI_ID and temp.TI_ID is not null and TI_ID is not null)
		or
		(Section_ID=temp.Section_ID and temp.Section_ID is not null and Section_ID is not null)
	) 
)
and not exists 
(
select top 1 1 from @result  t2
where 
t2.XMLExportConfig_ID=temp.XMLExportConfig_ID 
and (t2.ATSArea_ID= temp.ATSArea_ID)
and 
	(
		(t2.TI_ID=temp.TI_ID and temp.TI_ID is not null and t2.TI_ID is not null)
		or
		(t2.Section_ID=temp.Section_ID and temp.Section_ID is not null and t2.Section_ID is not null)
	) 
)

insert into Expl_XML_Export_ConfigContent
(	XMLExportConfig_ID ,
	ATSArea_ID ,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	UseAlternativeAlgorithmVersion,
	AlternativeObjectName
	)
select 
distinct 
	XMLExportConfig_ID ,
	NewATSArea_ID ,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	temp.UseAlternativeAlgorithmVersion,
	temp.AlternativeObjectName
from @result temp
where 
not exists 
(
select top 1 1 from Expl_XML_Export_ConfigContent 
where 
XMLExportConfig_ID=temp.XMLExportConfig_ID 
and (ATSArea_ID= temp.NewATSArea_ID)
and 
	(
		(TI_ID=temp.TI_ID and temp.TI_ID is not null and TI_ID is not null)
		or
		(Section_ID=temp.Section_ID and temp.Section_ID is not null and Section_ID is not null)
	) 
)
and isnull(temp.ActionName,'') like 'insert'



-- РЕДАКТИРОВАНИЕ 
insert into @result
(
ActionName,
	XMLExportConfig_ID ,
	ATSArea_ID ,
	NewATSArea_ID,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	UseAlternativeAlgorithmVersion,
	AlternativeObjectName
	)
select distinct 'update',XMLExportConfig_ID ,
	ATSArea_ID ,
	NewATSArea_ID,
	TI_ID	,
	Section_ID	,
	ATSCode ,
	DataSourceType	,
	AIAllowExport ,
	AICode ,
	AOAllowExport ,
	AOCode ,
	RIAllowExport ,
	RICode ,
	ROAllowExport ,
	ROCode ,
	UseAlternativeAlgorithmVersion,
	AlternativeObjectName
	 from @MergeSourceTable temp
where 
exists 
(
	select top 1 1 from Expl_XML_Export_ConfigContent 
	where 
	XMLExportConfig_ID=temp.XMLExportConfig_ID 
	and (ATSArea_ID= temp.NewATSArea_ID)
	and 
	(
		(TI_ID=temp.TI_ID and temp.TI_ID is not null and TI_ID is not null)
		or
		(Section_ID=temp.Section_ID and temp.Section_ID is not null and Section_ID is not null)
	) 
)
and not exists 
(
	select top 1 1 from @result  t2
	where 
	t2.XMLExportConfig_ID=temp.XMLExportConfig_ID 
	and (t2.ATSArea_ID= temp.ATSArea_ID)
	and 
	(
		(t2.TI_ID=temp.TI_ID and temp.TI_ID is not null and t2.TI_ID is not null)
		or
		(t2.Section_ID=temp.Section_ID and temp.Section_ID is not null and t2.Section_ID is not null)
	) 
)
and @IsInsert=0

update  Expl_XML_Export_ConfigContent
set
	ATSArea_ID=convert(varchar,temp.NewATSArea_ID),
	ATSCode = convert(varchar,temp.ATSCode),
	--255=по приоритету -пишем null
	DataSourceType	= temp.DataSourceType,
	AIAllowExport= convert(bit,temp.AIAllowExport) ,
	AICode = right('00'+isnull(convert(varchar,temp.AICode),'01'),2),
	AOAllowExport = convert(bit,temp.AOAllowExport),
	AOCode = right('00'+isnull(convert(varchar,temp.AOCode),'02'),2),
	RIAllowExport = convert(bit,temp.RIAllowExport),
	RICode =right('00'+isnull(convert(varchar,temp.RICode),'03'),2),
	ROAllowExport = convert(bit,temp.ROAllowExport),
	ROCode = right('00'+isnull(convert(varchar,temp.ROCode),'04'),2),
	UseAlternativeAlgorithmVersion=temp.UseAlternativeAlgorithmVersion,
	AlternativeObjectName=temp.AlternativeObjectName
from Expl_XML_Export_ConfigContent
join @result temp on temp.XMLExportConfig_ID= Expl_XML_Export_ConfigContent.XMLExportConfig_ID
and temp.NewATSArea_ID=Expl_XML_Export_ConfigContent.ATSArea_ID
and 
	(
		(Expl_XML_Export_ConfigContent.TI_ID=temp.TI_ID and temp.TI_ID is not null and Expl_XML_Export_ConfigContent.TI_ID is not null)
		or
		(Expl_XML_Export_ConfigContent.Section_ID=temp.Section_ID and temp.Section_ID is not null and Expl_XML_Export_ConfigContent.Section_ID is not null)
	) 
and isnull(temp.ActionName,'') like 'update'
where
@IsInsert=0




-- так как дата в ключе, то вот таким образом добавляем события в журнал действий пользователей
insert into 
Expl_User_Journal
(
User_ID,	EventDateTime,	EventString,	CommentString,	ApplicationType,	EventCategory,	ObjectName,	ObjectID,	ParentObjectName,	ParentObjectID,	EventType,	CUS_ID
)
select distinct *
from 
(
	select distinct
	User_ID=@User_ID,
	EventDateTime=dateadd(MILLISECOND,(ROW_NUMBER() over (order by TI_ID))*5, dateadd(SECOND,-3,@startDate)),
	EventString=
	replace(replace(replace(isnull(ActionName,''),'INSERT','Добавлен объект '),'UPDATE','Изменен объект'),'replace','Заменена Area')	
	+
	' AreaId='+convert(varchar,ATSArea_ID)+
	' NewATSArea_ID='+convert(varchar,NewATSArea_ID)+
	' Код АТС='+ATSCode+
	' Источник='+convert(varchar,isnull(DataSourceType,255)),

	CommentString=case when @InsertTIAsExists=1 then '1' else '0' end+
	'; '+case when AIAllowExport=1 then '1' else '0' end+'/'+AICode+
	'; '+case when AOAllowExport=1 then '1' else '0' end+'/'+AOCode+
	'; '+case when RIAllowExport=1 then '1' else '0' end+'/'+RICode+
	'; '+case when ROAllowExport=1 then '1' else '0' end+'/'+ROCode,
	ApplicationType=1,
	EventCategory=0,
	ObjectName = case 
					when TI_ID is not null then 'Info_TI'
					when Section_ID is not null then 'Info_Section_List'
				else '' end,
	ObjectID=	case 
					when TI_ID is not null then convert(varchar,TI_ID)
					when Section_ID is not null then  convert(varchar,Section_ID)
				else '' end,
	ParentObjectName='Expl_XML_Export_Config',
	ParentObjectID=convert(varchar,XMLExportConfig_ID),
	EventType=case when ActionName like '%insert%' then 0
	else 1 end,
	CUS_ID=0
	from @result
)
as res
where not exists(select top 1 1 from Expl_User_Journal where User_ID=res.User_ID and EventDateTime= res.EventDateTime)

END
GO

grant EXECUTE on usp2_XMLExportConfigContent_Merge to [UserCalcService]
go
grant EXECUTE on usp2_XMLExportConfigContent_Merge to [UserDeclarator]
go

