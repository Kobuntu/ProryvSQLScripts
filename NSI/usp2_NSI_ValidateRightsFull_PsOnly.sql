if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Forecast_DataRead')
          and type in ('P','PC'))
   drop procedure usp2_Forecast_DataRead
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Forecast_WorkInHoursRead')
          and type in ('P','PC'))
   drop procedure usp2_Forecast_WorkInHoursRead
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_NSI_ValidateRightsFull_PsOnly')
          and type in ('P','PC'))
   drop procedure usp2_NSI_ValidateRightsFull_PsOnly
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_OldTelescope_Read_Data')
          and type in ('P','PC'))
   drop procedure usp2_OldTelescope_Read_Data
go


IF  NOT EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss 	ON st.schema_id = ss.schema_id WHERE st.name = N'StringType' AND ss.name = N'dbo')
CREATE TYPE [dbo].[StringType] 
AS TABLE
(
	StringValue nvarchar(800) NOT NULL 
) 
GO

grant EXECUTE on TYPE::StringType to [UserCalcService]
go
grant EXECUTE on TYPE::StringType to [UserDeclarator]
go
grant EXECUTE on TYPE::StringType to [UserImportService]
go
grant EXECUTE on TYPE::StringType to [UserExportService]
go


if exists (select 1
          from sysobjects
          where  id = object_id('usp2_NSI_ValidateRightsFull_PsOnly')
          and type in ('P','PC'))
   drop procedure usp2_NSI_ValidateRightsFull_PsOnly
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Карпов Денис
--
-- Дата создания:
--
--		2016
--
-- Описание:
--
--	Проверка прав пользователя на несколько PS_ID в стандартном дереве, возвращает разрешенные ИД
-- ======================================================================================

create proc [dbo].usp2_NSI_ValidateRightsFull_PsOnly
	@User_ID nvarchar(200),
	@Right_ID nvarchar(200),
	@DBObjectList StringType READONLY
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


--возвращаем таблицу с перечнем StringValue
--процедура используется в Telescope.Office, поэтому при исправлениях следует проверять его работоспособность


declare @AdminRight_ID varchar(200)
set @AdminRight_ID='F7E018EE-C70B-4094-86F8-504057E7AF44'  --право admin

declare @Result int
set @Result=0
set @Right_ID=isnull(@Right_ID,'') 
set @User_ID=isnull(@User_ID,'') 

declare @ObjectTypeName nvarchar(200)= 'Dict_PS_'

--ИД объекта в БД. тип объекта, ИД узла, название, ИД в Expl_Users_DBObjects
declare @tempParentObj table 
(
CheckedObjectID nvarchar(200),
ObjectrealID nvarchar(200),
ObjectTypeName nvarchar(200),
FreeHierItem_ID int, 
StringName nvarchar(400), 
ID varchar(200), 
Lvl int,
IsAssent bit
)


insert into @tempParentObj (CheckedObjectID,ObjectrealID,ObjectTypeName,FreeHierItem_ID,StringName,ID,Lvl,IsAssent)
	select  distinct
		objList.StringValue,
		CONVERT(varchar(200),vw_Dict_HierarchyPS.HierLev3_ID ), 
		'Dict_HierLev3', 
		null,
		StringName=vw_Dict_HierarchyPS.HierLev3StringName, 
		null,3, null
	from vw_Dict_HierarchyPS 
	join @DBObjectList objList on CONVERT(int, objList.StringValue)=vw_Dict_HierarchyPS.PS_ID
union	
	select  distinct
		objList.StringValue,
		CONVERT(varchar(200),vw_Dict_HierarchyPS.HierLev2_ID ), 
		'Dict_HierLev2_', 
		null,
		StringName=vw_Dict_HierarchyPS.HierLev2StringName, 
		null,2, null
	from vw_Dict_HierarchyPS 
	join @DBObjectList objList on CONVERT(int, objList.StringValue)=vw_Dict_HierarchyPS.PS_ID
union	
	select  distinct
		objList.StringValue,
		CONVERT(varchar(200),vw_Dict_HierarchyPS.HierLev1_ID ), 
		'Dict_HierLev1_', 
		null,
		StringName=vw_Dict_HierarchyPS.StringName, 
		null,1, null
	from vw_Dict_HierarchyPS 
	join @DBObjectList objList on CONVERT(int, objList.StringValue)=vw_Dict_HierarchyPS.PS_ID

union
select objList.StringValue, objList.StringValue, @ObjectTypeName, null, 'выбранный объект', null, 999, null
from @DBObjectList objList



--получаем Объекты прав  объекты
update @tempParentObj
set ID= Expl_Users_DBObjects.ID
from 
 Expl_Users_DBObjects
 join @tempParentObj temp 
on  Expl_Users_DBObjects.ObjectTypeName=temp.ObjectTypeName and 
Expl_Users_DBObjects.Object_ID=temp.ObjectrealID

--удаляем объекты и узлы не найденные в Expl_Users_DBObjects
delete from @tempParentObj
where ID is null

--если заблокирован или неподтвержден или удален - нет прав
if (not exists (select  1 from Expl_Users where [User_ID]=@User_ID) or exists (select 1 from Expl_Users where [User_ID]=@User_ID and (IsLockedOut=1 or IsApproved=0 or Deleted=1)))
begin
	select StringValue= ''
end 

--если пользователь роль - админ то есть права на все запрошенные объекты
else if  (exists (select  1 from Expl_Users where [User_ID]=@User_ID and UserRole=1))
begin
	set @Result=1

	select distinct StringValue from @DBObjectList
end
--для остальных объектов проверяем права
else 
begin
	
	update @tempParentObj
	set
	IsAssent=isnull(Expl_UserGroup_Right.IsAssent,1)
	from 
	Expl_User_UserGroup 
	join Expl_UserGroup_Right on Expl_UserGroup_Right.UserGroup_ID=Expl_User_UserGroup.UserGroup_ID 
	join Expl_Users_DBObjects on Expl_Users_DBObjects.ID= Expl_UserGroup_Right.DBObject_ID
	join  @tempParentObj temp on temp.ID=Expl_Users_DBObjects.ID
	where 
	Expl_User_UserGroup.Deleted=0 
	and Expl_User_UserGroup.[User_ID]= @User_ID
	and (Expl_UserGroup_Right.RIGHT_ID=@Right_ID or Expl_UserGroup_Right.RIGHT_ID=@AdminRight_ID) --указанное право или админ
	and Expl_User_UserGroup.Deleted=0
	and Expl_UserGroup_Right.Deleted=0
	and Expl_Users_DBObjects.Deleted=0
	
	--оставляем только объекты на которые реально дано это право (с учетом старых версий isnull(Expl_UserGroup_Right.IsAssent,1))
	delete from @tempParentObj where IsAssent is null

	--в таблице остались только объекты и их родители на которые есть права
	--выбираем разрешенные объекты	
	select distinct
	StringValue=res.CheckedObjectID
	from 
	(
		--берем ближайший (по max lvl) - сам объект или его родителя
		--чем больше Lvl тем ближе он к объекту
		select CheckedObjectID,ObjectrealID,ObjectTypeName,FreeHierItem_ID,StringName,ID,maxLvl=max(Lvl), IsAssent	
		from
		@tempParentObj 
		group by CheckedObjectID,ObjectrealID,ObjectTypeName,FreeHierItem_ID,StringName,ID,IsAssent
	)
	as res
	where res.IsAssent =1 


end


end
go

   grant EXECUTE on usp2_NSI_ValidateRightsFull_PsOnly to [UserCalcService]
go
   grant EXECUTE on usp2_NSI_ValidateRightsFull_PsOnly to [UserDeclarator]
go
   grant EXECUTE on usp2_NSI_ValidateRightsFull_PsOnly to [UserImportService]
go
   grant EXECUTE on usp2_NSI_ValidateRightsFull_PsOnly to [UserExportService]
go

--declare
--@User_ID  nvarchar(200) = '800065UUTE1W0MC70U8E7I',
--@Right_ID nvarchar(200) = 'D1B5533F-E284-45a8-B86B-79011521941F'
--declare @DBObjectList StringType
--insert into @DBObjectList
--values 
--('12823')
--exec usp2_NSI_ValidateRightsFull_PsOnly @User_ID, @Right_ID, @DBObjectList

--=======================================================================================

-- Прогнозирование

--=======================================================================================

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2017
--
-- Описание:
--
--		Архивные данные прогнозов
--
-- ======================================================================================
create proc [dbo].[usp2_Forecast_DataRead]
	@forecastObjectUns dbo.StringType READONLY, --Таблица объектов для чтения
	@dtStart DateTime, -- Начальная дата, время
	@dtEnd DateTime, -- Конечная дата, время
	@Priority tinyint  = null, -- Приоритет (читаем только по одному приоритету)
	@forecastCalculateModelId int = null -- Если нужен прогноз составленный на определенной модели
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Выбираем параметры глубины разрешенных изменений данных и исключения
select * from [dbo].[Forecast_Objects_PlanTimeRules]
where ForecastObject_UN in (select StringValue from @forecastObjectUns)
and StartDate <= @dtEnd
and (FinishDate is null or FinishDate >= @dtStart)

--Исключения
select * from [dbo].[Forecast_Objects_PlanTimeRules_Exceptions]
where ForecastObject_UN in (select StringValue from @forecastObjectUns)
and EventDate between @dtStart and @dtEnd

--Субабоненты
--select * from [dbo].[Forecast_Subabonents]
--where ForecastObject_UN in (select StringValue from @forecastObjectUns)

--Группы
select fo.ForecastObject_UN, g.*  from [dbo].[Forecast_Objects] fo
join [dbo].[Forecast_Groups] g on g.ForecastGroup_ID = fo.ForecastGroup_ID
where fo.ForecastObject_UN in (select StringValue from @forecastObjectUns)
and fo.ForecastGroup_ID is not null

declare @dtStartDay DateTime;
set @dtStartDay = FLOOR(cast(@dtStart as float))

declare @forecastObjectUn varchar(22)

-- [Priority] 0 - максимальный приоритет

select a.AUTO_01, a.AUTO_02, a.AUTO_03, a.AUTO_04, a.AUTO_05, a.AUTO_06, a.AUTO_07, a.AUTO_08, a.AUTO_09, a.AUTO_10,
	a.AUTO_11, a.AUTO_12, a.AUTO_13, a.AUTO_14, a.AUTO_15, a.AUTO_16, a.AUTO_17, a.AUTO_18, a.AUTO_19, a.AUTO_20,
	a.AUTO_21, a.AUTO_22, a.AUTO_23, a.AUTO_24, a.AUTO_25, a.AUTO_26, a.AUTO_27, a.AUTO_28, a.AUTO_29, a.AUTO_30,
	a.AUTO_31, a.AUTO_32, a.AUTO_33, a.AUTO_34, a.AUTO_35, a.AUTO_36, a.AUTO_37, a.AUTO_38, a.AUTO_39, a.AUTO_40,
	a.AUTO_41, a.AUTO_42, a.AUTO_43, a.AUTO_44, a.AUTO_45, a.AUTO_46, a.AUTO_47, a.AUTO_48, 
	a.MANUAL_01, a.MANUAL_02, a.MANUAL_03, a.MANUAL_04, a.MANUAL_05, a.MANUAL_06, a.MANUAL_07, a.MANUAL_08, a.MANUAL_09, a.MANUAL_10,
	a.MANUAL_11, a.MANUAL_12, a.MANUAL_13, a.MANUAL_14, a.MANUAL_15, a.MANUAL_16, a.MANUAL_17, a.MANUAL_18, a.MANUAL_19, a.MANUAL_20,
	a.MANUAL_21, a.MANUAL_22, a.MANUAL_23, a.MANUAL_24, a.MANUAL_25, a.MANUAL_26, a.MANUAL_27, a.MANUAL_28, a.MANUAL_29, a.MANUAL_40,
	a.MANUAL_31, a.MANUAL_32, a.MANUAL_33, a.MANUAL_34, a.MANUAL_35, a.MANUAL_36, a.MANUAL_37, a.MANUAL_38, a.MANUAL_39, a.MANUAL_30,
	a.MANUAL_41, a.MANUAL_42, a.MANUAL_43, a.MANUAL_44, a.MANUAL_45, a.MANUAL_46, a.MANUAL_47, a.MANUAL_48,
	a.FACT_01, a.FACT_02, a.FACT_03, a.FACT_04, a.FACT_05, a.FACT_06, a.FACT_07, a.FACT_08, a.FACT_09, a.FACT_10,
	a.FACT_11, a.FACT_12, a.FACT_13, a.FACT_14, a.FACT_15, a.FACT_16, a.FACT_17, a.FACT_18, a.FACT_19, a.FACT_20,
	a.FACT_21, a.FACT_22, a.FACT_23, a.FACT_24, a.FACT_25, a.FACT_26, a.FACT_27, a.FACT_28, a.FACT_29, a.FACT_40,
	a.FACT_31, a.FACT_32, a.FACT_33, a.FACT_34, a.FACT_35, a.FACT_36, a.FACT_37, a.FACT_38, a.FACT_39, a.FACT_30,
	a.FACT_41, a.FACT_42, a.FACT_43, a.FACT_44, a.FACT_45, a.FACT_46, a.FACT_47, a.FACT_48,

	StringValue as ForecastObject_UN, f.dt as EventDate, [Priority], 
	[ForecastCalculateModel_ID], j.[User_ID], j.DispatchDateTime, j.Comment
	--into #journal
	from 
	(
		select * from @forecastObjectUns, usf2_Utils_HalfHoursByPeriod(@dtStart, @dtEnd)
	)f
	outer apply 
	(
		select top 1 * from [dbo].[Forecast_Archive_Journal]
		where ForecastObject_UN = f.StringValue and EventDate  = f.dt 
		and (@Priority is null or (@Priority is not null and [Priority] = @Priority)) --Фильтр по приоритету
		and (@forecastCalculateModelId is null or (@forecastCalculateModelId is not null and ForecastCalculateModel_ID = @forecastCalculateModelId)) --Фильтр по прогнозу составленному по определенной модели
		order by [Priority]
	) j
	outer apply usf2_Forecast_JoinToArchives([ForecastCalculateModel_ID], j.ForecastArchiveJournal_UN, f.StringValue) a

	order by f.StringValue, EventDate,[Priority]
	--select * from #journal
end
go
   grant EXECUTE on usp2_Forecast_DataRead to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2017
--
-- Описание:
--
--		Планы прогнозов
--
-- ======================================================================================
create proc [dbo].[usp2_Forecast_WorkInHoursRead]
	@forecastObjectUns dbo.StringType READONLY, --Таблица объектов для чтения
	@startDay DateTime, -- Начальная дата, время
	@endDay DateTime, -- Конечная дата, время
	@Priority tinyint  = null -- Приоритет
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Округление дат
set @startDay = floor(cast(@startDay as float));
set @endDay = floor(cast(@endDay as float));

--Выбираем параметры глубины разрешенных изменений данных и исключения

select * from [dbo].[Forecast_Objects_PlanTimeRules]
where ForecastObject_UN in (select StringValue from @forecastObjectUns)
and StartDate <= @endDay
and (FinishDate is null or FinishDate >= @startDay)

--Исключения
select * from [dbo].[Forecast_Objects_PlanTimeRules_Exceptions]
where ForecastObject_UN in (select StringValue from @forecastObjectUns)
and EventDate between @startDay and @endDay

if (@Priority is not null) begin
--Если приоритет явно задан
	select j.ForecastObject_UN, 
	j.EventDate, j.Priority, j.ForecastArchiveJournal_UN, 
	j.User_ID, j.DispatchDateTime, j.Comment,
	w.*  
	from [dbo].[Forecast_Archive_Journal] j 
	join Forecast_Archive_Data_WorkInHours w on w.ForecastArchiveJournal_UN = j.ForecastArchiveJournal_UN
	where j.ForecastObject_UN in (select StringValue from @forecastObjectUns)
	and EventDate between @startDay and @endDay
	and [Priority] = @Priority
	and ForecastCalculateModel_ID = 1
	order by j.EventDate, j.ForecastObject_UN, [Priority], w.ForecastObjectTypeMode_ID
end else begin
--Выбираем приоритет с минимальным значением (по смыслу он максимальный)
	select j.ForecastObject_UN, 
	j.EventDate, j.Priority, j.ForecastArchiveJournal_UN, 
	j.User_ID, j.DispatchDateTime, j.Comment,
	w.*  
	from @forecastObjectUns u
	join [dbo].[Forecast_Archive_Journal] j on j.ForecastObject_UN = u.StringValue 
	and j.Priority = 
	(
		select min(Priority) from [Forecast_Archive_Journal] where ForecastObject_UN = u.StringValue
		and EventDate = j.EventDate
	) 
	join Forecast_Archive_Data_WorkInHours w on w.ForecastArchiveJournal_UN = j.ForecastArchiveJournal_UN
	where j.ForecastObject_UN in (select StringValue from @forecastObjectUns)
	and EventDate between @startDay and @endDay
	--and (@Priority is null or [Priority] = @Priority)
	and ForecastCalculateModel_ID = 1
	order by j.EventDate, j.ForecastObject_UN, [Priority], w.ForecastObjectTypeMode_ID
end
end
go
   grant EXECUTE on usp2_Forecast_WorkInHoursRead to [UserCalcService]
go