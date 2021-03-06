if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetTIorContrTIForPS')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetTIorContrTIForPS
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetTisForTpByFormula')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetTisForTpByFormula
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetTisForTpByFormula')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetTisForTpByFormula
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Alarms_ReadTopNumbers')
          and type in ('P','PC'))
   drop procedure usp2_Alarms_ReadTopNumbers
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetTIandNearestTI')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetTIandNearestTI
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Monit_GetTreeInfoForMonitoring')
          and type in ('P','PC'))
   drop procedure usp2_Monit_GetTreeInfoForMonitoring
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchCommGetAllTIGroupinJournals')
          and type in ('P','PC'))
   drop procedure usp2_ArchCommGetAllTIGroupinJournals
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetTransfomatorsPropertyForLostFormulas')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetTransfomatorsPropertyForLostFormulas
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetTisForTpByFormula')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetTisForTpByFormula
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UserHasRightForTis')
          and type in ('P','PC'))
   drop procedure usp2_UserHasRightForTis
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Rep_StatementRequestIntegrals')
          and type in ('P','PC'))
   drop procedure usp2_Rep_StatementRequestIntegrals
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Rep_TiEvents')
          and type in ('P','PC'))
   drop procedure usp2_Rep_TiEvents
GO

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ExplDoc_Residues_Section')
          and type in ('P','PC'))
   drop procedure usp2_ExplDoc_Residues_Section
GO


--Нужен дополнительный индекс
--CREATE NONCLUSTERED INDEX [IX_Alarms_Archive_1] ON [dbo].[Alarms_Archive]
--(
--	[Confirmed] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
--GO



--Создаем тип, если его еще нет
IF  not EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'IntType' AND ss.name = N'dbo')
CREATE TYPE [dbo].[IntType] AS TABLE 
(
	Id int NOT NULL
)
go

grant EXECUTE on TYPE::IntType to [UserCalcService]
go

grant EXECUTE on TYPE::IntType to [UserDeclarator]
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель 2014
--
-- Описание:
--
--		Список ТИ в ТП со стороны МСК
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_GetTisForTpByFormula]
(
	@TpValues dbo.IntType READONLY,
	@ClosedPeriod_ID uniqueidentifier = null
)
RETURNS @ret TABLE
(
	Formula_UN varchar(22),
	TI_ID int,
	ChannelType tinyint,
    UsedFormula_UN varchar(22),
	ClosedPeriod_ID uniqueidentifier,
	TP_ID int
)
as
begin
	if (@ClosedPeriod_ID is not null) begin
		with innerTi (Formula_UN, ti_id, ChannelType, UsedFormula_UN, ClosedPeriod_ID, TP_ID) as 
			(
				select fl.Formula_UN, TI_ID, fd.Channeltype, UsedFormula_UN, fl.ClosedPeriod_ID, fl.TP_ID
				from Info_TP2_OurSide_Formula_List_Closed fl
				join Info_TP2_OurSide_Formula_Description_Closed fd on fd.Formula_UN = fl.Formula_UN and fl.FormulaType_ID = 0 and fd.ClosedPeriod_ID = fl.ClosedPeriod_ID
				where fl.ClosedPeriod_ID = @ClosedPeriod_ID and fl.TP_ID in (select distinct TP_ID from Info_TP2 where EvalModeOurSide = 1 and TP_ID in (select Id from @TpValues)) and fd.TI_ID is not null 
				-----Вложенные формулы
				union all
				select fl.Formula_UN, fd.TI_ID, fd.Channeltype, fd.UsedFormula_UN, fl.ClosedPeriod_ID, i.TP_ID
				from Info_TP2_OurSide_Formula_List_Closed fl  
				join Info_TP2_OurSide_Formula_Description_Closed fd on fd.Formula_UN = fl.Formula_UN and fd.ClosedPeriod_ID = fl.ClosedPeriod_ID
				join innerTi i on fl.Formula_UN = i.UsedFormula_UN and fl.ClosedPeriod_ID = i.ClosedPeriod_ID
				where fd.TI_ID is not null 
		)
		insert into @ret select * from innerTi;
	end else begin
		declare @dt DateTime;
		set @dt = GetDate();
		with innerTi (Formula_UN, ti_id, ChannelType, UsedFormula_UN, ClosedPeriod_ID, TP_ID) as 
			(
				select fl.Formula_UN, TI_ID, fd.Channeltype, UsedFormula_UN, cast(null as uniqueidentifier) as ClosedPeriod_ID, fl.TP_ID
				from Info_TP2_OurSide_Formula_List fl
				join Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN 
				where fl.TP_ID in (select distinct TP_ID from Info_TP2 where EvalModeOurSide = 1 and TP_ID in (select Id from @TpValues)) and @dt between fl.StartDateTime and isnull(fl.FinishDateTime, '21000101') and fl.FormulaType_ID = 0 and fd.TI_ID is not null 
				-----Вложенные формулы
				union all
				select fl.Formula_UN, fd.TI_ID, fd.Channeltype, fd.UsedFormula_UN, null as ClosedPeriod_ID, i.TP_ID
				from Info_TP2_OurSide_Formula_List fl  
				join Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN
				join innerTi i on fl.Formula_UN = i.UsedFormula_UN
				where fd.TI_ID is not null 
		)
		insert into @ret select * from innerTi;
	end
	return
end
go
   grant select on usf2_Info_GetTisForTpByFormula to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2014
--
-- Описание:
--
--		Список всех ТИ участвующих в формуле, с каналами
--
-- ======================================================================================
create proc [dbo].[usp2_Info_GetTisForTpByFormula]
(	
	@TpValues IntType READONLY, --Точки поставок
	@ClosedPeriod_ID uniqueidentifier = null
)
AS
BEGIN 
			set nocount on
			set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
			set numeric_roundabort off
			set transaction isolation level read uncommitted

			select * from usf2_Info_GetTisForTpByFormula(@TpValues, @ClosedPeriod_ID)
end;
go
   grant EXECUTE on usp2_Info_GetTisForTpByFormula to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2011
--
-- Описание:
--
--		Все точки ТИ или ТИ КА для своего родителя
--
-- ======================================================================================
create proc [dbo].[usp2_Info_GetTIorContrTIForPS]
(
	@PSList IntType readonly,	--Список родителей для которых выбираем ТИ
	@IsTP bit -- Родители ТП
)
as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	declare @dt DateTime
	set @dt = GETDATE();
	--ТИ
	if (@IsTP = 1) begin-- Если фильтруем по ТП
		select  distinct ti.TI_ID as ID,
			ti.TIName as StringName,
			ti.PS_ID as P_ID, 
			ti.TIType,
			ti.Commercial,
			ti.Voltage,
			ti.SectionNumber,
			Cast (0 as bit) as isCa,
			ISNULL(ti.TPCoefOurSide,1) as TPCoef,
			cast (ISNULL(it.COEFI * it.COEFU, 1) as float(26)) as Coeff,
			cast((case when ti.AIATSCode=2 then 1 else 0 end) as bit) as IsChanelReverse,
			ti.TP_ID,0 as ContrObject_ID,
			ti.Deleted,
			ti.PhaseNumber,
			dbo.usf2_Info_GetTariffsForTI(ti.TI_ID) as TariffArray,
			ti.CustomerKind,
			ti.AbsentChannelsMask,
			hm.MeterModel_ID,
			case when hm.MeterType_ID = 2012 then dbo.usf2_Hard_Meters_Addx_RemoteDisplay(hm.MeterSerialNumber) 
			else hm.MeterSerialNumber end as MeterSerialNumber,
			hm.AllowTariffWrite,
			ti.IsSmallTI,
			ti.MeasureQuantityType_UN,
			ti.TreeCategory,
			cast(case when ov.OV_ID is not null then 1 else 0 end as bit) as IsOv
		from  
		( 
		  select distinct ps_id from Info_TI  where TP_ID in (select Id from @PSList)
		) ti_ps
		join Info_TI ti on ti_ps.ps_id = ti.ps_id
		outer apply 
			(
				select top (1) hm.* from dbo.Info_Meters_TO_TI ti_to_m
				join dbo.Hard_Meters hm on ti_to_m.METER_ID = hm.Meter_ID
				where ti.TI_ID = ti_to_m.TI_ID and ti_to_m.StartDateTime < @dt and (ti_to_m.FinishDateTime is null or ti_to_m.FinishDateTime > @dt)
				order by ti_to_m.StartDateTime desc
			) hm
		outer apply
		(
			select top (1) * from dbo.Info_Transformators
			where TI_ID = ti.TI_ID and ( FinishDateTime is null or (FinishDateTime is not null and FinishDateTime >= @dt))
			order by StartDateTime desc
		) it
		left join Hard_OV_List ov on ov.OV_ID = ti.TI_ID
		where ti.Deleted = 0
		union 
		--Связанные через формулы
		select distinct ti.TI_ID as ID,
			ti.TIName as StringName,
			ti.PS_ID as P_ID, 
			ti.TIType,
			ti.Commercial,
			ti.Voltage,
			ti.SectionNumber,
			Cast (0 as bit) as isCa,
			ISNULL(ti.TPCoefOurSide,1) as TPCoef,
			cast (ISNULL(it.COEFI * it.COEFU, 1) as float) as Coeff,
			cast((case when ti.AIATSCode=2 then 1 else 0 end) as bit) as IsChanelReverse,
			case when ti.TI_ID = fl.TI_ID then fl.TP_ID else 
				(
					select top 1 fl.TP_ID from Info_TP2_OurSide_Formula_Description fd
					join Info_TP2_OurSide_Formula_List fl on fl.Formula_UN = fd.Formula_UN
					where TI_ID is not null and TI_ID = ti.TI_ID
				)
			end as TP_ID,
			--ISNULL(fl.TP_ID, -1) as TP_ID,
			0 as ContrObject_ID,
			ti.Deleted,
			ti.PhaseNumber,
			dbo.usf2_Info_GetTariffsForTI(ti.TI_ID) as TariffArray,
			ti.CustomerKind,
			ti.AbsentChannelsMask,
			hm.MeterModel_ID,
			case when hm.MeterType_ID = 2012 then dbo.usf2_Hard_Meters_Addx_RemoteDisplay(hm.MeterSerialNumber) 
			else hm.MeterSerialNumber end as MeterSerialNumber,
			hm.AllowTariffWrite,
			ti.IsSmallTI,
			ti.MeasureQuantityType_UN,
			ti.TreeCategory,
			cast(case when ov.OV_ID is not null then 1 else 0 end as bit) as IsOv
		from  usf2_Info_GetTisForTpByFormula(@PSList, null) fl
			join Info_TI ti_tp on ti_tp.TI_ID = fl.TI_ID
			join Info_TI ti on ti.PS_ID = ti_tp.PS_ID
			outer apply 
			(
				select top (1) hm.* from dbo.Info_Meters_TO_TI ti_to_m
				join dbo.Hard_Meters hm on ti_to_m.METER_ID = hm.Meter_ID
				where ti.TI_ID = ti_to_m.TI_ID and ti_to_m.StartDateTime < @dt and (ti_to_m.FinishDateTime is null or ti_to_m.FinishDateTime > @dt)
				order by ti_to_m.StartDateTime desc
			) hm
			outer apply
			(
				select top (1) * from dbo.Info_Transformators
				where TI_ID = ti.TI_ID and ( FinishDateTime is null or (FinishDateTime is not null and FinishDateTime >= @dt))
				order by StartDateTime desc
			) it
			left join Hard_OV_List ov on ov.OV_ID = ti.TI_ID
			where ti.Deleted = 0
	END ELSE BEGIN --Фильтруем по ПС							
		select  distinct ti.TI_ID as ID,
				ti.TIName as StringName,
				ti.PS_ID as P_ID, 
				ti.TIType,
				ti.Commercial,
				ti.Voltage,
				ti.SectionNumber,
				Cast (0 as bit) as isCa,
				ISNULL(ti.TPCoefOurSide,1) as TPCoef,
				cast (ISNULL(it.COEFI * it.COEFU, 1) as float(26)) as Coeff,
				cast((case when ti.AIATSCode=2 then 1 else 0 end) as bit) as IsChanelReverse,
				ti.TP_ID,0 as ContrObject_ID,
				ti.Deleted,
				ti.PhaseNumber,
				dbo.usf2_Info_GetTariffsForTI(ti.TI_ID) as TariffArray,
				ti.CustomerKind,
				ti.AbsentChannelsMask,
				hm.MeterModel_ID,
				case when hm.MeterType_ID = 2012 then dbo.usf2_Hard_Meters_Addx_RemoteDisplay(hm.MeterSerialNumber) 
				else hm.MeterSerialNumber end as MeterSerialNumber,
				hm.AllowTariffWrite,
				ti.IsSmallTI,
				ti.MeasureQuantityType_UN,
				ti.TreeCategory,
				cast(case when ov.OV_ID is not null then 1 else 0 end as bit) as IsOv
			from @PSList us
			join Info_TI ti on ti.PS_ID = us.Id
			outer apply 
			(
				select top (1) hm.* from dbo.Info_Meters_TO_TI ti_to_m
				join dbo.Hard_Meters hm on ti_to_m.METER_ID = hm.Meter_ID
				where ti.TI_ID = ti_to_m.TI_ID and ti_to_m.StartDateTime < @dt and (ti_to_m.FinishDateTime is null or ti_to_m.FinishDateTime > @dt)
				order by ti_to_m.StartDateTime desc
			) hm
			outer apply
			(
				select top (1) * from dbo.Info_Transformators
				where TI_ID = ti.TI_ID and ( FinishDateTime is null or (FinishDateTime is not null and FinishDateTime >= @dt))
				order by StartDateTime desc
			) it
			left join Hard_OV_List ov on ov.OV_ID = ti.TI_ID
			where ti.Deleted = 0
	END
end
go
   grant EXECUTE on usp2_Info_GetTIorContrTIForPS to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2013
--
-- Описание:
--
--		Выборка аварий
--
-- ======================================================================================
create proc [dbo].[usp2_Alarms_ReadTopNumbers]

	@topNumbers int,
	@Confirmed bit,
	@USER_ID ABS_NUMBER_TYPE_2,

	@AlarmDateTimeStart DateTime = null,
	@AlarmDateTimeFinish DateTime = null,
	@EventDateTimeStart DateTime = null,
	@EventDateTimeFinish DateTime = null,

	@AlarmSeverityIdArray IntType readonly,
	@WorkflowActivityIdArray IntType readonly
	
as
declare
@isAlarmSeverityExists bit,
@isWorkflowActivityExists bit

begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

set @EventDateTimeStart = ISNULL(@EventDateTimeStart, '20120101');
set @EventDateTimeFinish = ISNULL(@EventDateTimeFinish, getDate());

if (exists(select top 1 1 from @AlarmSeverityIdArray)) set @isAlarmSeverityExists = 1;
else set @isAlarmSeverityExists = 0;

if (exists(select top 1 1 from @WorkflowActivityIdArray)) set @isWorkflowActivityExists = 1;
else set @isWorkflowActivityExists = 0;

select top (@topNumbers) AlarmDescription, AlarmMessage, w.StringName as WorkFlowActivityName, AlarmSeverity, 
atti.TI_ID, atps.PS_ID,atbps.BalancePS_UN,AlarmDateTime,EventDateTime,a.Alarm_ID, atf.Formula_UN, atm.Slave61968System_ID 
into #result
from Alarms_Archive a
join Workflow_Activity_List w on w.WorkflowActivity_ID = a.WorkflowActivity_ID 
left join Alarms_Archive_To_TI atti on atti.Alarm_ID = a.Alarm_ID 
left join Alarms_Archive_To_PS atps on atps.Alarm_ID = a.Alarm_ID 
left join Alarms_Archive_To_Balance_PS atbps on atbps.Alarm_ID = a.Alarm_ID
left join Alarms_Archive_To_Formula atf on atf.Alarm_ID = a.Alarm_ID 
left join Alarms_Archive_To_Master61968_SlaveSystems atm on atm.Alarm_ID = a.Alarm_ID 
where a.[USER_ID] = @USER_ID and a.Confirmed = @Confirmed
and (@Confirmed = 1 or (@Confirmed = 0 and a.Alarm_ID in (select Alarm_ID from Alarms_Current)))
and 
((ISNULL(a.AlarmDateTime, a.EventDateTime)>=@EventDateTimeStart and ISNULL(a.AlarmDateTime, a.EventDateTime)<=@EventDateTimeFinish) OR
(a.EventDateTime>=@EventDateTimeStart and a.EventDateTime<=@EventDateTimeFinish))
and (@isAlarmSeverityExists=0 or (@isAlarmSeverityExists = 1 and AlarmSeverity in (select id from @AlarmSeverityIdArray)))
and (@isWorkflowActivityExists=0 or (@isWorkflowActivityExists = 1 and a.WorkflowActivity_ID in (select id from @WorkflowActivityIdArray)))
order by  EventDateTime desc

select * from #result;

--Точки измерения
select ti.TI_ID as ID, h1.StringName + '\' + h2.StringName + '\' + ps.StringName +'\' + TIName as FullHierarchy 
from Info_TI ti join Dict_PS ps on ps.PS_ID = ti.PS_ID 
join Dict_HierLev3 h3 on h3.HierLev3_ID = ps.HierLev3_ID
join Dict_HierLev2 h2 on h2.HierLev2_ID = h3.HierLev2_ID
join Dict_HierLev1 h1 on h1.HierLev1_ID = h2.HierLev1_ID where ti_id in (select distinct TI_ID from #result)

--Подстанции
select ps.PS_ID as ID, h1.StringName + '\' + h2.StringName + '\' + h3.StringName +'\' + ps.StringName as FullHierarchy 
from Dict_PS ps 
join Dict_HierLev3 h3 on h3.HierLev3_ID = ps.HierLev3_ID 
join Dict_HierLev2 h2 on h2.HierLev2_ID = h3.HierLev2_ID 
join Dict_HierLev1 h1 on h1.HierLev1_ID = h2.HierLev1_ID where ps_id in (select distinct PS_ID from #result)

--Формулы
select Formula_UN as ID, FormulaName as Name 
from Info_Formula_List where Formula_UN in (select distinct Formula_UN from #result)

--Баланс
select BalancePS_UN as ID, BalancePSName as Name 
from Info_Balance_PS_List_2 where BalancePS_UN in (select distinct BalancePS_UN from #result)

--61968
select Slave61968System_ID as ID, StringName as FullHierarchy 
from Master61968_SlaveSystems where Slave61968System_ID in (select distinct Slave61968System_ID from #result)

--Очень долго выполняется
--Общее количество тревог для данного пользователя
select Count(*) as [count] from Alarms_Archive a
join Workflow_Activity_List w on w.WorkflowActivity_ID = a.WorkflowActivity_ID 
where a.[USER_ID] = @USER_ID and a.Confirmed = @Confirmed
and (@Confirmed = 1 or (@Confirmed = 0 and a.Alarm_ID in (select Alarm_ID from Alarms_Current)))
and a.EventDateTime>=ISNULL(@EventDateTimeStart, '20120101')
and a.EventDateTime<=ISNULL(@EventDateTimeFinish, GetDate())
end;
go
   grant EXECUTE on usp2_Alarms_ReadTopNumbers to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2011
--
-- Описание:
--
--		Запрашиваем ТИ и возвращаем запрошенные ТИ + ТИ которые рядом на одной ПС
--
-- ======================================================================================
CREATE proc [dbo].[usp2_Info_GetTIandNearestTI]
(
	@TIList IntType READONLY	--Список ТИ которые выбираем 
)
as
begin
		set nocount on
		set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
		set numeric_roundabort off
		set transaction isolation level read uncommitted
		declare @dt DateTime
		set @dt = GETDATE();
		select  distinct ti.TI_ID as ID,
				ti.TIName as StringName,
				ti.PS_ID as P_ID, 
				ti.TIType,
				ti.Commercial,
				ti.Voltage,
				ti.SectionNumber,
				Cast (0 as bit) as isCa,
				ISNULL(ti.TPCoefOurSide,1) as TPCoef,
				cast (ISNULL(it.Coeff,1) as float(26)) as Coeff,
				cast((case when ti.AIATSCode=2 then 1 else 0 end) as bit) as IsChanelReverse,
				ti.TP_ID,0 as ContrObject_ID,
				ti.Deleted,
				ti.PhaseNumber,
				dbo.usf2_Info_GetTariffsForTI(ti.TI_ID) as TariffArray,
				ti.CustomerKind,
				ti.AbsentChannelsMask,
				hm.MeterModel_ID,
				case when hm.MeterType_ID = 2012 then dbo.usf2_Hard_Meters_Addx_RemoteDisplay(hm.MeterSerialNumber) 
				else hm.MeterSerialNumber end as MeterSerialNumber,
				hm.AllowTariffWrite,
				ti.IsSmallTI,
				ti.MeasureQuantityType_UN,
				ti.TreeCategory,
				cast(case when ov.OV_ID is not null then 1 else 0 end as bit) as IsOv
			from 
			(
				select distinct ti.PS_ID from @TIList u
				join Info_TI ti on ti.TI_ID = u.Id
			) us
			join Info_TI ti on ti.PS_ID = us.PS_ID
			outer apply 
			(
				select top (1) hm.MeterModel_ID,hm.MeterSerialNumber, hm.AllowTariffWrite, hm.MeterType_ID from dbo.Info_Meters_TO_TI ti_to_m
				join dbo.Hard_Meters hm on ti_to_m.METER_ID = hm.Meter_ID
				where ti.TI_ID = ti_to_m.TI_ID and ti_to_m.StartDateTime < @dt and (ti_to_m.FinishDateTime is null or ti_to_m.FinishDateTime > @dt)
				order by ti_to_m.StartDateTime desc
			) hm
			outer apply
			(
				select top (1) StartDateTime,TI_ID, COEFU*COEFI as Coeff from Info_Transformators
				where TI_ID = ti.TI_ID
				order by StartDateTime desc
			) it
			left join Hard_OV_List ov on ov.OV_ID = ti.TI_ID
			--where ti.Deleted <> 1
end
go
   grant EXECUTE on usp2_Info_GetTIandNearestTI to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2012
--
-- Описание:
--
--		Выбираем дерево с информацией для анализа мониторнига (все кроме счетчиков)
--
-- ======================================================================================
create proc [dbo].[usp2_Monit_GetTreeInfoForMonitoring]
	@Is61968 bit, @M61968_Array IntType READONLY, @tiArray IntType READONLY, @startDateTime DateTime = null, @finishDateTime DateTime = null
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
	@DateNow DateTime, @isArrayExists bit;

	declare @isTiFilterEnabled bit;
	set @isTiFilterEnabled = case when (select top 1 1 from @tiArray) = 1 then 1 else 0 end
	
	set @DateNow = floor(cast(GetDate() as float));
	if (exists(select top 1 1 from @M61968_Array) or @isTiFilterEnabled = 1) set @isArrayExists = 1 else set @isArrayExists = 0;

if @Is61968 = 1 begin
----Информация по 61968
	if (@isArrayExists = 0) begin
		select s.*, m.EventCode, m.EventDateTime, m.AttachedHardwareState
		,	case when m.EventCode = 1 then
				case when DATEDIFF(day, m.EventDateTime, @DateNow) > 2 then 2 else DATEDIFF(day, m.EventDateTime, @DateNow) end 
			else 0
			end	as EventState 
		from dbo.Master61968_SlaveSystems s
		left join dbo.Monit_Current_State_Master61968_SlaveSystems m on s.Slave61968System_ID = m.Slave61968System_ID and m.EventDateTime = 
		(select Max(EventDateTime) from dbo.Monit_Current_State_Master61968_SlaveSystems where Slave61968System_ID = m.Slave61968System_ID)
	end else begin
		select s.*, m.EventCode, m.EventDateTime, m.AttachedHardwareState
		,	case when m.EventCode = 1 then
				case when DATEDIFF(day, m.EventDateTime, @DateNow) > 2 then 2 else DATEDIFF(day, m.EventDateTime, @DateNow) end 
			else 0
			end	as EventState 
		from dbo.Master61968_SlaveSystems s
		left join dbo.Monit_Current_State_Master61968_SlaveSystems m on s.Slave61968System_ID = m.Slave61968System_ID and m.EventDateTime = 
		(select Max(EventDateTime) from dbo.Monit_Current_State_Master61968_SlaveSystems where Slave61968System_ID = m.Slave61968System_ID)
		where s.Slave61968System_ID in (select Id from @M61968_Array)
	end;
end else begin
----------------------------------------------------
	--Объекты для фильтра ТИ
	create table #filteredTis
	(
		ID int,
		ParrentMonitoringHierarchy tinyint,
		PS_ID int, 
		TI_ID int
	)

	if (@isTiFilterEnabled = 1) begin
		--УСПД
		insert into #filteredTis
		select distinct hml.USPD_ID, 3, cc.PS_ID, TI_ID
		from  @tiArray tis
		join Info_Meters_TO_TI mti on tis.Id = mti.TI_ID
		join Hard_MetersUSPD_Links hml on hml.Meter_ID = mti.METER_ID
		join dbo.Hard_USPDCommChannels_Links tochannel on tochannel.USPD_ID = hml.USPD_ID
		join dbo.Hard_CommChannels cc on cc.CommChannel_ID = tochannel.CommChannel_ID
		where StartDateTime <= @finishDateTime and ISNULL(FinishDateTime, '21000101') >= @startDateTime and cc.PS_ID is not null

		--E422
		insert into #filteredTis
		select distinct hml.E422_ID, 1, cc.PS_ID, mti.TI_ID
		from  @tiArray tis
		join Info_Meters_TO_TI  mti on mti.TI_ID = tis.Id
		join Hard_MetersE422_Links hml on hml.Meter_ID = mti.METER_ID
		join dbo.Hard_E422CommChannels_Links tochannel on tochannel.E422_ID = hml.E422_ID
		join dbo.Hard_CommChannels cc on cc.CommChannel_ID = tochannel.CommChannel_ID
		where StartDateTime <= @finishDateTime and ISNULL(FinishDateTime, '21000101') >= @startDateTime and cc.PS_ID is not null
		--merge @M61968_Array a
		--using (select distinct PS_ID from #filteredTis) f on a.Id = f.PS_ID
		--when not matched then insert (Id)
		--values (PS_ID);

	end

	--Информация по фильтру ТИ
	select * from  #filteredTis order by ParrentMonitoringHierarchy, ID

----Информация по УСПД
	select cc.USPD_ID as ID,cc.USPDIPMain as IPMain, cc.USPDPortMain as PortMain, cc.USPDSerialNumber as SerialNumber
		, cc.PS_ID, CAST(3 as tinyint)as TypeMonitoringHierarchy
		, m.EventCode, m.AttachedHardwareState  
		, archClock.ClockDiff as ClockDiff
		, cc.InstallationPlace
		, cc.USPD_ID
		, cc.USPDType
	into #uspdResult
	from 
	(
		--Фильтр по ПС
		select c.CommChannel_ID, c.PS_ID, h.USPD_ID,USPDIPMain,USPDPortMain,USPDSerialNumber, InstallationPlace, USPDType
		from @M61968_Array a
		join dbo.Hard_CommChannels c on c.PS_ID = a.Id
		join dbo.Hard_USPDCommChannels_Links tochannel on c.CommChannel_ID = tochannel.CommChannel_ID
		join dbo.Hard_USPD h on tochannel.USPD_ID = h.USPD_ID
		
		union

		--Фильтр по ТИ
		select c.CommChannel_ID, c.PS_ID, h.USPD_ID,USPDIPMain,USPDPortMain,USPDSerialNumber, InstallationPlace, USPDType 
		from #filteredTis t
		join dbo.Hard_USPD h on h.USPD_ID = t.ID
		join dbo.Hard_USPDCommChannels_Links tochannel on h.USPD_ID = tochannel.USPD_ID
		join dbo.Hard_CommChannels c on c.CommChannel_ID = tochannel.CommChannel_ID
		where t.ParrentMonitoringHierarchy = 3

		--(select distinct PS_ID from #filteredTis where ParrentMonitoringHierarchy = 3)
	) cc 
		
	left join  dbo.Monit_Current_State_USPD m on m.USPD_ID = cc.USPD_ID
	outer apply
	(
		select top (1) dc.* 
		from dbo.ArchComm_ClockDiff_Center_USPD dc
		join Hard_USPD hu on hu.USPD_ID = dc.USPD_ID
		where hu.USPD_ID = cc.USPD_ID and hu.USPDType not in (13,14,15,16,18,20,26)
		order by EventDateTime desc
	) archClock
	order by cc.CommChannel_ID --Упорядочниваем по родителям
	
	--Собираем последнее событие из архива событий по всем УСПД, выбираем только одну запись по каждому УСПД
	select distinct a.USPD_ID, a.EventDateTime,a.EventCode
	into #ArchComm_Events_Journal_TempUSPD
	from (select distinct USPD_ID from #uspdResult) r
	cross apply
	(
		select top (1) *
		from dbo.ArchComm_Events_Journal_USPD
		where USPD_ID = r.USPD_ID
		order by EventDateTime desc, EventCode desc
	) a 
		
	--Собираем последнее событие из журнала опроса по всем УСПД, выбираем только одну запись по каждому УСПД
	select a.USPD_ID, a.EventDateTime,a.EventCode,  
	case when DATEDIFF(day, g.EventDateTime, @DateNow) > 1  --null - нет вообще данных, 0 - норма, 1 - успешным данным больше суток, но есть другой ответ, 2 - любому опросу > суток
				then 
					case when DATEDIFF(day, a.EventDateTime, @DateNow) > 1 then 2 else 1 end -- Проверяем статус хоть какого то опроса
				else DATEDIFF(day, g.EventDateTime, @DateNow) 
			end as EventState, g.EventDateTime as GoodEventDateTime 
	into #JournalDataCollect_TempUSPD
	from (select distinct USPD_ID from #uspdResult) r
	cross apply
	(
		select top (1) * from dbo.JournalDataCollect_Server_From_USPD  --Журнал
		where USPD_ID = r.USPD_ID and (EventCode=2 or EventCode>3)
		order by EventDateTime desc
	) a
	outer apply --Это максимальная дата с успешным сбором
	(
		select top (1) *
		from dbo.JournalDataCollect_Server_From_USPD 
		where USPD_ID = r.USPD_ID and (EventCode = 2 or EventCode = 20)
		order by EventDateTime desc
	) g 

	select ID,IPMain, PortMain, SerialNumber
		, PS_ID, TypeMonitoringHierarchy
		, r.EventCode, j.EventDateTime, AttachedHardwareState  
		, ClockDiff
		, archEvent.EventCode as EventCodeJournalEvents
		, j.EventCode as EventCodeDataCollect
		, j.EventState
		, r.InstallationPlace, r.USPDType 
	from #uspdResult r
	left join #ArchComm_Events_Journal_TempUSPD archEvent on archEvent.USPD_ID = r.USPD_ID
	left join #JournalDataCollect_TempUSPD j on j.USPD_ID = r.USPD_ID
	
	drop table #uspdResult
	drop table #ArchComm_Events_Journal_TempUSPD
	drop table #JournalDataCollect_TempUSPD	
----------------------------------------------------
	----Информация по Е422
	select distinct cc.E422_ID as ID,cc.E422IPMain as IPMain, cc.E422PortMain as PortMain, cc.E422SerialNumber as SerialNumber
		, cc.PS_ID, CAST(1 as tinyint) as TypeMonitoringHierarchy
		, m.EventCode
		, m.AttachedHardwareState  
		, archClock.ClockDiff as ClockDiff
		, cc.Concentrator_ID
		, cc.E422_ID
		, cc.CommChannel_ID
	into #e422Result
	from  
	(
		--Фильтр по ПС
		select c.CommChannel_ID, c.PS_ID, h.E422_ID,E422IPMain,E422PortMain,E422SerialNumber,Concentrator_ID
		from @M61968_Array a
		join dbo.Hard_CommChannels c on c.PS_ID = a.Id
		join dbo.Hard_E422CommChannels_Links tochannel on c.CommChannel_ID = tochannel.CommChannel_ID
		join Hard_MetersE422_Links hml on tochannel.E422_ID = hml.E422_ID
		join dbo.Hard_E422 h on tochannel.E422_ID = h.E422_ID
		
		union

		--Фильтр по ТИ
		select c.CommChannel_ID, c.PS_ID, h.E422_ID,E422IPMain,E422PortMain,E422SerialNumber,Concentrator_ID
		from #filteredTis t
		join dbo.Hard_E422 h on h.E422_ID = t.ID
		join dbo.Hard_E422CommChannels_Links tochannel on h.E422_ID = tochannel.E422_ID
		join dbo.Hard_CommChannels c on c.CommChannel_ID = tochannel.CommChannel_ID
		join Hard_MetersE422_Links hml on tochannel.E422_ID = hml.E422_ID
		where t.ParrentMonitoringHierarchy = 1

		--(select distinct PS_ID from #filteredTis where ParrentMonitoringHierarchy = 1)
	)cc 
	left join  dbo.Monit_Current_State_E422 m on m.E422_ID = cc.E422_ID
	outer apply
	(
		select top (1) * 
		from dbo.ArchComm_ClockDiff_Center_E422 
		where E422_ID = cc.E422_ID
		order by EventDateTime desc
	) archClock
	order by cc.CommChannel_ID --Упорядочниваем по родителям
	
	--Собираем последнее событие из архива событий по всем E422, выбираем только одну запись по каждому E422
	select a.E422_ID, a.EventDateTime, a.EventCode
	into #ArchComm_Events_Journal_TempE422
	from (select distinct E422_ID from #e422Result) r
	cross apply
	(
		select top (1) *
		from dbo.ArchComm_Events_Journal_E422
		where E422_ID = r.E422_ID
		order by EventDateTime desc, EventCode desc
	) a 
	
	--Собираем последнее событие из журнала опроса по всем E422, выбираем только одну запись по каждому E422
	select a.E422_ID, a.EventDateTime,a.EventCode, 
	case when DATEDIFF(day, g.EventDateTime, @DateNow) > 1  --null - нет вообще данных, 0 - норма, 1 - успешным данным больше суток, но есть другой ответ, 2 - любому опросу > суток
				then 
					case when DATEDIFF(day, a.EventDateTime, @DateNow) > 1 then 2 else 1 end -- Проверяем статус хоть какого то опроса
				else DATEDIFF(day, g.EventDateTime, @DateNow) 
			end as EventState, g.EventDateTime as GoodEventDateTime  
	into #JournalDataCollect_TempE422
	from (select distinct E422_ID from #e422Result) r
	cross apply
	(
		select top (1) * from dbo.JournalDataCollect_Server_From_E422  --Журнал
		where E422_ID = r.E422_ID and (EventCode=2 or EventCode>3)
		order by EventDateTime desc
	) a
	outer apply --Это максимальная дата с успешным сбором
	(
		select top (1) *
		from dbo.JournalDataCollect_Server_From_E422 
		where E422_ID = r.E422_ID and (EventCode = 2 or EventCode = 20)
		order by EventDateTime desc
	) g 


	select distinct ID, IPMain, PortMain, SerialNumber
		, PS_ID, TypeMonitoringHierarchy
		, archEvent.EventCode as EventCodeJournalEvents
		, j.EventDateTime, AttachedHardwareState  
		, ClockDiff
		, j.EventCode as EventCodeDataCollect
		, j.EventState
		, j.GoodEventDateTime from #e422Result r
	left join #ArchComm_Events_Journal_TempE422 archEvent on archEvent.E422_ID = r.E422_ID
	left join #JournalDataCollect_TempE422 j on j.E422_ID = r.E422_ID	
	--order by CommChannel_ID --Упорядочниваем по родителям

	drop table #ArchComm_Events_Journal_TempE422
	drop table #JournalDataCollect_TempE422
----------------------------------------------------
--Информация по концентратору
	--Собираем последнее событие из журнала опроса по всем концентраторам, выбираем только одну запись по каждому концентратору
	select a.Concentrator_ID, a.EventDateTime, EventCode, case when DATEDIFF(day, a.EventDateTime, @DateNow) > 2 then 2 else DATEDIFF(day, a.EventDateTime, @DateNow) end as EventState 
	into #JournalDataCollect_TempConcentrator
	from (select distinct Concentrator_ID from #e422Result where Concentrator_ID is not null) c
	cross apply 
	(
		select top (1) *
		from dbo.JournalDataCollect_E422_From_Concentrators
		where Concentrator_ID = c.Concentrator_ID
		order by EventDateTime desc
	) a 
	
	select h.*, m.EventCode, m.EventDateTime, j.EventCode as EventCodeDataCollect, j.EventState from dbo.Hard_Concentrators h
	left join dbo.Monit_Current_State_Concentrators m on m.Concentrator_ID = h.Concentrator_ID
	left join #JournalDataCollect_TempConcentrator j on j.Concentrator_ID = h.Concentrator_ID
	where h.Concentrator_ID in (select distinct Concentrator_ID from #e422Result where Concentrator_ID is not null)
	order by h.E422_ID --Упорядочниваем по родителям
	
	drop table #e422Result
	drop table #JournalDataCollect_TempConcentrator;
	drop table #filteredTis;
end;
--На всякий сбрасываем кэш у сервера
--DBCC FREEPROCCACHE
end

go
   grant EXECUTE on usp2_Monit_GetTreeInfoForMonitoring to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2012
--
-- Описание:
--
--		Выбирает журналы событий по группе точек
--
-- ======================================================================================
create proc [dbo].[usp2_ArchCommGetAllTIGroupinJournals]
	@Tis IntType READONLY, --Идентификаторы ТИ
	@EventCodes  IntType READONLY, -- Фильтр событий
	@DateStart datetime,
	@DateEnd datetime,
	@isReadLastEvents bit = 1
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@tableName varchar(255),
@titype tinyint

declare @isEvensFilterEnabled bit;
set @isEvensFilterEnabled = case when exists(select top 1 1 from @EventCodes) then 1 else 0 end;

create table #result(
	[TI_ID] int NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[EventCode] [int] NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[ExtendedEventCode] [bigint] NULL,
	[CUS_ID] tinyint NOT NULL,
	[Event61968Domain_ID] tinyint NULL,
	[Event61968DomainPart_ID] tinyint NULL,
	[Event61968Type_ID] tinyint NULL,
	[Event61968Index_ID] int NULL,
	[Event61968Param] [varchar](255) NULL,
)

--Таблица с последними состояниями
create table #lastStates(
	[TI_ID] int NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[EventCode] [int] NOT NULL,
	[Event61968Domain_ID] tinyint NULL,
	[Event61968DomainPart_ID] tinyint NULL,
	[Event61968Type_ID] tinyint NULL,
	[Event61968Index_ID] int NULL,
	[CUS_ID] tinyint NOT NULL
)

create table #tiList
(
 TI_ID int,
 TIType tinyint,
 PRIMARY KEY CLUSTERED (tiType, TI_ID)
);

insert into #tiList
select TI_ID, TIType 
from Info_TI ti
join @Tis usf on ti.TI_ID = usf.Id
order by TIType;

--select * from #tiList;

DECLARE @ParmDefinition NVARCHAR(1000);
SET @ParmDefinition = N'@titype tinyint,@DateStart datetime,@DateEnd datetime, @EventCodes IntType READONLY'
DECLARE @SQLString NVARCHAR(4000);

declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #tiList
  open t;
	FETCH NEXT FROM t into @titype
	WHILE @@FETCH_STATUS = 0
	BEGIN
	IF @titype>10 BEGIN
			set @tableName = 'dbo.ArchBit_Events_Journal_' + + ltrim(str(@TIType - 10,2));
		END ELSE BEGIN
			set @tableName = 'dbo.ArchComm_Events_Journal_TI';
		END;
		
		SET @SQLString = 'insert #result([TI_ID]
           ,[EventDateTime]
           ,[EventCode]
           ,[DispatchDateTime]
           ,[ExtendedEventCode]
           ,[CUS_ID]
           ,[Event61968Domain_ID]
           ,[Event61968DomainPart_ID]
           ,[Event61968Type_ID]
           ,[Event61968Index_ID]
           ,[Event61968Param])
		select [TI_ID]
           ,[EventDateTime]
           ,[EventCode]
           ,[DispatchDateTime]
           ,[ExtendedEventCode]
           ,[CUS_ID]
           ,[Event61968Domain_ID]
           ,[Event61968DomainPart_ID]
           ,[Event61968Type_ID]
           ,[Event61968Index_ID]
           ,[Event61968Param]
		from ' + @tableName + ' arch  WITH(NoLock) 
		where arch.TI_ID in (select TI_ID from #tiList where TIType = @titype) AND EventDateTime between @DateStart and @DateEnd and arch.EventCode >=0' +
		case when @isEvensFilterEnabled = 1
		then ' and dbo.usf2_Event61968_ToLowLevelCode(arch.EventCode, arch.Event61968Domain_ID, arch.Event61968DomainPart_ID, arch.Event61968Type_ID, arch.Event61968Index_ID) in (select Id from @EventCodes)' else '' end
		
		EXEC sp_executesql @SQLString, @ParmDefinition, @titype, @DateStart , @DateEnd, @EventCodes ;
		
		if (@isReadLastEvents = 1) begin
			SET @SQLString = 'insert #lastStates([TI_ID]
			,[EventDateTime]
			,[EventCode]
			,[Event61968Domain_ID]
			,[Event61968DomainPart_ID]
			,[Event61968Index_ID]
			,[Event61968Type_ID]
			,[CUS_ID]
			)
			select distinct arch.TI_ID, arch.EventDateTime, dbo.usf2_Event61968_ToLowLevelCode(arch.EventCode, arch.Event61968Domain_ID, arch.Event61968DomainPart_ID, arch.Event61968Type_ID, arch.Event61968Index_ID) as EventCode, arch.Event61968Domain_ID, arch.Event61968DomainPart_ID, arch.Event61968Index_ID, arch.Event61968Type_ID, 0 from 
			#tiList m
			cross apply (
				select top (1) *
				from ' + @tableName + ' t
				where t.TI_ID=m.TI_ID and t.EventCode >=0
				order by EventDateTime desc
			) arch where m.TIType = @titype '

			EXEC sp_executesql @SQLString, @ParmDefinition, @titype, @DateStart, @DateEnd, @EventCodes;
		end;

	FETCH NEXT FROM t into @titype
	end;
	CLOSE t
	DEALLOCATE t
	
	select * from #result
	order by TI_ID, EventDateTime;
	
	if (@isReadLastEvents = 1) begin
		select * from #lastStates
		order by TI_ID, EventDateTime;
	end

	drop table #result
	drop table #lastStates
	drop table #tiList
end
go
   grant EXECUTE on usp2_ArchCommGetAllTIGroupinJournals to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2014
--
-- Описание:
--
--		Данные по понижающим трансформаторам для составленя формулы потерь по ТП
--
-- ======================================================================================
create proc [dbo].[usp2_Info_GetTransfomatorsPropertyForLostFormulas]
(	
	@tis inttype readonly
)

AS
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

	declare @dt DateTime;
	set @dt = GETDATE();

	--Все ТИ соотносящиеся с этими трансформаторами
	select tp.* 
	into #tis
	from @tis ti
	join Info_TI_To_PTransformators tp on tp.TI_ID = ti.Id;

	select * from #tis

	--Характеристики всех трансформаторов
	select ht.PTransformator_ID, PTransformatorName, RatedPower, 
			ShortCircuitLosses, ShortCircuitLossesMV, ShortCircuitLossesHV, IdlingLosses, TypeInstallationMeters,
			 IsNull(il.CoeffAverVoltageToNominal,cast(1 as float)) as CoeffAverVoltageToNominal, Voltage
	from dbo.Hard_PTransformators ht
	left join Info_PTransformator_IdlingLossesVoltageCoeff il on ht.PTransformator_ID = il.PTransformator_ID
	and il.StartDateTime = 
		(
			select max(Info_PTransformator_IdlingLossesVoltageCoeff.StartDateTime)
					from Info_PTransformator_IdlingLossesVoltageCoeff
					where Info_PTransformator_IdlingLossesVoltageCoeff.PTransformator_ID = ht.PTransformator_ID
						and Info_PTransformator_IdlingLossesVoltageCoeff.StartDateTime <= @dt
						and Info_PTransformator_IdlingLossesVoltageCoeff.FinishDateTime >= @dt

		)
	where ht.PTransformator_ID in (select distinct PTransformator_ID from #tis)

end
go
	grant EXECUTE on usp2_Info_GetTransfomatorsPropertyForLostFormulas to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2014
--
-- Описание:
--
--		Проверка прав на группу точек, возвращаем идентификаторы ТИ на которые нет прав, или право явно запрещено
--
-- ======================================================================================
create proc [dbo].[usp2_UserHasRightForTis]

	@userId varchar(22),
	@rightId uniqueidentifier,
	@tis IntType READONLY
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Проверяем на администратора
if (select top 1 [UserRole] from dbo.Expl_Users where [USER_ID] = @userId) = 1 return;

--Формируем стандартное дерево
create table #tree
(
	lev smallint,
	ObjectTypeName varchar(255) collate Cyrillic_General_CI_AS,
	Id int,
	TI_ID int
)

--Сначала наполняем стандартное дерево
insert into #tree (TI_ID, lev, ObjectTypeName, Id)
select TI_ID, 
case ObjectTypeName
	when 'lev1' then 0 
	when 'lev2' then 1
	when 'lev3' then 2
	when 'lev4' then 3
end as lev,
case ObjectTypeName
	when 'lev1' then 'Dict_HierLev1_' 
	when 'lev2' then 'Dict_HierLev2_' 
	when 'lev3' then 'Dict_HierLev3' 
	when 'lev4' then 'Dict_PS_' 
end as ObjectTypeName
, Id  from 
(
--Цепочка от ПС до HierLev1
select t.PS_ID as lev4, h3.HierLev3_ID as lev3, cast(h2.HierLev2_ID as int) as lev2, cast(h1.HierLev1_ID as int) as lev1, TI_ID  
from Info_TI t
join Dict_PS ps on ps.PS_ID = t.PS_ID
join Dict_HierLev3 h3 on h3.HierLev3_ID = ps.HierLev3_ID
join Dict_HierLev2 h2 on h2.HierLev2_ID = h3.HierLev2_ID
join Dict_HierLev1 h1 on h1.HierLev1_ID = h2.HierLev1_ID
where t.TI_ID in (select Id from @tis)
) as SourceTable
unpivot
(
--Разворачивваем цепочку 
	Id FOR ObjectTypeName IN 
      (lev1, lev2, lev3, lev4)
) PivotTable

--Это раскрутка дерева свободной иерархии
;with freeHier(HierID, ParentHierID, FreeHierItem_ID, FreeHierTree_ID, FreeHierItemType, StringName, TI_ID) as
(
	select f.HierID, f.HierID.GetAncestor(1), f.FreeHierItem_ID, f.FreeHierTree_ID, f.FreeHierItemType, 
	f.StringName, d.TI_ID
	from Dict_FreeHierarchyTree_Description d
	join Dict_FreeHierarchyTree f on f.FreeHierItem_ID = d.FreeHierItem_ID
	where d.TI_ID in (select Id from @tis)
	--Рекурсия
	union all
	select f.HierID, f.HierID.GetAncestor(1), f.FreeHierItem_ID, f.FreeHierTree_ID, f.FreeHierItemType, f.StringName, r.TI_ID
	from Dict_FreeHierarchyTree f
	join freeHier r on r.ParentHierID = f.HierID and r.FreeHierTree_ID = f.FreeHierTree_ID
)

--Дерево ТИ в свободной иерархии
insert into #tree
select distinct f.HierID.GetLevel() lev,
case FreeHierItemType
	when 0 then 'Dict_FreeHierarchyTree'
	when 1 then 'Dict_HierLev1_'
	when 2 then 'Dict_HierLev2_'
	when 3 then 'Dict_HierLev3'
	when 4 then 'Dict_PS_'
	when 9 then 'Hard_USPD'
	--when 5 then 'Info_TI'
	--when 6 then d.Formula_UN
	when 7 then 'Info_Section_List'
	--when 8 then d.TP_ID
	when 10 then 'Dict_JuridicalPersons_Contracts'
	when 12 then 'Expl_XML_System_List'
	when 13 then 'Dict_JuridicalPersons'
	when 18 then 'Dict_DistributingArrangement'
	when 19 then 'Dict_BusSystem'
	when 23 then 'UANode'
end as  ObjectTypeName, 
case FreeHierItemType 
	when 0 then d.FreeHierItem_ID
	when 1 then d.HierLev1_ID
	when 2 then d.HierLev2_ID
	when 3 then d.HierLev3_ID
	when 4 then  d.PS_ID
	when 9 then d.USPD_ID
	--when 5 then d.TI_ID
	--when 6 then d.Formula_UN
	when 7 then d.Section_ID
	--when 8 then d.TP_ID
	when 10 then d.JuridicalPersonContract_ID
	when 12 then d.XMLSystem_ID
	when 13 then d.JuridicalPerson_ID
	when 18 then d.DistributingArrangement_ID
	when 19 then d.BusSystem_ID
	when 23 then d.UANode_ID
end as Id --Идентификатор
,f.TI_ID
from freeHier f
join Dict_FreeHierarchyTree_Description d on d.FreeHierItem_ID = f.FreeHierItem_ID
where d.TI_ID is null;

--select * from #tree
declare @adminRight uniqueidentifier;
set @adminRight = 'F7E018EE-C70B-4094-86F8-504057E7AF44';

--Права
select t.ObjectTypeName, t.Id, r.IsAssent 
into #rights
from (select distinct ObjectTypeName, Id from #tree) t
join Expl_Users_DBObjects o on o.ObjectTypeName = t.ObjectTypeName and o.[Object_ID] = t.Id
join Expl_UserGroup_Right r on DBObject_ID = o.ID
join Expl_User_UserGroup u on u.UserGroup_ID = r.UserGroup_ID 
where u.[User_ID] = @userId and (RIGHT_ID = @rightId or RIGHT_ID = @adminRight)

--Возващаем объекты на которые не найдены права, или права запрещены явно
select cast(id as int) as TI_ID from @tis
except
--Отсеиваем ТИ на которые права найдены
select tr.TI_ID from 
(select distinct TI_ID from #tree) tr
cross apply
(
	--Сортируем по  типу разрешения, запрет приоритетен
	select top 1 TI_ID, IsAssent 
	from #tree t
	join #rights r on r.Id = t.Id and r.ObjectTypeName = t.ObjectTypeName
	where t.TI_ID = tr.TI_ID
	order by IsAssent
) c where IsAssent = 1 --Выбираем только разрешенные права без явного запрета

drop table #tree
drop table #rights

end
go
   grant EXECUTE on usp2_UserHasRightForTis to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2015
--
-- Описание:
--
--		Для анализа бизнес модели "Ведомость опроса счетчиков"
--
-- ======================================================================================
create proc [dbo].[usp2_Rep_StatementRequestIntegrals]
(	
	@tis IntType READONLY, --Идентификатор объекта
	@dtStart DateTime, --Дата, время начала
	@dtEnd DateTime = null --Дата, время окончания
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	--declare @SQLString NVARCHAR(4000),@ParmDefinition NVARCHAR(1000)

	--set @ParmDefinition = N'@dtStart DateTime, @dtEnd DateTime,@tis IntType READONLY'

	SELECT ti.TI_ID as Номер_ТИ, ti.TIName as Имя_ТИ, ps.StringName AS ИмяПС,
	h3.StringName as Улица,h2.StringName as Район, h1.StringName as НазваниеУровень1,
	al.BitAbonentCode as Код_Абонента, al.BitAbonentSurname as Фамилия,
	al.BitAbonentName as Имя,
	al.BitAbonentMiddleName as Отчество, tz.ChannelType1 as Тип_Канала_1,
	mtti.MeterSerialNumber as Серийный_номер_счетчика,
	ISNULL(mtti.StartDateTime, '21000101') as Дата_Установки_Счетчика,
	mtti.FinishDateTime as Дата_демонтажа_счетчика,
	al.StartDateTime as Дата_Договора_ЛС,
	ti.TIType as Хранилище, ti.PS_ID as Номер_ПС, ps.PSType as Тип_ПС,
	ti.AIATSCode as Код_Канала_АП,ti.AOATSCode as Код_Канала_АО,ti.RIATSCode as Код_Канала_РП,ti.ROATSCode as Код_Канала_РО,h3.HierLev3_ID as Идентификатор_Уровня_3,
	dbo.usf2_Info_GetTariffChannelsForTIUseChannelReverse(ti.TI_ID, ti.AbsentChannelsMask, case when ti.AIATSCode=2 then 1 else 0 end) as Список_тарифных_каналов,
	--'' as Список_тарифных_каналов,
	--'' as Список_тарифных_каналов,
	Commercial as Коммерческий, ti.Voltage as Напряжение, PhaseNumber as Номер_фазы, CustomerKind as Тип_потребителя,
	mtti.Название_модели_счетчика,
	tz.Tariff_ID as Идентификатор_тарифа, tz.Название_тарифа,
	fa.FullAddress as Полный_адрес,
	mtti.MeterExtendedTypeName as Тип_счетчика_расширенный
	FROM @tis t
	join Info_TI ti on ti.TI_ID = t.id
	JOIN Dict_PS ps ON ti.PS_ID = ps.PS_ID 
	JOIN Dict_HierLev3 h3 ON ps.HierLev3_ID = h3.HierLev3_ID 
	JOIN Dict_HierLev2 h2 ON h3.HierLev2_ID = h2.HierLev2_ID 
	JOIN Dict_HierLev1 h1 ON h2.HierLev1_ID = h1.HierLev1_ID 
	outer apply
	(
	select top 1 Info_Meters_TO_TI.*, Hard_Meters.MeterSerialNumber, m.StringName as Название_модели_счетчика, em.MeterExtendedTypeName
	from Info_Meters_TO_TI 
	JOIN Hard_Meters ON Info_Meters_TO_TI.METER_ID = Hard_Meters.Meter_ID 
	left JOIN Dict_Meters_Model m ON m.MeterModel_ID = Hard_Meters.MeterModel_ID
	left join [dbo].[Dict_Meters_Extended_Types] em ON em.MeterExtendedType_ID = m.MeterExtendedType_ID
	where TI_ID=ti.TI_ID 
	and @dtStart between StartDateTime AND ISNULL(FinishDateTime, '21000101') 
	order by StartDateTime desc 
	)	mtti 
	outer apply 
	(
	select top 1 DictTariffs_Zones.*, t.StringName as Название_тарифа from DictTariffs_ToTI WITH (NOLOCK)
	JOIN DictTariffs_Zones ON DictTariffs_ToTI.Tariff_ID = DictTariffs_Zones.Tariff_ID 
	JOIN DictTariffs_Tariffs t ON t.Tariff_ID = DictTariffs_ToTI.Tariff_ID
	where TI_ID = ti.TI_ID 
	and  @dtStart between DictTariffs_ToTI.StartDateTime AND ISNULL(DictTariffs_ToTI.FinishDateTime, '20790101') 
	 and  @dtStart between DictTariffs_Zones.StartDateTime AND ISNULL(DictTariffs_Zones.FinishDateTime, '20790101') 
	 order by DictTariffs_ToTI.StartDateTime desc 
	) tz 
		
	outer apply
	(
		select top 1 al.*, atti.StartDateTime from InfoBit_Abonents_To_TI atti 
		left JOIN InfoBit_Abonents_List al ON atti.BitAbonent_ID = al.BitAbonent_ID 
		where ti.TI_ID = atti.TI_ID 
		and @dtStart <= ISNULL(FinishDateTime, '21000101') and @dtEnd >= StartDateTime
		order by StartDateTime desc 
	) al

	left join [dbo].[FIAS_FullAddressToHierarchy] fth on (fth.TI_ID is not null and fth.TI_ID = ti.TI_ID) or (fth.PS_ID is not null and fth.PS_ID = ti.PS_ID)
	left join [dbo].[FIAS_FullAddress] fa on fa.AOGUID = fth.AOGUID

	WHERE ISNULL(ti.Deleted, 0) = 0 
	ORDER BY h2.StringName,h3.StringName,ps.StringName,ti.TIName--'
		
	--EXEC sp_executesql @SQLString,@ParmDefinition,@dtStart,@dtEnd, @tis;

end

go
   grant EXECUTE on usp2_Rep_StatementRequestIntegrals to [UserCalcService]
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2015
--
-- Описание:
--
--		События ТИ
--
-- ======================================================================================
create proc [dbo].[usp2_Rep_TiEvents]
(	
	@tis IntType READONLY, --Идентификатор объекта
	@dtStart DateTime, --Дата, время начала
	@dtEnd DateTime = null --Дата, время окончания
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	--Архивы с событиями
	create table #archives(
	[TI_ID] int NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[EventCode] [int] NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[ExtendedEventCode] [bigint] NULL,
	[CUS_ID] tinyint NOT NULL,
	[Event61968Domain_ID] tinyint NULL,
	[Event61968DomainPart_ID] tinyint NULL,
	[Event61968Type_ID] tinyint NULL,
	[Event61968Index_ID] int NULL,
	[Event61968Param] [varchar](255) NULL,
	)	

	declare @EventCodes IntType;

	insert into #archives --(TI_ID, EventDateTime, EventCode, DispatchDateTime, ExtendedEventCode, CUS_ID, Event61968Domain_ID, Event61968DomainPart_ID, 
		--Event61968Type_ID, Event61968Index_ID, Event61968Param)
	exec usp2_ArchCommGetAllTIGroupinJournals @tis, @EventCodes, @dtStart, @dtEnd, 0


	select h1.StringName as НазваниеУровень1, h2.StringName as НазваниеУровень2,h3.StringName as НазваниеУровень3,
    TIName as НазваниеТи, ps.StringName as НазваниеПс, ti.TI_ID as НомерТи, mtti.MeterSerialNumber as СерийныйНомерСчетчика,
    dt.StringName as ТипТи, mt.MeterTypeName as ТипСчетчика,
	a.EventDateTime as ДатаВремя,
	dc.StringName as Сообщение,
	edc.StringName as ДополнительноеСообщение,
	a.DispatchDateTime as ДатаВремяКогдаПолучено,
	a.Event61968Param as ПараметрСобытия
	from @tis t 
    join Info_TI ti on ti.TI_ID = t.Id join Dict_PS ps on ps.PS_ID = ti.PS_ID 
    join Dict_HierLev3 h3 on h3.HierLev3_ID = ps.HierLev3_ID 
    join Dict_HierLev2 h2 on h2.HierLev2_ID = h3.HierLev2_ID 
    join Dict_HierLev1 h1 on h1.HierLev1_ID = h2.HierLev1_ID 
    join Dict_TI_Types dt on dt.TIType = ti.TIType 
    outer apply 
    (select top 1 Info_Meters_TO_TI.*, Hard_Meters.MeterSerialNumber, Hard_Meters.MeterType_ID from 
    Info_Meters_TO_TI JOIN Hard_Meters ON Info_Meters_TO_TI.METER_ID = Hard_Meters.Meter_ID 
    where TI_ID=ti.TI_ID and @dtStart <= ISNULL(FinishDateTime, '21000101') and @dtEnd >= StartDateTime order by StartDateTime desc) mtti 
    left join Dict_Meters_Types mt on mt.MeterType_ID = mtti.MeterType_ID 
	--Архивы
	join #archives a on a.TI_ID = ti.TI_ID
	left join Dict_TI_Journal_Event_Codes dc on dc.EventCode = dbo.usf2_Event61968_ToLowLevelCode(a.EventCode, a.Event61968Domain_ID, a.Event61968DomainPart_ID, a.Event61968Type_ID, a.Event61968Index_ID)
	left join Dict_TI_Journal_ExtendedEvent_Codes edc on edc.ExtendedEventCode = a.ExtendedEventCode
	order by h1.StringName, h2.StringName, h3.StringName, ps.StringName, TIName
end
go
   grant EXECUTE on usp2_Rep_TiEvents to [UserCalcService]
GO

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2018
--
-- Описание:
--
--		Выбираем остатки по списку сечений
--
-- ======================================================================================
create proc [dbo].[usp2_ExplDoc_Residues_Section]
(	
	@sectionIds IntType READONLY, --сечения
	@DateStart DateTime, --Начало
	@IsReadCalculatedValues bit,
	@HalfHoursShiftClientFromServer int = 0 --Смещение количество получасовок между сервером и клиентом для 80020
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	SELECT r.[Section_ID]
      ,r.[EventDate]
      ,r.[DataSource_ID]
      ,r.[HalfHoursShiftFromUTC]
      ,case when @IsReadCalculatedValues = 1 then ISNULL(r.[CAL],r.[VAL]) else r.[VAL] end as VAL
      ,r.[LatestDispatchDateTime]
	  ,r.LatestApplyDateTimeOurSideFormulaList
	  ,r.LatestApplyDateTimeOurSideFormulaListDescription
	  ,r.LatestApplyDateTimeContrFormulaList
	  ,r.LatestApplyDateTimeContrFormulaListDescription
	FROM @sectionIds a
	join [dbo].[ExplDoc_Residues_Section_XML80020] r on r.Section_ID = a.Id
	where r.EventDate = DateAdd(day, -1, floor(cast(DATEADD(minute, -ISNULL(@HalfHoursShiftClientFromServer,0) * 30, @DateStart) as float))) 
	and r.HalfHoursShiftFromUTC = ISNULL(@HalfHoursShiftClientFromServer,0)
END
go
   grant EXECUTE on usp2_ExplDoc_Residues_Section to [UserCalcService]
go