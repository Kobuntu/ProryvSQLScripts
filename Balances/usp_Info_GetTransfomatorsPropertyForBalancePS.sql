if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetTransfomatorsPropertyForBalancePS')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetTransfomatorsPropertyForBalancePS
go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь, 2009
--
-- Описание:
--
--		Данные по понижающим трансформаторам для баланса ПС
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetTransfomatorsPropertyForBalancePS]
(	
	@PS_ID int,
	@DTStart DateTime = null,
	@DTEnd DateTime  = null,
	@Transformators varchar(4000) = '-1'
)
AS
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
	if (@DTStart is null) begin 
		declare @currDate DateTime;
		set @currDate = floor(cast(GetDate() as float));
		set @DTStart = DateAdd(day,-DatePart(day,DateAdd(month,-1,@currDate))+1,DateAdd(month,-1,@currDate));
	end
	if (@DTEnd is null) set @DTEnd = DateAdd(minute, -30, DateAdd(month,1,@DTStart));
	create table #transformatorIds
	(
		Items int
	)
	if (@PS_ID is not null) insert into #transformatorIds select PTransformator_ID from Hard_PTransformators 
		where PS_ID = @PS_ID and (StartDateTime is null or StartDateTime <= @DTEnd) and (FinishDateTime is null or FinishDateTime >= @DTStart)
	else insert into #transformatorIds select Items from dbo.usf2_Utils_Split(@Transformators, ',')
	--Характеристики всех трансформаторов на ПС
	select ht.PTransformator_ID, PTransformatorName, RatedPower, 
			ShortCircuitLosses, ShortCircuitLossesMV, ShortCircuitLossesHV, IdlingLosses, TypeInstallationMeters,
			 IsNull(c.CoeffAverVoltageToNominal,cast(1 as float)) as CoeffAverVoltageToNominal, PS_ID, ht.StartDateTime, ht.FinishDateTime
	into #y
	from dbo.Hard_PTransformators ht
	outer apply 
	(
	   select top 1 * from Info_PTransformator_IdlingLossesVoltageCoeff 
	   where PTransformator_ID = ht.PTransformator_ID and StartDateTime <= @DTEnd and ISNULL(FinishDateTime, '21000101') >= @DTStart
	   order by StartDateTime desc
	) c
	where ht.PTransformator_ID in (select Items from #transformatorIds)
	--Время работы трансформаторов
	--Время в этой таблице храним в часовом поясе сервера
	select a.PTransformator_ID, a.StartDateTime, a.FinishDateTime
	from ArchComm_PTransformatorDisabledPeriod a 
	where a.PTransformator_ID in (select Items from #transformatorIds)
		and a.StartDateTime <= @DTEnd and FinishDateTime >= @DTStart
	order by a.PTransformator_ID, a.StartDateTime
	--Характеристики трансформаторов
	select * from #y
	--Все ТИ и ТИ КА соотносящиеся с этими трансформаторами
	select ti_id,cast(0 as bit) as [IsCA], InstallationType, PTransformator_ID
	from dbo.Info_TI_To_PTransformators 
	where PTransformator_ID in (select Items from #transformatorIds)
	union all
	select ContrTI_ID,cast(1 as bit) as [IsCA], InstallationType, PTransformator_ID
	from dbo.Info_Contr_TI_To_PTransformators
	where PTransformator_ID in (select Items from #transformatorIds)
	order by PTransformator_ID
	drop table #transformatorIds
end
go
   grant exec on usp2_Info_GetTransfomatorsPropertyForBalancePS to [UserCalcService]
go