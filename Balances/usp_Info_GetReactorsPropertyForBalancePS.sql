if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetReactorsPropertyForBalancePS')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetReactorsPropertyForBalancePS
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
--		Май, 2010
--
-- Описание:
--
--		Данные по реакторам для баланса ПС
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetReactorsPropertyForBalancePS]
(	
	@PS_ID int,
	@DTStart DateTime = null, 
	@DTEnd DateTime  = null
)

AS
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


	if (@DTStart is null) set @DTStart = DateAdd(day,-DatePart(day,DateAdd(month,-1,floor(cast(GetDate() as float))))+1,DateAdd(month,-1,floor(cast(GetDate() as float))));
	if (@DTEnd is null) set @DTEnd = DateAdd(minute, -30, DateAdd(month,1,@DTStart));

	--Характеристики всех трансформаторов на ПС
	select Hard_PReactors.PReactor_ID, PReactorName, Q, 
			Pyear, Pnom,
			 IsNull(Info_PReactors_IdlingLossesVoltageCoeff.CoeffAverVoltageToNominal,cast(1 as float)) as CoeffAverVoltageToNominal
	into #y
	from dbo.Hard_PReactors
	left join 
	Info_PReactors_IdlingLossesVoltageCoeff on Hard_PReactors.PReactor_ID = Info_PReactors_IdlingLossesVoltageCoeff.PReactor_ID
	and Info_PReactors_IdlingLossesVoltageCoeff.StartDateTime = 
		(
			select max(Info_PReactors_IdlingLossesVoltageCoeff.StartDateTime)
					from Info_PReactors_IdlingLossesVoltageCoeff
					where Info_PReactors_IdlingLossesVoltageCoeff.PReactor_ID = Hard_PReactors.PReactor_ID
						and Info_PReactors_IdlingLossesVoltageCoeff.StartDateTime <= @DTEnd
						and Info_PReactors_IdlingLossesVoltageCoeff.FinishDateTime >= @DTStart

		)
	
	where ps_id = @PS_ID


	--Время работы реакторов
	--Время в этой таблице храним по местному времени
	select ArchComm_PReactorWorkRange.PReactor_ID, ArchComm_PReactorWorkRange.MonthYear, ArchComm_PReactorWorkRange.WorkedHours
	from #y
	join dbo.ArchComm_PReactorWorkRange 
	on #y.PReactor_ID = ArchComm_PReactorWorkRange.PReactor_ID
		and ArchComm_PReactorWorkRange.MonthYear <= @DTEnd
						and DateAdd(mm, 1, ArchComm_PReactorWorkRange.MonthYear) > @DTStart


	--Характеристики реакторов
	select * from #y

	declare	@PReactor_ID int


	--Все ТИ соотносящиеся с реакторами с этой ПС
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select PReactor_ID from #y
	open t;
	FETCH NEXT FROM t into @PReactor_ID
	WHILE @@FETCH_STATUS = 0
	BEGIN

	select ti.ti_id, imce.MeasuringComplexError
	from dbo.Info_TI_To_PReactors ti
	left join dbo.Info_MeasuringComplexError imce
	on	ti.TI_ID = imce.TI_ID and imce.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_MeasuringComplexError
				where Info_MeasuringComplexError.TI_ID = ti.TI_ID
					and StartDateTime <= @DTEnd 
					and FinishDateTime >= @DTStart
				)   and imce.FinishDateTime >= @DTEnd
	where PReactor_ID = @PReactor_ID

	FETCH NEXT FROM t into @PReactor_ID
	end;
	CLOSE t
	DEALLOCATE t

end
go
   grant exec on usp2_Info_GetReactorsPropertyForBalancePS to [UserCalcService]
go