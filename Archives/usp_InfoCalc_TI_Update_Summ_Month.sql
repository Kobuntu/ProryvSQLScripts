if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InfoCalc_TI_Update_Summ_Month')
          and type in ('P','PC'))
   drop procedure usp2_InfoCalc_TI_Update_Summ_Month
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
--		Февраль, 2010
--
-- Описание:
--
--		Обновление таблицы суммарного расхода за месяц малой ТИ
--
-- ======================================================================================

create proc [dbo].[usp2_InfoCalc_TI_Update_Summ_Month]
	@TI_Array varchar(4000),-- Идентификатор ТИ , время, значения АО и АП разделенные запятой
	@DispatchDateTime DateTime, --Время когда запись была вставлена
	@ChannelType tinyint -- Номер канала
as
begin
--Берем идентификатор ЦУС--------------------------------------------------------
	declare
	@CUS_ID tinyint,  --Номер ЦУС сформировавший эту запись
	@PlanFact tinyint

	set @CUS_ID = (select top 1 CUS_ID from dbo.Expl_Current_CUS)
	set @PlanFact = 1

---Таблица здесь храним пересечение множеств
CREATE TABLE #Tmp(	
		[ti_id] int,
		[EventDate]DateTime,
		[Float1] float,
		[ti_id_old] int,
		[ChannelType] int
		);
	BEGIN TRY  BEGIN TRANSACTION

		insert #Tmp
		select usf.TI_ID, usf.EventDate,usf.Float1, fl.TI_id, @ChannelType 
		from usf2_Utils_iter_floatlist_to_table(@TI_Array) usf
		left join dbo.InfoCalc_TI_Month_Rasxod_Month fl 
		on usf.TI_ID = fl.TI_ID and usf.EventDate = fl.MonthYear and fl.ChannelType = @ChannelType and fl.PlanFact = @PlanFact

	---------------Обновляем таблицу архивов за месяц------------------------------------------------------------	
	---Обновляем поля котрые пересекаются
		Update	dbo.InfoCalc_TI_Month_Rasxod_Month  set InfoCalc_TI_Month_Rasxod_Month.VALMonth = fl.Float1 
		from 
		(
			select * from #Tmp 
			where not ti_id_old is null
		) fl
		where InfoCalc_TI_Month_Rasxod_Month.TI_ID=fl.TI_ID and InfoCalc_TI_Month_Rasxod_Month.ChannelType=fl.ChannelType and InfoCalc_TI_Month_Rasxod_Month.PlanFact = @PlanFact and InfoCalc_TI_Month_Rasxod_Month.MonthYear = fl.EventDate
	---Добавляем поля для которых нет пересечения в соответствующей таблице
		Insert InfoCalc_TI_Month_Rasxod_Month  (TI_ID,ChannelType,PlanFact,MonthYear,VALMonth, CUS_ID,DispatchDateTime)
		select fl.TI_ID, fl.ChannelType, @PlanFact, fl.EventDate, fl.Float1, @CUS_ID,  @DispatchDateTime
		from 
		(
			select * from #Tmp 
			where ti_id_old is null
		) fl

	COMMIT
	END TRY	
	BEGIN CATCH
		drop table #TMP
		IF @@TRANCOUNT > 0 ROLLBACK 
		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 10 
		SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH
end

go
   grant EXECUTE on usp2_InfoCalc_TI_Update_Summ_Month to [UserCalcService]
go