if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_Get_TP2_PowerParams')
          and type in ('P','PC'))
   drop procedure usp2_Info_Get_TP2_PowerParams
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июнь, 2010
--
-- Описание:
--
--		Параметры «заявленная мощность», «Присоединенная мощность», «Максимальная мощность»
--
-- ======================================================================================

create proc [dbo].[usp2_Info_Get_TP2_PowerParams]
(	
	@ti_id_Array varchar(4000), -- Список ТП
	@datestart datetime,
	@dateend datetime
)
AS
BEGIN 
	SELECT tp_list.TInumber as tp_id, power_param.StartDateTime, power_param.FinishDateTime
			,power_param.AssertedPower
			,power_param.ConnectedPower
			,power_param.MaximumPower 
	from  usf2_Utils_iter_intlist_to_table(@ti_id_Array) tp_list
	left join dbo.Info_TP2_Power power_param
  		on power_param.TP_ID = tp_list.TInumber 
			and power_param.StartDateTime <= @dateend 
			and power_param.FinishDateTime >= @datestart 
	order by tp_list.TInumber, power_param.StartDateTime
END

go
   grant EXECUTE on usp2_Info_Get_TP2_PowerParams to [UserCalcService]
go
