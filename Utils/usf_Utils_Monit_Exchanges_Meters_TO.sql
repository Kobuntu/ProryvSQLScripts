if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_Monit_Exchanges_Meters_TO')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_Monit_Exchanges_Meters_TO
go

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
--		Отслеживаем время действия счетчика ТИ
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_Monit_Exchanges_Meters_TO]
(
	@TI_ID int,
	@DateStart datetime,
	@DateEnd datetime
)
RETURNS TABLE
--RETURNS @tbl TABLE (
--		[TI_ID] int NOT NULL,					--Идентификатор точки
--		[StartDateTime] datetime NULL,			--Период начала 
--		[FinishDateTime] datetime NULL,			--Период окончания
--		[MeterSerialNumber] varchar(255) NULL,   --Серийный номер
--		[MetersReplaceSession_ID] uniqueidentifier,  --Идентификатор сессии замещения
--		[Meter_ID] int,
--		[DigitСapacity] float
--)
AS
RETURN

	--insert @tbl
	select top 100 mt.TI_ID,mt.StartDateTime,mt.FinishDateTime,hm.MeterSerialNumber, mt.MetersReplaceSession_ID, hm.Meter_ID, hm.DigitСapacity 
	from dbo.Info_Meters_TO_TI mt
	left join dbo.Hard_Meters hm on hm.Meter_ID = mt.Meter_ID
	where mt.TI_ID = @TI_ID and mt.StartDateTime <= @DateEnd and mt.FinishDateTime > @DateStart
	order by mt.StartDateTime

go
grant select on usf2_Utils_Monit_Exchanges_Meters_TO to [UserCalcService]
go