if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetFormulaParams')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetFormulaParams
go

/****** Object:  UserDefinedFunction [dbo].[usf2_Info_GetFormulasNeededForMainFormula]    Script Date: 09/25/2008 12:50:13 ******/
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
--		Декабрь, 2009
--
-- Описание:
--
-- Возвращаем параметры формулы, выбираем параметры первой попавшейся точки в формуле
-- ======================================================================================
create  FUNCTION [dbo].[usf2_Info_GetFormulaParams] (
			@Formula_UN [dbo].[ABS_NUMBER_TYPE_2],
			@datestart datetime,
			@dateend datetime
)	
	RETURNS @tbl TABLE 
(
		[Formula_UN] [dbo].[ABS_NUMBER_TYPE_2],
		[TI_ID] int, --Первая попавшаяся точка в формуле
		[Voltage] float, 
		[Meter_ID] int,
		[MeterSerialNumber] varchar(255),
		[FormulaName] varchar(255)
)
AS
BEGIN
	declare
	@Voltage float,
	@Meter_ID int,
	@MeterSerialNumber varchar(255),
	@TI_ID int, -- идентификатор первой найденой с ТП точкой от которой вытаскиваем параметры
	@FormulaName varchar(255)


	select top 1 @FormulaName = FormulaName
	from dbo.Info_Formula_List
	where Formula_UN = @Formula_UN

	select top 1 @TI_ID=tp.TI_ID,@Voltage=Voltage from
				(
				--Прямые точки
				select  TI_ID, StringNumber	from dbo.Info_Formula_Description fd 
				where fd.Formula_UN = @Formula_UN and not fd.TI_ID is null
				union all
				--Вложенные через формулы
				select  TI_ID, StringNumber	from usf2_Info_GetFormulasNeededForMainFormulaOurSide(@Formula_UN, 0, null) ff
				where not ff.TI_ID is null
				) tp
				inner join Info_TI on Info_TI.TI_ID=tp.TI_ID 
				order by StringNumber

	--Номер и идентификатор счетчика
	select @METER_ID = mt.METER_ID, @MeterSerialNumber = MeterSerialNumber 
	from dbo.Info_Meters_TO_TI mt
	left join  dbo.Hard_Meters hm
	on hm.Meter_ID = mt.Meter_ID
	where  mt.TI_ID = @TI_ID and mt.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_Meters_TO_TI
				where Info_Meters_TO_TI.TI_ID = @TI_ID
					and StartDateTime <= @dateend
					and FinishDateTime > @datestart 
				) and mt.FinishDateTime >= @dateend
	
				
	insert into @tbl
	select @Formula_UN, @TI_ID, @Voltage, @Meter_ID, @MeterSerialNumber, @FormulaName

	RETURN
END
go
   grant select on usf2_Info_GetFormulaParams to [UserCalcService]
go