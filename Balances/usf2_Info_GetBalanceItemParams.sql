if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetBalanceItemParams')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetBalanceItemParams
go
/****** Object:  StoredProcedure [dbo].[usp2_Info_GetBalancePSData]    Script Date: 09/25/2008 12:46:17 ******/
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
--		Октябрь, 2015
--
-- Описание:
--
--		Возвращаем название объекта участвующего в балансе
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_GetBalanceItemParams]
(	
	@TI_ID int, @TP_ID int, @Formula_UN varchar(22), 
	@OurFormula_UN varchar(22), @ContrFormula_UN varchar(22), 
	@PTransformator_ID int, @PReactor_ID int, @Section_ID int, @FormulaConstant_UN varchar(22),
	@datestart datetime, --Начальная дата
	@dateend datetime --Конечная дата
)
RETURNS @tbl TABLE 
(
	Name nvarchar(1024),
	MeasuringComplexError float,
	MeterSerialNumber varchar(255),
	Voltage float,
	CoeffTransformation float,
	CoeffLosses float
)
AS
BEGIN

	declare
	@Name nvarchar(1024),
	@MeasuringComplexError float,
	@MeterSerialNumber varchar(255),
	@Voltage float,
	@CoeffTransformation float,
	@CoeffLosses float

	if (@TI_ID is not null) begin 
		select top 1 @Name =TIName,@Voltage = Voltage from Info_TI where TI_ID = @TI_ID
		set @MeterSerialNumber = (select top 1 MeterSerialNumber from Hard_Meters where Meter_ID = 
				(select top 1 Meter_ID from Info_Meters_TO_TI 
				where TI_ID = @TI_ID and StartDateTime <= @dateend and ISNULL(FinishDateTime, '21000101') > @datestart
				order by StartDateTime desc))
		
	end else if (@TP_ID is not null) begin
		set @Name = (select top 1 StringName from Info_TP2 where TP_ID = @TP_ID)
		select top 1 @TI_ID = TI_ID, @Voltage = Voltage, @MeterSerialNumber= MeterSerialNumber 
		from dbo.usf2_Info_GetTPParams(@TP_ID, null, @datestart, @dateend, 1, 0, null)

	end else if (@Formula_UN is not null) begin
		set @Name = (select top 1 FormulaName from Info_Formula_List where Formula_UN = @Formula_UN)
		select top 1 @TI_ID = TI_ID, @Voltage = Voltage, @MeterSerialNumber= MeterSerialNumber 
		from dbo.usf2_Info_GetFormulaParams(@Formula_UN,@datestart, @dateend)

	end else if (@OurFormula_UN is not null) begin 
		--Параметры формулы через ТП
		select top 1 @TP_ID = TP_ID, @Name = FormulaName from Info_TP2_OurSide_Formula_List where Formula_UN = @OurFormula_UN
		select top 1 @TI_ID = TI_ID, @Voltage = Voltage, @MeterSerialNumber= MeterSerialNumber 
		from dbo.usf2_Info_GetTPParams(@TP_ID, null, @datestart, @dateend, 1, 0, null)

	end else if (@ContrFormula_UN is not null) begin
		--Параметры формулы через ТП
		select top 1 @TP_ID = TP_ID, @Name = FormulaName from Info_TP2_Contr_Formula_List where Formula_UN = @ContrFormula_UN
		select top 1 @TI_ID = TI_ID, @Voltage = Voltage, @MeterSerialNumber= MeterSerialNumber 
		from dbo.usf2_Info_GetTPParams(@TP_ID, null, @datestart, @dateend, 1, 0, null)

	end else if (@PTransformator_ID is not null) begin
		set @Name = (select top 1 PTransformatorName from Hard_PTransformators where PTransformator_ID = @PTransformator_ID)
	end else if (@PReactor_ID is not null) begin
		set @Name = (select top 1 PReactorName from Hard_PReactors where PReactor_ID = @PReactor_ID)
	end else if (@Section_ID is not null) begin
		set @Name = (select top 1 SectionName from Info_Section_List where Section_ID = @Section_ID)
	end else if (@FormulaConstant_UN is not null) begin
		set @Name = (select top 1 FormulaConstantName from Info_Formula_Constants where FormulaConstant_UN = @FormulaConstant_UN)
	end

	--Догружаем параметры по первой попавшейся ТИ в объекте
	set @MeasuringComplexError = (select top 1 MeasuringComplexError 
		from Info_MeasuringComplexError
		where TI_ID = @TI_ID and StartDateTime <= @dateend and ISNULL(FinishDateTime, '21000101') > @datestart
		order by StartDateTime desc)

	set @CoeffTransformation = (select top 1 COEFU*COEFI as CoeffTransformation
			from dbo.Info_Transformators 
			where TI_ID = @TI_ID and StartDateTime <= @dateend and ISNULL(FinishDateTime, '21000101') > @datestart
			order by StartDateTime desc)

	set @CoeffLosses = (select top 1 LossesCoefficient as CoeffTransformation
			from dbo.Info_TI_LossesCoefficients 
			where TI_ID = @TI_ID and StartDateTime <= @dateend and ISNULL(FinishDateTime, '21000101') > @datestart
			order by StartDateTime desc)

	insert into @tbl select @Name, @MeasuringComplexError, @MeterSerialNumber, @Voltage, @CoeffTransformation, @CoeffLosses

	RETURN
END
go
   grant select on usf2_Info_GetBalanceItemParams to [UserCalcService]
go