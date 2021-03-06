if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Forecast_GetCharacteristicDay')
          and type in ('P','PC'))
   drop procedure usp2_Forecast_GetCharacteristicDay
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
--		Март, 2017
--
-- Описание:
--
--		Выборка характерного дня по объекту
--
-- ======================================================================================
create proc [dbo].[usp2_Forecast_GetCharacteristicDay]
	@forecastObjectUn nvarchar(22) --идентификатор объекта
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Сначала смотрим в исключения для данного объекта
declare @forecastCharacteristicDay_UN uniqueidentifier;

set @forecastCharacteristicDay_UN = 
(
	select top 1 [ForecastCharacteristicDay_UN] 
	from [dbo].[Forecast_CharacteristicDays_ObjectsExcludes] 
	where [ForecastObject_UN] = @forecastObjectUn
)

--Нашли исключение для данного объекта
if (@forecastCharacteristicDay_UN is not null) begin 
	select top 1 [ForecastCharacteristicDay_UN],[ByDefaultForAllObjects],[Name],[ApplyDateTime],[User_ID],[HolydayWeightCoeff]
      ,[PLAN_01],[PLAN_02],[PLAN_03],[PLAN_04],[PLAN_05],[PLAN_06],[PLAN_07],[PLAN_08],[PLAN_09],[PLAN_10],
	  [PLAN_11],[PLAN_12],[PLAN_13],[PLAN_14],[PLAN_15],[PLAN_16],[PLAN_17],[PLAN_18],[PLAN_19],[PLAN_20]
      ,[PLAN_21],[PLAN_22],[PLAN_23],[PLAN_24],
	  
	  ISNULL([FACT_01],[PLAN_01]) as FACT_01,
	  ISNULL([FACT_02],[PLAN_02]) as FACT_02,
	  ISNULL([FACT_03],[PLAN_03]) as FACT_03,
	  ISNULL([FACT_04],[PLAN_04]) as FACT_04,
	  ISNULL([FACT_05],[PLAN_05]) as FACT_05,
	  ISNULL([FACT_06],[PLAN_06]) as FACT_06,
	  ISNULL([FACT_07],[PLAN_07]) as FACT_07,
	  ISNULL([FACT_08],[PLAN_08]) as FACT_08,
	  ISNULL([FACT_09],[PLAN_09]) as FACT_09,
	  ISNULL([FACT_10],[PLAN_10]) as FACT_10,
	  ISNULL([FACT_11],[PLAN_11]) as FACT_11,
	  ISNULL([FACT_12],[PLAN_12]) as FACT_12,
	  ISNULL([FACT_13],[PLAN_13]) as FACT_13,
	  ISNULL([FACT_14],[PLAN_14]) as FACT_14,
	  ISNULL([FACT_15],[PLAN_15]) as FACT_15,
	  ISNULL([FACT_16],[PLAN_16]) as FACT_16,
	  ISNULL([FACT_17],[PLAN_17]) as FACT_17,
	  ISNULL([FACT_18],[PLAN_18]) as FACT_18,
	  ISNULL([FACT_19],[PLAN_19]) as FACT_19,
	  ISNULL([FACT_20],[PLAN_20]) as FACT_20,
	  ISNULL([FACT_21],[PLAN_21]) as FACT_21,
	  ISNULL([FACT_22],[PLAN_22]) as FACT_22,
	  ISNULL([FACT_23],[PLAN_23]) as FACT_23,
	  ISNULL([FACT_24],[PLAN_24]) as FACT_24
	  
	  from [dbo].[Forecast_CharacteristicDays] where [ForecastCharacteristicDay_UN] = @forecastCharacteristicDay_UN
end else begin
--Берем набор по умолчанию
	select top 1 [ForecastCharacteristicDay_UN],[ByDefaultForAllObjects],[Name],[ApplyDateTime],[User_ID],[HolydayWeightCoeff]
      ,[PLAN_01],[PLAN_02],[PLAN_03],[PLAN_04],[PLAN_05],[PLAN_06],[PLAN_07],[PLAN_08],[PLAN_09],[PLAN_10],
	  [PLAN_11],[PLAN_12],[PLAN_13],[PLAN_14],[PLAN_15],[PLAN_16],[PLAN_17],[PLAN_18],[PLAN_19],[PLAN_20]
      ,[PLAN_21],[PLAN_22],[PLAN_23],[PLAN_24],
	  
	  ISNULL([FACT_01],[PLAN_01]) as FACT_01,
	  ISNULL([FACT_02],[PLAN_02]) as FACT_02,
	  ISNULL([FACT_03],[PLAN_03]) as FACT_03,
	  ISNULL([FACT_04],[PLAN_04]) as FACT_04,
	  ISNULL([FACT_05],[PLAN_05]) as FACT_05,
	  ISNULL([FACT_06],[PLAN_06]) as FACT_06,
	  ISNULL([FACT_07],[PLAN_07]) as FACT_07,
	  ISNULL([FACT_08],[PLAN_08]) as FACT_08,
	  ISNULL([FACT_09],[PLAN_09]) as FACT_09,
	  ISNULL([FACT_10],[PLAN_10]) as FACT_10,
	  ISNULL([FACT_11],[PLAN_11]) as FACT_11,
	  ISNULL([FACT_12],[PLAN_12]) as FACT_12,
	  ISNULL([FACT_13],[PLAN_13]) as FACT_13,
	  ISNULL([FACT_14],[PLAN_14]) as FACT_14,
	  ISNULL([FACT_15],[PLAN_15]) as FACT_15,
	  ISNULL([FACT_16],[PLAN_16]) as FACT_16,
	  ISNULL([FACT_17],[PLAN_17]) as FACT_17,
	  ISNULL([FACT_18],[PLAN_18]) as FACT_18,
	  ISNULL([FACT_19],[PLAN_19]) as FACT_19,
	  ISNULL([FACT_20],[PLAN_20]) as FACT_20,
	  ISNULL([FACT_21],[PLAN_21]) as FACT_21,
	  ISNULL([FACT_22],[PLAN_22]) as FACT_22,
	  ISNULL([FACT_23],[PLAN_23]) as FACT_23,
	  ISNULL([FACT_24],[PLAN_24]) as FACT_24
	  from [dbo].[Forecast_CharacteristicDays] where [ByDefaultForAllObjects] = 1
end


end
go
   grant EXECUTE on usp2_Forecast_GetCharacteristicDay to [UserCalcService]
go