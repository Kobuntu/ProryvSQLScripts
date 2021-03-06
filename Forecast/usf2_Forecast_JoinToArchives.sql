if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Forecast_JoinToArchives')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Forecast_JoinToArchives
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
--		Апрель, 2009
--
-- Описание:
--
-- Возврат коэфф трансформации по получасовкам
-- ======================================================================================
create FUNCTION [dbo].[usf2_Forecast_JoinToArchives] (
			@ForecastCalculateModel_ID int,
			@ForecastArchiveJournal_UN uniqueidentifier,
			@ForecastObject_UN varchar(22)
)	
	RETURNS @tbl TABLE 
(
		[ForecastArchiveJournal_UN] [uniqueidentifier] NOT NULL,
	[AUTO_01] [float] NULL,
	[AUTO_02] [float] NULL,
	[AUTO_03] [float] NULL,
	[AUTO_04] [float] NULL,
	[AUTO_05] [float] NULL,
	[AUTO_06] [float] NULL,
	[AUTO_07] [float] NULL,
	[AUTO_08] [float] NULL,
	[AUTO_09] [float] NULL,
	[AUTO_10] [float] NULL,
	[AUTO_11] [float] NULL,
	[AUTO_12] [float] NULL,
	[AUTO_13] [float] NULL,
	[AUTO_14] [float] NULL,
	[AUTO_15] [float] NULL,
	[AUTO_16] [float] NULL,
	[AUTO_17] [float] NULL,
	[AUTO_18] [float] NULL,
	[AUTO_19] [float] NULL,
	[AUTO_20] [float] NULL,
	[AUTO_21] [float] NULL,
	[AUTO_22] [float] NULL,
	[AUTO_23] [float] NULL,
	[AUTO_24] [float] NULL,
	[AUTO_25] [float] NULL,
	[AUTO_26] [float] NULL,
	[AUTO_27] [float] NULL,
	[AUTO_28] [float] NULL,
	[AUTO_29] [float] NULL,
	[AUTO_30] [float] NULL,
	[AUTO_31] [float] NULL,
	[AUTO_32] [float] NULL,
	[AUTO_33] [float] NULL,
	[AUTO_34] [float] NULL,
	[AUTO_35] [float] NULL,
	[AUTO_36] [float] NULL,
	[AUTO_37] [float] NULL,
	[AUTO_38] [float] NULL,
	[AUTO_39] [float] NULL,
	[AUTO_40] [float] NULL,
	[AUTO_41] [float] NULL,
	[AUTO_42] [float] NULL,
	[AUTO_43] [float] NULL,
	[AUTO_44] [float] NULL,
	[AUTO_45] [float] NULL,
	[AUTO_46] [float] NULL,
	[AUTO_47] [float] NULL,
	[AUTO_48] [float] NULL,
	[MANUAL_01] [float]  NULL,
	[MANUAL_02] [float]  NULL,
	[MANUAL_03] [float]  NULL,
	[MANUAL_04] [float]  NULL,
	[MANUAL_05] [float]  NULL,
	[MANUAL_06] [float]  NULL,
	[MANUAL_07] [float]  NULL,
	[MANUAL_08] [float]  NULL,
	[MANUAL_09] [float]  NULL,
	[MANUAL_10] [float]  NULL,
	[MANUAL_11] [float]  NULL,
	[MANUAL_12] [float]  NULL,
	[MANUAL_13] [float]  NULL,
	[MANUAL_14] [float]  NULL,
	[MANUAL_15] [float]  NULL,
	[MANUAL_16] [float]  NULL,
	[MANUAL_17] [float]  NULL,
	[MANUAL_18] [float]  NULL,
	[MANUAL_19] [float]  NULL,
	[MANUAL_20] [float]  NULL,
	[MANUAL_21] [float]  NULL,
	[MANUAL_22] [float]  NULL,
	[MANUAL_23] [float]  NULL,
	[MANUAL_24] [float]  NULL,
	[MANUAL_25] [float]  NULL,
	[MANUAL_26] [float]  NULL,
	[MANUAL_27] [float]  NULL,
	[MANUAL_28] [float]  NULL,
	[MANUAL_29] [float]  NULL,
	[MANUAL_30] [float]  NULL,
	[MANUAL_31] [float]  NULL,
	[MANUAL_32] [float]  NULL,
	[MANUAL_33] [float]  NULL,
	[MANUAL_34] [float]  NULL,
	[MANUAL_35] [float]  NULL,
	[MANUAL_36] [float]  NULL,
	[MANUAL_37] [float]  NULL,
	[MANUAL_38] [float]  NULL,
	[MANUAL_39] [float]  NULL,
	[MANUAL_40] [float]  NULL,
	[MANUAL_41] [float]  NULL,
	[MANUAL_42] [float]  NULL,
	[MANUAL_43] [float]  NULL,
	[MANUAL_44] [float]  NULL,
	[MANUAL_45] [float]  NULL,
	[MANUAL_46] [float]  NULL,
	[MANUAL_47] [float]  NULL,
	[MANUAL_48] [float]  NULL,
	[FACT_01] [float]  NULL,
	[FACT_02] [float]  NULL,
	[FACT_03] [float]  NULL,
	[FACT_04] [float]  NULL,
	[FACT_05] [float]  NULL,
	[FACT_06] [float]  NULL,
	[FACT_07] [float]  NULL,
	[FACT_08] [float]  NULL,
	[FACT_09] [float]  NULL,
	[FACT_10] [float]  NULL,
	[FACT_11] [float]  NULL,
	[FACT_12] [float]  NULL,
	[FACT_13] [float]  NULL,
	[FACT_14] [float]  NULL,
	[FACT_15] [float]  NULL,
	[FACT_16] [float]  NULL,
	[FACT_17] [float]  NULL,
	[FACT_18] [float]  NULL,
	[FACT_19] [float]  NULL,
	[FACT_20] [float]  NULL,
	[FACT_21] [float]  NULL,
	[FACT_22] [float]  NULL,
	[FACT_23] [float]  NULL,
	[FACT_24] [float]  NULL,
	[FACT_25] [float]  NULL,
	[FACT_26] [float]  NULL,
	[FACT_27] [float]  NULL,
	[FACT_28] [float]  NULL,
	[FACT_29] [float]  NULL,
	[FACT_30] [float]  NULL,
	[FACT_31] [float]  NULL,
	[FACT_32] [float]  NULL,
	[FACT_33] [float]  NULL,
	[FACT_34] [float]  NULL,
	[FACT_35] [float]  NULL,
	[FACT_36] [float]  NULL,
	[FACT_37] [float]  NULL,
	[FACT_38] [float]  NULL,
	[FACT_39] [float]  NULL,
	[FACT_40] [float]  NULL,
	[FACT_41] [float]  NULL,
	[FACT_42] [float]  NULL,
	[FACT_43] [float]  NULL,
	[FACT_44] [float]  NULL,
	[FACT_45] [float]  NULL,
	[FACT_46] [float]  NULL,
	[FACT_47] [float]  NULL,
	[FACT_48] [float]  NULL

)
AS
BEGIN

		if (@ForecastCalculateModel_ID is not null and @ForecastArchiveJournal_UN is not null) BEGIN
			--Читаем из таблицы часов работы агрегатов
			if (@ForecastCalculateModel_ID = 1) BEGIN
				insert into @tbl 
				select 	a.ForecastArchiveJournal_UN,
						NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
						NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
						NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
						NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
						NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
						SUM(a.MANUAL_01 * powerValue), SUM(a.MANUAL_01 * powerValue), --Час делим на получасовки
						SUM(a.MANUAL_02 * powerValue), SUM(a.MANUAL_02 * powerValue),
						SUM(a.MANUAL_03 * powerValue), SUM(a.MANUAL_03 * powerValue),
						SUM(a.MANUAL_04 * powerValue), SUM(a.MANUAL_04 * powerValue),
						SUM(a.MANUAL_05 * powerValue), SUM(a.MANUAL_05 * powerValue),
						SUM(a.MANUAL_06 * powerValue), SUM(a.MANUAL_06 * powerValue),
						SUM(a.MANUAL_07 * powerValue), SUM(a.MANUAL_07 * powerValue),
						SUM(a.MANUAL_08 * powerValue), SUM(a.MANUAL_08 * powerValue),
						SUM(a.MANUAL_09 * powerValue), SUM(a.MANUAL_09 * powerValue),
						SUM(a.MANUAL_10 * powerValue), SUM(a.MANUAL_10 * powerValue),
						SUM(a.MANUAL_11 * powerValue), SUM(a.MANUAL_11 * powerValue),
						SUM(a.MANUAL_12 * powerValue), SUM(a.MANUAL_12 * powerValue),
						SUM(a.MANUAL_13 * powerValue), SUM(a.MANUAL_13 * powerValue),
						SUM(a.MANUAL_14 * powerValue), SUM(a.MANUAL_14 * powerValue),
						SUM(a.MANUAL_15 * powerValue), SUM(a.MANUAL_15 * powerValue),
						SUM(a.MANUAL_16 * powerValue), SUM(a.MANUAL_16 * powerValue),
						SUM(a.MANUAL_17 * powerValue), SUM(a.MANUAL_17 * powerValue),
						SUM(a.MANUAL_18 * powerValue), SUM(a.MANUAL_18 * powerValue),
						SUM(a.MANUAL_19 * powerValue), SUM(a.MANUAL_19 * powerValue),
						SUM(a.MANUAL_20 * powerValue), SUM(a.MANUAL_20 * powerValue),
						SUM(a.MANUAL_21 * powerValue), SUM(a.MANUAL_21 * powerValue),
						SUM(a.MANUAL_22 * powerValue), SUM(a.MANUAL_22 * powerValue),
						SUM(a.MANUAL_23 * powerValue), SUM(a.MANUAL_23 * powerValue),
						SUM(a.MANUAL_24 * powerValue), SUM(a.MANUAL_24 * powerValue),

						SUM(a.FACT_01 * powerValue), SUM(a.FACT_01 * powerValue), 
						SUM(a.FACT_02 * powerValue), SUM(a.FACT_02 * powerValue),
						SUM(a.FACT_03 * powerValue), SUM(a.FACT_03 * powerValue),
						SUM(a.FACT_04 * powerValue), SUM(a.FACT_04 * powerValue),
						SUM(a.FACT_05 * powerValue), SUM(a.FACT_05 * powerValue),
						SUM(a.FACT_06 * powerValue), SUM(a.FACT_06 * powerValue),
						SUM(a.FACT_07 * powerValue), SUM(a.FACT_07 * powerValue),
						SUM(a.FACT_08 * powerValue), SUM(a.FACT_08 * powerValue),
						SUM(a.FACT_09 * powerValue), SUM(a.FACT_09 * powerValue),
						SUM(a.FACT_10 * powerValue), SUM(a.FACT_10 * powerValue),
						SUM(a.FACT_11 * powerValue), SUM(a.FACT_11 * powerValue),
						SUM(a.FACT_12 * powerValue), SUM(a.FACT_12 * powerValue),
						SUM(a.FACT_13 * powerValue), SUM(a.FACT_13 * powerValue),
						SUM(a.FACT_14 * powerValue), SUM(a.FACT_14 * powerValue),
						SUM(a.FACT_15 * powerValue), SUM(a.FACT_15 * powerValue),
						SUM(a.FACT_16 * powerValue), SUM(a.FACT_16 * powerValue),
						SUM(a.FACT_17 * powerValue), SUM(a.FACT_17 * powerValue),
						SUM(a.FACT_18 * powerValue), SUM(a.FACT_18 * powerValue),
						SUM(a.FACT_19 * powerValue), SUM(a.FACT_19 * powerValue),
						SUM(a.FACT_20 * powerValue), SUM(a.FACT_20 * powerValue),
						SUM(a.FACT_21 * powerValue), SUM(a.FACT_21 * powerValue),
						SUM(a.FACT_22 * powerValue), SUM(a.FACT_22 * powerValue),
						SUM(a.FACT_23 * powerValue), SUM(a.FACT_23 * powerValue),
						SUM(a.FACT_24 * powerValue), SUM(a.FACT_24 * powerValue)

				from [Forecast_Archive_Data_WorkInHours] a 
				cross apply
				(
					select top 1 PowerValue / 2 * 1000 as PowerValue from [dbo].[Forecast_ObjectTypeMode] o where o.ForecastObjectTypeMode_ID = a.ForecastObjectTypeMode_ID
				) c
				where a.ForecastArchiveJournal_UN = @ForecastArchiveJournal_UN
				group by a.ForecastArchiveJournal_UN 
				

			END ELSE BEGIN
			--Читаем от туда, где лежат получасовки
				insert into @tbl 
				select a.ForecastArchiveJournal_UN,
						a.AUTO_01, a.AUTO_02, a.AUTO_03, a.AUTO_04, a.AUTO_05, a.AUTO_06, a.AUTO_07, a.AUTO_08, a.AUTO_09, a.AUTO_10,
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
						a.FACT_41, a.FACT_42, a.FACT_43, a.FACT_44, a.FACT_45, a.FACT_46, a.FACT_47, a.FACT_48
				from [Forecast_Archive_Data_30] a where a.ForecastArchiveJournal_UN = @ForecastArchiveJournal_UN

			END
		END

		RETURN
END
go
grant select on usf2_Forecast_JoinToArchives to [UserCalcService]
go
