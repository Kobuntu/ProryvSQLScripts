if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ArchComm_GetSmallTIHavingValuesForMonth')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ArchComm_GetSmallTIHavingValuesForMonth
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2009
--
-- Описание:
--
--		Возвращаем список ТИ для которых есть значения в выбранном периоде
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_ArchComm_GetSmallTIHavingValuesForMonth] (
			@DTStart DateTime, --Время проверки начала, Московское зимнее 
			@DTEnd DateTime --время проверки окончания, Московское зимнее
)	
	RETURNS TABLE 
AS
return 
(
	select distinct(ti_id) from
		(
			select DateAdd(minute,(SUBSTRING(ValueRow,5,2)-1) * 30,EventDate) as EventDate, [Val], ti_id, ChannelType  from 
			(
				select EventDate, ChannelType,  [VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10] ,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
				,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30] ,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
				,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48], arh.ti_id from dbo.Info_TI ti
				join dbo.ArchCalc_30_Month_Values  arh on ti.ti_id = arh.ti_id
				where ti.TIType = 2 and
				arh.EventDate between @DTStart and @DTEnd and arh.PlanFact = 1
				
			) as arh
			unpivot 
			([Val] for ValueRow in (
			[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10] ,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
			,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30] ,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
			,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48]
			)) as unpvt
		) a
		where [Val]>0 and EventDate>=@DTStart and EventDate<@DTEnd 
);

go
grant select on usf2_ArchComm_GetSmallTIHavingValuesForMonth to [UserCalcService]
go