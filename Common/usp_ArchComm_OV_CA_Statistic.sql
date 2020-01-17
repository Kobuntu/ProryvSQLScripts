if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_OV_CA_Statistic')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_OV_CA_Statistic
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
--		Сентябрь, 2009
--
-- Описание:
--
--		Статистика неразнесенных ОВ КА
--
-- ======================================================================================
create proc [dbo].[usp2_ArchComm_OV_CA_Statistic]
	@SummerOrWinter tinyint, --Зимнее (0) или летнее(1) время 
	@OffsetFromMoscow int, --Смещение относительно Москвы в минутах
	@DayMonthYear DateTime, --Дата на которую смотрим
	@Section_ID int = null -- Если сечение не указанно то выбираем все нераспределенные ОВ
							--Если указанно то выбираем только для этого сечения
as
declare
@StartDate DateTime, 
@FinishDate DateTime
begin


set @StartDate = floor(cast(@DayMonthYear as float))
set @FinishDate = DateAdd(day,1,@StartDate)

-- корректируем время на летнее - зимнее и учитываем смещение относительно Москвы
set @StartDate = case when (@SummerOrWinter=1) and (@StartDate >= dbo.usf2_Utils_GetDaylightSavingsTimeStart(@StartDate) and @StartDate < dbo.usf2_Utils_GetDaylightSavingsTimeEnd(@StartDate)) then DateAdd(n,-@OffsetFromMoscow - 60 ,@StartDate) else DateAdd(n,-@OffsetFromMoscow,@StartDate) end
set @FinishDate = case when (@SummerOrWinter=1) and (@FinishDate >= dbo.usf2_Utils_GetDaylightSavingsTimeStart(@FinishDate) and @FinishDate < dbo.usf2_Utils_GetDaylightSavingsTimeEnd(@FinishDate)) then DateAdd(n,-@OffsetFromMoscow - 60 ,@FinishDate) else DateAdd(n,-@OffsetFromMoscow,@FinishDate) end

select hti.Contrti_id, Contr_ps_id,StartDate,FinishDate from 
(select distinct hl.Contrti_id,Min(EventDate) as StartDate, Max(EventDate) as FinishDate from
	(select ContrTI_ID  from dbo.Info_Contr_TI where 
		IsOV = 1
		and 
		not	ContrTI_ID in 
			(select distinct ContrTI_ID from ArchComm_Contr_OV_Switches where  FinishDateTime>=@FinishDate and StartDateTime <=@StartDate)
		and(
			Contrti_id in 
			(select ti.Contrti_id  from 
				(select Tp_ID from Info_Section_Description2 where Section_ID = @Section_ID) sd
				join 
				(select TP_ID from Info_TP2 where IsMoneyOurSide = 0 and EvalModeContr = 0) tp on tp.tp_id = sd.tp_id
				join info_Contr_ti ti on ti.tp_id2 = tp.tp_id)
			)) hl
	cross apply (
		select EventDate, [Val] from(
			select DateAdd(minute,(SUBSTRING(ValueRow,5,2)-1) * 30,EventDate) as EventDate, [Val]  from 
			(select ch1.EventDate
					,ch1.[VAL_01]+ch2.[VAL_01] as [VAL_01] ,ch1.[VAL_02]+ch2.[VAL_02] as [VAL_02] ,ch1.[VAL_03]+ch2.[VAL_03] as [VAL_03] ,ch1.[VAL_04]+ch2.[VAL_04] as [VAL_04] ,ch1.[VAL_05]+ch2.[VAL_05] as [VAL_05]
					,ch1.[VAL_06]+ch2.[VAL_06] as [VAL_06] ,ch1.[VAL_07]+ch2.[VAL_07] as [VAL_07] ,ch1.[VAL_08]+ch2.[VAL_08] as [VAL_08] ,ch1.[VAL_09]+ch2.[VAL_09] as [VAL_09] ,ch1.[VAL_10]+ch2.[VAL_10] as [VAL_10] 
					,ch1.[VAL_11]+ch2.[VAL_11] as [VAL_11] ,ch1.[VAL_12]+ch2.[VAL_12] as [VAL_12] ,ch1.[VAL_13]+ch2.[VAL_13] as [VAL_13] ,ch1.[VAL_14]+ch2.[VAL_14] as [VAL_14] ,ch1.[VAL_15]+ch2.[VAL_15] as [VAL_15]
					,ch1.[VAL_16]+ch2.[VAL_16] as [VAL_16] ,ch1.[VAL_17]+ch2.[VAL_17] as [VAL_17] ,ch1.[VAL_18]+ch2.[VAL_18] as [VAL_18] ,ch1.[VAL_19]+ch2.[VAL_19] as [VAL_19] ,ch1.[VAL_20]+ch2.[VAL_20] as [VAL_20]
					,ch1.[VAL_21]+ch2.[VAL_21] as [VAL_21] ,ch1.[VAL_22]+ch2.[VAL_22] as [VAL_22] ,ch1.[VAL_23]+ch2.[VAL_23] as [VAL_23] ,ch1.[VAL_24]+ch2.[VAL_24] as [VAL_24] ,ch1.[VAL_25]+ch2.[VAL_25] as [VAL_25]
					,ch1.[VAL_26]+ch2.[VAL_26] as [VAL_26] ,ch1.[VAL_27]+ch2.[VAL_27] as [VAL_27] ,ch1.[VAL_28]+ch2.[VAL_28] as [VAL_28] ,ch1.[VAL_29]+ch2.[VAL_29] as [VAL_29] ,ch1.[VAL_30]+ch2.[VAL_30] as [VAL_30] 
					,ch1.[VAL_31]+ch2.[VAL_31] as [VAL_31] ,ch1.[VAL_32]+ch2.[VAL_32] as [VAL_32] ,ch1.[VAL_33]+ch2.[VAL_33] as [VAL_33] ,ch1.[VAL_34]+ch2.[VAL_34] as [VAL_34] ,ch1.[VAL_35]+ch2.[VAL_35] as [VAL_35]
					,ch1.[VAL_36]+ch2.[VAL_36] as [VAL_36] ,ch1.[VAL_37]+ch2.[VAL_37] as [VAL_37] ,ch1.[VAL_38]+ch2.[VAL_38] as [VAL_38] ,ch1.[VAL_39]+ch2.[VAL_39] as [VAL_39] ,ch1.[VAL_40]+ch2.[VAL_40] as [VAL_40] 
					,ch1.[VAL_41]+ch2.[VAL_41] as [VAL_41] ,ch1.[VAL_42]+ch2.[VAL_42] as [VAL_42] ,ch1.[VAL_43]+ch2.[VAL_43] as [VAL_43] ,ch1.[VAL_44]+ch2.[VAL_44] as [VAL_44] ,ch1.[VAL_45]+ch2.[VAL_45] as [VAL_45]
					,ch1.[VAL_46]+ch2.[VAL_46] as [VAL_46] ,ch1.[VAL_47]+ch2.[VAL_47] as [VAL_47] ,ch1.[VAL_48]+ch2.[VAL_48] as [VAL_48] 
			 from 
				(select * from dbo.ArchComm_Contr_30_Import_From_XML where ContrTI_ID = hl.Contrti_id and ChannelType = 1 and (EventDate between floor(cast(@StartDate as float)) and floor(cast(@FinishDate as float))))  ch1
			 inner join 
				(select * from dbo.ArchComm_Contr_30_Import_From_XML where ContrTI_ID = hl.Contrti_id and ChannelType = 2 and (EventDate between floor(cast(@StartDate as float)) and floor(cast(@FinishDate as float))))  ch2
				on ch1.Contrti_id = ch2.Contrti_id and ch1.EventDate = ch2.EventDate 
	) as arh
	unpivot 
	([Val] for ValueRow in (
	[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10] ,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
			,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30] ,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
			,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48]
	)) as unpvt
	) a
	where [Val]>0 and EventDate>=@StartDate and EventDate<@FinishDate
	) arh
	group by hl.ContrTI_ID
) hti
join Info_Contr_TI ti on hti.ContrTI_ID = ti.ContrTI_ID
order by ti.Contr_PS_ID, hti.Contrti_id;

end
go
   grant EXECUTE on usp2_ArchComm_OV_CA_Statistic to [UserCalcService]
go