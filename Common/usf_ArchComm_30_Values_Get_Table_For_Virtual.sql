if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ArchComm_30_Values_Get_Table_For_Virtual')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ArchComm_30_Values_Get_Table_For_Virtual
go

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
--		На вход функции задаем строку, в которой упакована таблица со временем, идентификатором ТИ, каналом (описание ниже)
--		на выходе получаем таблицу с датой и 48 получасовками 
-- ======================================================================================
create  FUNCTION [dbo].[usf2_ArchComm_30_Values_Get_Table_For_Virtual] (
			@TI_Array varchar(4000) -- Идентификатор ТИ (строго 10 цифр), 
									-- время начала периода(строго 16 цифр), 
									-- время окончания периода(строго 16 цифр),
									-- номер канала,
									-- между полями запятая в конце обязательна точка с запятой
)	
	RETURNS @tbl TABLE 
(
		[TI_ID] int,
		[ChannelType] int, --Номер канала
		[oldday] DateTime null, --Поле по которому определяем есть уже такие данные в архивной таблице 30 минуток
		[EventDate] DateTime, --Дата календаря
		--Получасовки
		[VAL_01] float,[VAL_02] float,[VAL_03] float,[VAL_04] float,[VAL_05] float,[VAL_06] float,[VAL_07] float,[VAL_08] float,[VAL_09] float,[VAL_10] float,
		[VAL_11] float,[VAL_12] float,[VAL_13] float,[VAL_14] float,[VAL_15] float,[VAL_16] float,[VAL_17] float,[VAL_18] float,[VAL_19] float,[VAL_20] float,
		[VAL_21] float,[VAL_22] float,[VAL_23] float,[VAL_24] float,[VAL_25] float,[VAL_26] float,[VAL_27] float,[VAL_28] float,[VAL_29] float,[VAL_30] float,
		[VAL_31] float,[VAL_32] float,[VAL_33] float,[VAL_34] float,[VAL_35] float,[VAL_36] float,[VAL_37] float,[VAL_38] float,[VAL_39] float,[VAL_40] float,
		[VAL_41] float,[VAL_42] float,[VAL_43] float,[VAL_44] float,[VAL_45] float,[VAL_46] float,[VAL_47] float,[VAL_48] float,
		[ValidStatus] bigint,IsDataInNecessaryRangeOfTime bigint -- Побитово выставляем биты для тех получасовок которые присутствуют в нужном диапазоне даты (времени)
)
AS
BEGIN
		insert @tbl([TI_ID],[EventDate], [oldday], [ChannelType]
		,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10] ,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
		,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30] ,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
		,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48],[ValidStatus],[IsDataInNecessaryRangeOfTime])
		select usf.[TI_ID],arh.[dt],virt.[EventDate] as olddate, arh.[ChannelType]
		,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10] ,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
		,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30] ,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
		,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48],ISNULL([ValidStatus],0),
		IsDataInNecessaryRangeOfTime = 
				case when arh.[dt] between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,0,1) else 0 end |
				case when DateAdd(n,30,arh.[dt]) between usf.StartDate and usf.FinishDate  then	dbo.sfclr_Utils_BitOperations (0,1,1) else 0 end |
				case when DateAdd(n,60,arh.[dt]) between usf.StartDate and usf.FinishDate  then	dbo.sfclr_Utils_BitOperations (0,2,1) else 0 end |
				case when DateAdd(n,90,arh.[dt]) between usf.StartDate and usf.FinishDate  then	dbo.sfclr_Utils_BitOperations (0,3,1) else 0 end |
				case when DateAdd(n,120,arh.[dt]) between usf.StartDate and usf.FinishDate  then	dbo.sfclr_Utils_BitOperations (0,4,1) else 0 end |
				case when DateAdd(n,150,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,5,1) else 0 end |
				case when DateAdd(n,180,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,6,1) else 0 end |
				case when DateAdd(n,210,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,7,1) else 0 end |
				case when DateAdd(n,240,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,8,1) else 0 end |
				case when DateAdd(n,270,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,9,1) else 0 end |
				case when DateAdd(n,300,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,10,1) else 0 end |
				case when DateAdd(n,330,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,11,1) else 0 end |
				case when DateAdd(n,360,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,12,1) else 0 end |
				case when DateAdd(n,390,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,13,1) else 0 end |
				case when DateAdd(n,420,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,14,1) else 0 end |
				case when DateAdd(n,450,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,15,1) else 0 end |
				case when DateAdd(n,480,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,16,1) else 0 end |
				case when DateAdd(n,510,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,17,1) else 0 end |
				case when DateAdd(n,540,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,18,1) else 0 end |
				case when DateAdd(n,570,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,19,1) else 0 end |
				case when DateAdd(n,600,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,20,1) else 0 end |
				case when DateAdd(n,630,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,21,1) else 0 end |
				case when DateAdd(n,660,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,22,1) else 0 end |
				case when DateAdd(n,690,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,23,1) else 0 end |
				case when DateAdd(n,720,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,24,1) else 0 end |
				case when DateAdd(n,750,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,25,1) else 0 end |
				case when DateAdd(n,780,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,26,1) else 0 end |
				case when DateAdd(n,810,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,27,1) else 0 end |
				case when DateAdd(n,840,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,28,1) else 0 end |
				case when DateAdd(n,870,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,29,1) else 0 end |
				case when DateAdd(n,900,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,30,1) else 0 end |
				case when DateAdd(n,930,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,31,1) else 0 end |
				case when DateAdd(n,960,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,32,1) else 0 end |
				case when DateAdd(n,990,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,33,1) else 0 end |
				case when DateAdd(n,1020,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,34,1) else 0 end |
				case when DateAdd(n,1050,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,35,1) else 0 end |
				case when DateAdd(n,1080,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,36,1) else 0 end |
				case when DateAdd(n,1110,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,37,1) else 0 end |
				case when DateAdd(n,1140,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,38,1) else 0 end |
				case when DateAdd(n,1170,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,39,1) else 0 end |
				case when DateAdd(n,1200,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,40,1) else 0 end |
				case when DateAdd(n,1230,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,41,1) else 0 end |
				case when DateAdd(n,1260,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,42,1) else 0 end |
				case when DateAdd(n,1290,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,43,1) else 0 end |
				case when DateAdd(n,1320,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,44,1) else 0 end |
				case when DateAdd(n,1350,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,45,1) else 0 end |
				case when DateAdd(n,1380,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,46,1) else 0 end |
				case when DateAdd(n,1410,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,47,1) else 0 end |
				case when DateAdd(n,1440,arh.[dt]) between usf.StartDate and usf.FinishDate  then dbo.sfclr_Utils_BitOperations (0,48,1) else 0 end 
from usf2_Utils_iter_datetimelist_to_table(@TI_Array) usf 
outer apply usf2_ArchComm_30_Values_Get_Table_By_TIType(usf.ti_id, cast(floor([Float2]) as int), usf.StartDate, usf.FinishDate, 0,0) as arh
left join 
(select TI_ID,ChannelType,EventDate from dbo.ArchCalc_30_Virtual) virt
on virt.TI_ID = arh.TI_ID and virt.ChannelType = arh.ChannelType and virt.EventDate = arh.dt
		
	
		RETURN
END
go
grant select on usf2_ArchComm_30_Values_Get_Table_For_Virtual to [UserCalcService]
go