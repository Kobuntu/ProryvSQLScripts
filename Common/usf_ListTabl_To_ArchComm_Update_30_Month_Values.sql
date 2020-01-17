if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ListTabl_To_ArchComm_Update_30_Month_Values')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ListTabl_To_ArchComm_Update_30_Month_Values
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2010
--
-- Описание:
--
--		На вход функции задаем строку, в которой упакована таблица со временем, значением, каналом (описание ниже)
--		на выходе получаем таблицу с датой схожей с таблицей получасовок, процедура возвращает только достоверность
-- ======================================================================================
create FUNCTION [dbo].[usf2_ListTabl_To_ArchComm_Update_30_Month_Values] (
			@TI_Array varchar(4000)-- Идентификатор ТИ (строго 10 цифр) , время (строго 16 цифр), значение, номер канала 
									-- между полями , в конце обязательна ;
)	
	RETURNS @tbl TABLE 
(
		[TI_ID] int,
		[ChannelType] int, --Номер канала
		[oldday] DateTime null, --Поле по которому определяем есть уже такие данные в архивной таблице 30 минуток
		[CalendarDayDate] DateTime, --Дата календаря
		--Получасовки
		[k1] float,[k2] float,[k3] float,[k4] float,[k5] float,[k6] float,[k7] float,[k8] float,[k9] float,[k10] float,
		[k11] float,[k12] float,[k13] float,[k14] float,[k15] float,[k16] float,[k17] float,[k18] float,[k19] float,[k20] float,
		[k21] float,[k22] float,[k23] float,[k24] float,[k25] float,[k26] float,[k27] float,[k28] float,[k29] float,[k30] float,
		[k31] float,[k32] float,[k33] float,[k34] float,[k35] float,[k36] float,[k37] float,[k38] float,[k39] float,[k40] float,
		[k41] float,[k42] float,[k43] float,[k44] float,[k45] float,[k46] float,[k47] float,[k48] float,
		NotNullStatus bigint -- Побитово выставляем биты для тех получасовок которые присутствуют

)
AS
BEGIN
		insert @tbl
		SELECT TI_ID, [ChannelType], [olddate] as olddate, cast([DayDate] as DateTime) as [CalendarDayDate],
					[0] as k1, [30] as k2,[60] as k3,[90] as k4,[120] as k5,[150] as k6,[180] as k7,[210] as k8,[240] as k9,[270] as k10,
					[300] as k11,[330] as k12,[360] as k13,[390] as k14,[420] as k15,[450] as k16,[480] as k17,[510] as k18,[540] as k19,[570] as k20,
					[600] as k21,[630] as k22,[660] as k23,[690] as k24,[720] as k25,[750] as k26,[780] as k27,[810] as k28,[840] as k29,[870] as k30,
					[900] as k31,[930] as k32,[960] as k33,[990] as k34,[1020] as k35,[1050] as k36,[1080] as k37,[1110] as k38,[1140] as k39,[1170] as k40,
					[1200] as k41,[1230] as k42,[1260] as k43,[1290] as k44,[1320] as k45,[1350] as k46,[1380] as k47,[1410] as k48,
				NotNullStatus = 
				case when not [0] is null then	dbo.sfclr_Utils_BitOperations (0,0,1) else 0 end |
				case when not [30] is null then	dbo.sfclr_Utils_BitOperations (0,1,1) else 0 end |
				case when not [60] is null then	dbo.sfclr_Utils_BitOperations (0,2,1) else 0 end |
				case when not [90] is null then	dbo.sfclr_Utils_BitOperations (0,3,1) else 0 end |
				case when not [120] is null then dbo.sfclr_Utils_BitOperations (0,4,1) else 0 end |
				case when not [150] is null then dbo.sfclr_Utils_BitOperations (0,5,1) else 0 end |
				case when not [180] is null then dbo.sfclr_Utils_BitOperations (0,6,1) else 0 end |
				case when not [210] is null then dbo.sfclr_Utils_BitOperations (0,7,1) else 0 end |
				case when not [240] is null then dbo.sfclr_Utils_BitOperations (0,8,1) else 0 end |
				case when not [270] is null then dbo.sfclr_Utils_BitOperations (0,9,1) else 0 end |
				case when not [300] is null then dbo.sfclr_Utils_BitOperations (0,10,1) else 0 end |
				case when not [330] is null then dbo.sfclr_Utils_BitOperations (0,11,1) else 0 end |
				case when not [360] is null then dbo.sfclr_Utils_BitOperations (0,12,1) else 0 end |
				case when not [390] is null then dbo.sfclr_Utils_BitOperations (0,13,1) else 0 end |
				case when not [420] is null then dbo.sfclr_Utils_BitOperations (0,14,1) else 0 end |
				case when not [450] is null then dbo.sfclr_Utils_BitOperations (0,15,1) else 0 end |
				case when not [480] is null then dbo.sfclr_Utils_BitOperations (0,16,1) else 0 end |
				case when not [510] is null then dbo.sfclr_Utils_BitOperations (0,17,1) else 0 end |
				case when not [540] is null then dbo.sfclr_Utils_BitOperations (0,18,1) else 0 end |
				case when not [570] is null then dbo.sfclr_Utils_BitOperations (0,19,1) else 0 end |
				case when not [600] is null then dbo.sfclr_Utils_BitOperations (0,20,1) else 0 end |
				case when not [630] is null then dbo.sfclr_Utils_BitOperations (0,21,1) else 0 end |
				case when not [660] is null then dbo.sfclr_Utils_BitOperations (0,22,1) else 0 end |
				case when not [690] is null then dbo.sfclr_Utils_BitOperations (0,23,1) else 0 end |
				case when not [720] is null then dbo.sfclr_Utils_BitOperations (0,24,1) else 0 end |
				case when not [750] is null then dbo.sfclr_Utils_BitOperations (0,25,1) else 0 end |
				case when not [780] is null then dbo.sfclr_Utils_BitOperations (0,26,1) else 0 end |
				case when not [810] is null then dbo.sfclr_Utils_BitOperations (0,27,1) else 0 end |
				case when not [840] is null then dbo.sfclr_Utils_BitOperations (0,28,1) else 0 end |
				case when not [870] is null then dbo.sfclr_Utils_BitOperations (0,29,1) else 0 end |
				case when not [900] is null then dbo.sfclr_Utils_BitOperations (0,30,1) else 0 end |
				case when not [930] is null then dbo.sfclr_Utils_BitOperations (0,31,1) else 0 end |
				case when not [960] is null then dbo.sfclr_Utils_BitOperations (0,32,1) else 0 end |
				case when not [990] is null then dbo.sfclr_Utils_BitOperations (0,33,1) else 0 end |
				case when not [1020] is null then dbo.sfclr_Utils_BitOperations (0,34,1) else 0 end |
				case when not [1050] is null then dbo.sfclr_Utils_BitOperations (0,35,1) else 0 end |
				case when not [1080] is null then dbo.sfclr_Utils_BitOperations (0,36,1) else 0 end |
				case when not [1110] is null then dbo.sfclr_Utils_BitOperations (0,37,1) else 0 end |
				case when not [1140] is null then dbo.sfclr_Utils_BitOperations (0,38,1) else 0 end |
				case when not [1170] is null then dbo.sfclr_Utils_BitOperations (0,39,1) else 0 end |
				case when not [1200] is null then dbo.sfclr_Utils_BitOperations (0,40,1) else 0 end |
				case when not [1230] is null then dbo.sfclr_Utils_BitOperations (0,41,1) else 0 end |
				case when not [1260] is null then dbo.sfclr_Utils_BitOperations (0,42,1) else 0 end |
				case when not [1290] is null then dbo.sfclr_Utils_BitOperations (0,43,1) else 0 end |
				case when not [1320] is null then dbo.sfclr_Utils_BitOperations (0,44,1) else 0 end |
				case when not [1350] is null then dbo.sfclr_Utils_BitOperations (0,45,1) else 0 end |
				case when not [1380] is null then dbo.sfclr_Utils_BitOperations (0,46,1) else 0 end |
				case when not [1410] is null then dbo.sfclr_Utils_BitOperations (0,47,1) else 0 end 
			FROM
			(
				SELECT t.[ti_id],t.[ChannelType] as [ChannelType]
				, cast(floor(cast(t.[EventDate] as float)) as DateTime) as [DayDate], DateDiff(n,cast(floor(cast(t.[EventDate] as float)) as DateTime),t.[EventDate]) as CalendarDayDate, [Value], mr.EventDate as [olddate], [valid] as valid
					FROM
				(
					--Параметры запроса в табличный вид
					select [ti_id], [EventDate], [Float1] as [Value], cast(floor([Float2] / 10) as int) as ChannelType, cast( ([Float2] % 10) as int) as [valid]  from usf2_Utils_iter_floatlist_to_table(@TI_Array)
				) t
				left join 
				(
					--Проверяем есть ли данные за этот период (обновлять или добавлять)
					select EventDate, TI_ID,ChannelType  from dbo.ArchCalc_30_Month_Values
				) mr 
				on mr.TI_ID=t.TI_ID and cast(floor(cast(t.[EventDate] as float)) as DateTime) = mr.EventDate and mr.ChannelType = t.[ChannelType]				
			) AS SourceTable
			--поворачиваем в обратную сторону (-90 градусов)
			PIVOT
			(
			--Берем среднее (на тот случай если будет несколько данных за одно и тоже время)
			avg([Value]) FOR [CalendarDayDate] IN ([0],[30],[60],[90],[120],[150],[180],[210],[240],[270],[300],[330],[360],[390],[420],[450],[480],[510],[540],[570],[600],[630],[660],[690],[720],[750],[780],[810],[840],[870],[900],[930],[960],[990],[1020],[1050],[1080],[1110],[1140],[1170],[1200],[1230],[1260],[1290],[1320],[1350],[1380],[1410])
			) AS PivotTable
			order by [DayDate]
	
		RETURN
END
go
grant select on usf2_ListTabl_To_ArchComm_Update_30_Month_Values to [UserCalcService]
go
