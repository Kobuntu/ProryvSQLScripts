if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ListTabl_To_ArchComm_Contr_30_Import_From_XML_Validate')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ListTabl_To_ArchComm_Contr_30_Import_From_XML_Validate
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
--		На вход функции задаем строку, в которой упакована таблица со временем, значением, каналом (описание ниже)
--		на выходе получаем таблицу с датой схожей с таблицей получасовок, процедура возвращает только достоверность
--		эта прроцедура для КА
-- ======================================================================================
create  FUNCTION [dbo].[usf2_ListTabl_To_ArchComm_Contr_30_Import_From_XML_Validate] (
			@TI_Array varchar(4000)-- Идентификатор ТИ (строго 10 цифр) , время (строго 16 цифр), значение, номер канала 
									-- между полями , в конце обязательна ;
)	
	RETURNS @tbl TABLE 
(
		[TI_ID] int,
		[ChannelType] int, --Номер канала
		[oldday] DateTime null, --Поле по которому определяем есть уже такие данные в архивной таблице 30 минуток
		[CalendarDayDate] DateTime, --Дата календаря
		ValidStatus bigint

)
AS
BEGIN
		insert @tbl

SELECT PivotTable.TI_ID, PivotTable.[ChannelType], [olddate] as olddate, cast([DayDate] as DateTime) as [CalendarDayDate],
				ValidStatus = 
				case when not [0] is null then	dbo.sfclr_Utils_BitOperations (0,0,[0]) else 0 end |
				case when not [30] is null then	dbo.sfclr_Utils_BitOperations (0,1,[30]) else 0 end |
				case when not [60] is null then	dbo.sfclr_Utils_BitOperations (0,2,[60]) else 0 end |
				case when not [90] is null then	dbo.sfclr_Utils_BitOperations (0,3,[90]) else 0 end |
				case when not [120] is null then dbo.sfclr_Utils_BitOperations (0,4,[120]) else 0 end |
				case when not [150] is null then dbo.sfclr_Utils_BitOperations (0,5,[150]) else 0 end |
				case when not [180] is null then dbo.sfclr_Utils_BitOperations (0,6,[180]) else 0 end |
				case when not [210] is null then dbo.sfclr_Utils_BitOperations (0,7,[210]) else 0 end |
				case when not [240] is null then dbo.sfclr_Utils_BitOperations (0,8,[240]) else 0 end |
				case when not [270] is null then dbo.sfclr_Utils_BitOperations (0,9,[270]) else 0 end |
				case when not [300] is null then dbo.sfclr_Utils_BitOperations (0,10,[300]) else 0 end |
				case when not [330] is null then dbo.sfclr_Utils_BitOperations (0,11,[330]) else 0 end |
				case when not [360] is null then dbo.sfclr_Utils_BitOperations (0,12,[360]) else 0 end |
				case when not [390] is null then dbo.sfclr_Utils_BitOperations (0,13,[390]) else 0 end |
				case when not [420] is null then dbo.sfclr_Utils_BitOperations (0,14,[420]) else 0 end |
				case when not [450] is null then dbo.sfclr_Utils_BitOperations (0,15,[450]) else 0 end |
				case when not [480] is null then dbo.sfclr_Utils_BitOperations (0,16,[480]) else 0 end |
				case when not [510] is null then dbo.sfclr_Utils_BitOperations (0,17,[510]) else 0 end |
				case when not [540] is null then dbo.sfclr_Utils_BitOperations (0,18,[540]) else 0 end |
				case when not [570] is null then dbo.sfclr_Utils_BitOperations (0,19,[570]) else 0 end |
				case when not [600] is null then dbo.sfclr_Utils_BitOperations (0,20,[600]) else 0 end |
				case when not [630] is null then dbo.sfclr_Utils_BitOperations (0,21,[630]) else 0 end |
				case when not [660] is null then dbo.sfclr_Utils_BitOperations (0,22,[660]) else 0 end |
				case when not [690] is null then dbo.sfclr_Utils_BitOperations (0,23,[690]) else 0 end |
				case when not [720] is null then dbo.sfclr_Utils_BitOperations (0,24,[720]) else 0 end |
				case when not [750] is null then dbo.sfclr_Utils_BitOperations (0,25,[750]) else 0 end |
				case when not [780] is null then dbo.sfclr_Utils_BitOperations (0,26,[780]) else 0 end |
				case when not [810] is null then dbo.sfclr_Utils_BitOperations (0,27,[810]) else 0 end |
				case when not [840] is null then dbo.sfclr_Utils_BitOperations (0,28,[840]) else 0 end |
				case when not [870] is null then dbo.sfclr_Utils_BitOperations (0,29,[870]) else 0 end |
				case when not [900] is null then dbo.sfclr_Utils_BitOperations (0,30,[900]) else 0 end |
				case when not [930] is null then dbo.sfclr_Utils_BitOperations (0,31,[930]) else 0 end |
				case when not [960] is null then dbo.sfclr_Utils_BitOperations (0,32,[960]) else 0 end |
				case when not [990] is null then dbo.sfclr_Utils_BitOperations (0,33,[990]) else 0 end |
				case when not [1020] is null then dbo.sfclr_Utils_BitOperations (0,34,[1020]) else 0 end |
				case when not [1050] is null then dbo.sfclr_Utils_BitOperations (0,35,[1050]) else 0 end |
				case when not [1080] is null then dbo.sfclr_Utils_BitOperations (0,36,[1080]) else 0 end |
				case when not [1110] is null then dbo.sfclr_Utils_BitOperations (0,37,[1110]) else 0 end |
				case when not [1140] is null then dbo.sfclr_Utils_BitOperations (0,38,[1140]) else 0 end |
				case when not [1170] is null then dbo.sfclr_Utils_BitOperations (0,39,[1170]) else 0 end |
				case when not [1200] is null then dbo.sfclr_Utils_BitOperations (0,40,[1200]) else 0 end |
				case when not [1230] is null then dbo.sfclr_Utils_BitOperations (0,41,[1230]) else 0 end |
				case when not [1260] is null then dbo.sfclr_Utils_BitOperations (0,42,[1260]) else 0 end |
				case when not [1290] is null then dbo.sfclr_Utils_BitOperations (0,43,[1290]) else 0 end |
				case when not [1320] is null then dbo.sfclr_Utils_BitOperations (0,44,[1320]) else 0 end |
				case when not [1350] is null then dbo.sfclr_Utils_BitOperations (0,45,[1350]) else 0 end |
				case when not [1380] is null then dbo.sfclr_Utils_BitOperations (0,46,[1380]) else 0 end |
				case when not [1410] is null then dbo.sfclr_Utils_BitOperations (0,47,[1410]) else 0 end 
			FROM
			(
				SELECT t.[ti_id],[ChannelType] = dbo.usf2_ReverseTariffChannel(1, t.[ChannelType], ti.AOATSCode,ti.AIATSCode,ti.ROATSCode,ti.RIATSCode, t.TI_ID, t.[EventDate], t.[EventDate])
				, cast(floor(cast(t.[EventDate] as float)) as DateTime) as [DayDate], DateDiff(n,cast(floor(cast(t.[EventDate] as float)) as DateTime),t.[EventDate]) as CalendarDayDate, mr.EventDate as [olddate], [valid] as valid
					FROM
				(
					--Параметры запроса в табличный вид
					select [ti_id], [EventDate], cast(floor([Float2] / 10) as int) as ChannelType, cast(( case when ([Float2] % 10) > 0  then 1 else 0 end) as int) as [valid]  from usf2_Utils_iter_floatlist_to_table(@TI_Array)
				) t
				left join 
				(
					--Проверяем достоверность каналов
					select ContrTI_ID,AIATSCode,AOATSCode,RIATSCode,ROATSCode from Info_Contr_TI
				) ti
				on ti.ContrTI_ID =t.TI_ID 
				left join 
				(
					--Проверяем есть ли данные за этот период (обновлять или добавлять)
					select EventDate, ContrTI_ID ,ChannelType  from dbo.[ArchComm_Contr_30_Import_From_XML]
				) mr 
				on cast(floor(cast(t.[EventDate] as float)) as DateTime) = mr.EventDate and mr.ContrTI_ID=t.TI_ID and mr.ChannelType = case t.[ChannelType] when 1 then (ti.AOATSCode)
									when 2 then (ti.AIATSCode) 
									when 3 then (ti.ROATSCode) 
									when 4 then (ti.RIATSCode)
					end  
				

			) AS SourceTable
			--поворачиваем в обратную сторону (-90 градусов)
			PIVOT
			(
			--Берем среднее (на тот случай если будет несколько данных за одно и тоже время)
			avg([valid]) FOR [CalendarDayDate] IN ([0],[30],[60],[90],[120],[150],[180],[210],[240],[270],[300],[330],[360],[390],[420],[450],[480],[510],[540],[570],[600],[630],[660],[690],[720],[750],[780],[810],[840],[870],[900],[930],[960],[990],[1020],[1050],[1080],[1110],[1140],[1170],[1200],[1230],[1260],[1290],[1320],[1350],[1380],[1410])
			) AS PivotTable
			order by [DayDate]
	
		RETURN
END
go
grant select on usf2_ListTabl_To_ArchComm_Contr_30_Import_From_XML_Validate to [UserCalcService]
go
