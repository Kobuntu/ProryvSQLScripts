if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_OV_Statistic')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_OV_Statistic
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
--		Июнь, 2009
--
-- Описание:
--
--		Выбираем общее количество ОВ по ПС, и количество разнесенных ОВ для ПС
--
-- ======================================================================================
create proc [dbo].[usp2_ArchComm_OV_Statistic]
	@DayMonthYear DateTime, --Дата на которую смотрим
	@Section_ID int = null -- Если сечение не указанно то выбираем все нераспределенные ОВ
							--Если указанно то выбираем только для этого сечения
as
declare
@startDateTime DateTime, 
@finishDateTime DateTime,
@minuteDiff int

set @startDateTime = floor(cast(@DayMonthYear as float))
set @finishDateTime = DateAdd(minute,1439,@startDateTime)

set @minuteDiff = DateDiff(minute,@startDateTime, @finishDateTime) + 1

create table #ovs(ti_id int, titype tinyint, isCa bit, ps_id int, absentChannelsMask tinyint);
create table #linkedOv(ti_id int, StartDateTime DateTime, FinishDateTime DateTime)

--Выбираем точки с признаком частичного распределения в нужной нам дате или вообще не распределенные в нужной дате
if @Section_ID is null begin

	insert into #ovs 
	select distinct ho.ti_id, ti.TIType, CONVERT(bit, 0) as isCa, ti.ps_id, AbsentChannelsMask from Hard_OV_List ho
	join Info_TI ti on ti.TI_ID = ho.TI_ID
	where ov_id not in 
	(
		select OV_ID from  
			(
				select OV_ID,
				DateDiff(minute,
					dbo.usf2_Utils_DateTimeRoundToHalfHour(case when StartDateTime > @startDateTime then StartDateTime else @startDateTime end, 1), --Начало округляем в меньшую сторону
					dbo.usf2_Utils_DateTimeRoundToHalfHour(case when FinishDateTime < @finishDateTime then FinishDateTime else @finishDateTime end,0) --Конец округляем в большую сторону
				)as Minutes
				from ArchComm_OV_Switches 
				where FinishDateTime>@startDateTime and StartDateTime<@finishDateTime
			) arh_ov group by OV_ID having Sum(Minutes) >= @minuteDiff
	 )
end else begin
	
	--Обычные точки
	insert into #ovs
	select distinct ho.ti_id,ti.TIType, CONVERT(bit, 0) as isCa, ti.ps_id, ti.AbsentChannelsMask from Hard_OV_List ho
	join Info_TI ti on ti.TI_ID = ho.TI_ID
	where 
		ov_id not in 
		(
			select OV_ID from  
				(
					select OV_ID,
					DateDiff(minute,
						dbo.usf2_Utils_DateTimeRoundToHalfHour(case when StartDateTime > @startDateTime then StartDateTime else @startDateTime end, 1), --Начало округляем в меньшую сторону
						dbo.usf2_Utils_DateTimeRoundToHalfHour(case when FinishDateTime < @finishDateTime then FinishDateTime else @finishDateTime end,0) --Конец округляем в большую сторону
					)as Minutes
					from ArchComm_OV_Switches 
					where FinishDateTime>@startDateTime and StartDateTime<@finishDateTime
				) arh_ov group by OV_ID having Sum(Minutes) >= @minuteDiff
		)
		and 
		(	
			ho.ti_id in 	
				(select ti.ti_id  from 
					(select Tp_ID from Info_Section_Description2 where Section_ID = @Section_ID) sd
					join 
					(select TP_ID from Info_TP2 where IsMoneyOurSide = 1 and EvalModeOurSide = 0) tp on tp.tp_id = sd.tp_id
					join info_ti ti on ti.tp_id = tp.tp_id)
			or 
			OV_ID in 
				(select ov_id from Hard_OV_Positions_List hl
					join info_ti ti on ti.ti_id = hl.ti_id
					join (select Tp_ID from Info_Section_Description2 where Section_ID = @Section_ID) s on s.tp_id = ti.tp_id
					join (select TP_ID from Info_TP2 where IsMoneyOurSide = 1 and EvalModeOurSide = 0) tp on tp.tp_id = s.tp_id
				)
		)

	--Точки КА
	insert into #ovs
	select distinct ti.ContrTI_ID, ti.TIType, CONVERT(bit, 1) as isCa, ti.Contr_PS_ID, 0 as AbsentChannelsMask from Info_Contr_TI ti 
	where IsOV = 1 and
		ContrTI_ID not in 
		(
			select ContrTI_ID from  
				(
					select ContrTI_ID,
					DateDiff(minute,
						dbo.usf2_Utils_DateTimeRoundToHalfHour(case when StartDateTime > @startDateTime then StartDateTime else @startDateTime end, 1), --Начало округляем в меньшую сторону
						dbo.usf2_Utils_DateTimeRoundToHalfHour(case when FinishDateTime < @finishDateTime then FinishDateTime else @finishDateTime end,0) --Конец округляем в большую сторону
					)as Minutes
					from ArchComm_Contr_OV_Switches 
					where FinishDateTime>@startDateTime and StartDateTime<@finishDateTime
				) arh_ov group by ContrTI_ID having Sum(Minutes) >= @minuteDiff
		)
		and 
		(	
			ContrTI_ID in 	
				(select ti.ContrTI_ID  from 
					(select Tp_ID from Info_Section_Description2 where Section_ID = @Section_ID) sd
					join 
					(select TP_ID from Info_TP2 where IsMoneyOurSide = 1 and EvalModeOurSide = 0) tp on tp.tp_id = sd.tp_id
					join info_Contr_ti ti on ti.tp_id = tp.tp_id)
		)
end		
		 

----ОВ которые частично разнесены в нашем диапазоне, определяем первую и последние получасовки, исключаем затем эти получасовки
insert into #linkedOv
select 
	#ovs.ti_id,
	dbo.usf2_Utils_DateTimeRoundToHalfHour(case when StartDateTime > @startDateTime then StartDateTime else @startDateTime end, 1), --Начало округляем в меньшую сторону
	DateAdd(n, -30, dbo.usf2_Utils_DateTimeRoundToHalfHour(case when FinishDateTime < @finishDateTime then FinishDateTime else @finishDateTime end,0)) --Конец округляем в большую сторону
from #ovs
join Hard_OV_List on Hard_OV_List.ti_id = #ovs.ti_id
join ArchComm_OV_Switches on Hard_OV_List.ov_id = ArchComm_OV_Switches.ov_id and FinishDateTime>@startDateTime and StartDateTime<@finishDateTime
where #ovs.isCa = 0

declare @TI_ID int, @tiType tinyint, @absentChannelsMask tinyint;

--Результирующая таблица
declare @archives table(
	[EventDate] [smalldatetime] NOT NULL,
	[ValidStatus] [bigint] NOT NULL,
	[VAL_01] [float] NULL,
	[VAL_02] [float] NULL,
	[VAL_03] [float] NULL,
	[VAL_04] [float] NULL,
	[VAL_05] [float] NULL,
	[VAL_06] [float] NULL,
	[VAL_07] [float] NULL,
	[VAL_08] [float] NULL,
	[VAL_09] [float] NULL,
	[VAL_10] [float] NULL,
	[VAL_11] [float] NULL,
	[VAL_12] [float] NULL,
	[VAL_13] [float] NULL,
	[VAL_14] [float] NULL,
	[VAL_15] [float] NULL,
	[VAL_16] [float] NULL,
	[VAL_17] [float] NULL,
	[VAL_18] [float] NULL,
	[VAL_19] [float] NULL,
	[VAL_20] [float] NULL,
	[VAL_21] [float] NULL,
	[VAL_22] [float] NULL,
	[VAL_23] [float] NULL,
	[VAL_24] [float] NULL,
	[VAL_25] [float] NULL,
	[VAL_26] [float] NULL,
	[VAL_27] [float] NULL,
	[VAL_28] [float] NULL,
	[VAL_29] [float] NULL,
	[VAL_30] [float] NULL,
	[VAL_31] [float] NULL,
	[VAL_32] [float] NULL,
	[VAL_33] [float] NULL,
	[VAL_34] [float] NULL,
	[VAL_35] [float] NULL,
	[VAL_36] [float] NULL,
	[VAL_37] [float] NULL,
	[VAL_38] [float] NULL,
	[VAL_39] [float] NULL,
	[VAL_40] [float] NULL,
	[VAL_41] [float] NULL,
	[VAL_42] [float] NULL,
	[VAL_43] [float] NULL,
	[VAL_44] [float] NULL,
	[VAL_45] [float] NULL,
	[VAL_46] [float] NULL,
	[VAL_47] [float] NULL,
	[VAL_48] [float] NULL,
	[Coeff] [float],
	[DataSource_ID] [tinyint] NULL,
	[CAL_01] [float] NULL,
	[CAL_02] [float] NULL,
	[CAL_03] [float] NULL,
	[CAL_04] [float] NULL,
	[CAL_05] [float] NULL,
	[CAL_06] [float] NULL,
	[CAL_07] [float] NULL,
	[CAL_08] [float] NULL,
	[CAL_09] [float] NULL,
	[CAL_10] [float] NULL,
	[CAL_11] [float] NULL,
	[CAL_12] [float] NULL,
	[CAL_13] [float] NULL,
	[CAL_14] [float] NULL,
	[CAL_15] [float] NULL,
	[CAL_16] [float] NULL,
	[CAL_17] [float] NULL,
	[CAL_18] [float] NULL,
	[CAL_19] [float] NULL,
	[CAL_20] [float] NULL,
	[CAL_21] [float] NULL,
	[CAL_22] [float] NULL,
	[CAL_23] [float] NULL,
	[CAL_24] [float] NULL,
	[CAL_25] [float] NULL,
	[CAL_26] [float] NULL,
	[CAL_27] [float] NULL,
	[CAL_28] [float] NULL,
	[CAL_29] [float] NULL,
	[CAL_30] [float] NULL,
	[CAL_31] [float] NULL,
	[CAL_32] [float] NULL,
	[CAL_33] [float] NULL,
	[CAL_34] [float] NULL,
	[CAL_35] [float] NULL,
	[CAL_36] [float] NULL,
	[CAL_37] [float] NULL,
	[CAL_38] [float] NULL,
	[CAL_39] [float] NULL,
	[CAL_40] [float] NULL,
	[CAL_41] [float] NULL,
	[CAL_42] [float] NULL,
	[CAL_43] [float] NULL,
	[CAL_44] [float] NULL,
	[CAL_45] [float] NULL,
	[CAL_46] [float] NULL,
	[CAL_47] [float] NULL,
	[CAL_48] [float] NULL,
	[ContrReplaceStatus] [bigint] NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[ManualEnterStatus] [bigint] NOT NULL,
	[ManualValidStatus] [bigint] NULL,
	PreviousDispatchDateTime datetime, 
	[TI_ID] int NOT NULL,
	[ChannelType] tinyint NOT NULL,
	[StartChannelStatus] dateTime null,
	[FinishChannelStatus] dateTime null
)


--Читаем получасовки проверяем наличие данных
declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select TI_ID, tiType, ISNULL(absentChannelsMask,0) from #ovs where #ovs.isCa = 0
  open t;
	FETCH NEXT FROM t into @TI_ID, @tiType, @absentChannelsMask
	WHILE @@FETCH_STATUS = 0
	BEGIN

	--select @TI_ID, @tiType, @absentChannelsMask
	if ((@absentChannelsMask & 1) = 0)
		insert @archives
		exec usp2_Arch_30_Select @TI_ID, @startDateTime,@finishDateTime,1,@tiType,null,null,0, 0, 0
		
	if (((@absentChannelsMask / 2)  & 1) = 0)
		insert @archives
		exec usp2_Arch_30_Select @TI_ID, @startDateTime,@finishDateTime,2,@tiType,null,null,0, 0, 0

	FETCH NEXT FROM t into @TI_ID, @tiType, @absentChannelsMask
	END;
	CLOSE t
	DEALLOCATE t

--select * from #ovs
--select * from #linkedOv
--select * from #archives

select hti.ti_id, ti.ps_id,StartDate,FinishDate from 
(
	select distinct hl.ti_id,Min(EventDate) as StartDate, Max(EventDate) as FinishDate from #ovs hl
	cross apply
	(
		select EventDate, [Val] from
		(
			select DateAdd(minute,(SUBSTRING(ValueRow,5,2)-1) * 30,EventDate) as EventDate, [Val]  from 
			(
				select ISNULL(a1.EventDate, a2.EventDate) as EventDate
					,ISNULL(a1.[VAL_01], 0)+ISNULL(a2.[VAL_01], 0) as [VAL_01] ,ISNULL(a1.[VAL_02], 0)+ISNULL(a2.[VAL_02], 0) as [VAL_02] ,ISNULL(a1.[VAL_03], 0)+ISNULL(a2.[VAL_03], 0) as [VAL_03] ,ISNULL(a1.[VAL_04], 0)+ISNULL(a2.[VAL_04], 0) as [VAL_04] ,ISNULL(a1.[VAL_05], 0)+ISNULL(a2.[VAL_05], 0) as [VAL_05]
					,ISNULL(a1.[VAL_06], 0)+ISNULL(a2.[VAL_06], 0) as [VAL_06] ,ISNULL(a1.[VAL_07], 0)+ISNULL(a2.[VAL_07], 0) as [VAL_07] ,ISNULL(a1.[VAL_08], 0)+ISNULL(a2.[VAL_08], 0) as [VAL_08] ,ISNULL(a1.[VAL_09], 0)+ISNULL(a2.[VAL_09], 0) as [VAL_09] ,ISNULL(a1.[VAL_10], 0)+ISNULL(a2.[VAL_10], 0) as [VAL_10] 
					,ISNULL(a1.[VAL_11], 0)+ISNULL(a2.[VAL_11], 0) as [VAL_11] ,ISNULL(a1.[VAL_12], 0)+ISNULL(a2.[VAL_12], 0) as [VAL_12] ,ISNULL(a1.[VAL_13], 0)+ISNULL(a2.[VAL_13], 0) as [VAL_13] ,ISNULL(a1.[VAL_14], 0)+ISNULL(a2.[VAL_14], 0) as [VAL_14] ,ISNULL(a1.[VAL_15], 0)+ISNULL(a2.[VAL_15], 0) as [VAL_15]
					,ISNULL(a1.[VAL_16], 0)+ISNULL(a2.[VAL_16], 0) as [VAL_16] ,ISNULL(a1.[VAL_17], 0)+ISNULL(a2.[VAL_17], 0) as [VAL_17] ,ISNULL(a1.[VAL_18], 0)+ISNULL(a2.[VAL_18], 0) as [VAL_18] ,ISNULL(a1.[VAL_19], 0)+ISNULL(a2.[VAL_19], 0) as [VAL_19] ,ISNULL(a1.[VAL_20], 0)+ISNULL(a2.[VAL_20], 0) as [VAL_20]
					,ISNULL(a1.[VAL_21], 0)+ISNULL(a2.[VAL_21], 0) as [VAL_21] ,ISNULL(a1.[VAL_22], 0)+ISNULL(a2.[VAL_22], 0) as [VAL_22] ,ISNULL(a1.[VAL_23], 0)+ISNULL(a2.[VAL_23], 0) as [VAL_23] ,ISNULL(a1.[VAL_24], 0)+ISNULL(a2.[VAL_24], 0) as [VAL_24] ,ISNULL(a1.[VAL_25], 0)+ISNULL(a2.[VAL_25], 0) as [VAL_25]
					,ISNULL(a1.[VAL_26], 0)+ISNULL(a2.[VAL_26], 0) as [VAL_26] ,ISNULL(a1.[VAL_27], 0)+ISNULL(a2.[VAL_27], 0) as [VAL_27] ,ISNULL(a1.[VAL_28], 0)+ISNULL(a2.[VAL_28], 0) as [VAL_28] ,ISNULL(a1.[VAL_29], 0)+ISNULL(a2.[VAL_29], 0) as [VAL_29] ,ISNULL(a1.[VAL_30], 0)+ISNULL(a2.[VAL_30], 0) as [VAL_30] 
					,ISNULL(a1.[VAL_31], 0)+ISNULL(a2.[VAL_31], 0) as [VAL_31] ,ISNULL(a1.[VAL_32], 0)+ISNULL(a2.[VAL_32], 0) as [VAL_32] ,ISNULL(a1.[VAL_33], 0)+ISNULL(a2.[VAL_33], 0) as [VAL_33] ,ISNULL(a1.[VAL_34], 0)+ISNULL(a2.[VAL_34], 0) as [VAL_34] ,ISNULL(a1.[VAL_35], 0)+ISNULL(a2.[VAL_35], 0) as [VAL_35]
					,ISNULL(a1.[VAL_36], 0)+ISNULL(a2.[VAL_36], 0) as [VAL_36] ,ISNULL(a1.[VAL_37], 0)+ISNULL(a2.[VAL_37], 0) as [VAL_37] ,ISNULL(a1.[VAL_38], 0)+ISNULL(a2.[VAL_38], 0) as [VAL_38] ,ISNULL(a1.[VAL_39], 0)+ISNULL(a2.[VAL_39], 0) as [VAL_39] ,ISNULL(a1.[VAL_40], 0)+ISNULL(a2.[VAL_40], 0) as [VAL_40] 
					,ISNULL(a1.[VAL_41], 0)+ISNULL(a2.[VAL_41], 0) as [VAL_41] ,ISNULL(a1.[VAL_42], 0)+ISNULL(a2.[VAL_42], 0) as [VAL_42] ,ISNULL(a1.[VAL_43], 0)+ISNULL(a2.[VAL_43], 0) as [VAL_43] ,ISNULL(a1.[VAL_44], 0)+ISNULL(a2.[VAL_44], 0) as [VAL_44] ,ISNULL(a1.[VAL_45], 0)+ISNULL(a2.[VAL_45], 0) as [VAL_45]
					,ISNULL(a1.[VAL_46], 0)+ISNULL(a2.[VAL_46], 0) as [VAL_46] ,ISNULL(a1.[VAL_47], 0)+ISNULL(a2.[VAL_47], 0) as [VAL_47] ,ISNULL(a1.[VAL_48], 0)+ISNULL(a2.[VAL_48], 0) as [VAL_48] 
				from (select * from @archives where TI_ID = hl.ti_id and ChannelType = 1) a1 
				full outer join (select * from @archives where TI_ID = hl.ti_id and ChannelType = 2) a2 on a2.TI_ID = a1.TI_ID and a2.EventDate = a1.EventDate
			) as arh
			unpivot 
			([Val] for ValueRow in (
			[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10] ,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
					,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30] ,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
					,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48]
			)) as unpvt
		) a
		where [Val]>0 and EventDate>= @startDateTime and EventDate<=@finishDateTime and not exists (select ti_id from #linkedOv lo where lo.ti_id = hl.ti_id and EventDate between StartDateTime and FinishDateTime)
	) arh
	where hl.isCa = 0
	group by hl.TI_ID
) hti
join #ovs ti on hti.TI_ID = ti.TI_ID
order by ti.PS_ID, hti.ti_id

if (not @Section_ID is null) begin 
	select ti.Contrti_id, Contr_ps_id,StartDate,FinishDate from 
		(
			select distinct hl.ti_id,Min(EventDate) as StartDate, Max(EventDate) as FinishDate from #ovs hl
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
					(select * from dbo.ArchComm_Contr_30_Import_From_XML where ContrTI_ID = hl.ti_id and ChannelType = 1 and (EventDate between floor(cast(@startDateTime as float)) and floor(cast(@finishDateTime as float))))  ch1
				 inner join 
					(select * from dbo.ArchComm_Contr_30_Import_From_XML where ContrTI_ID = hl.ti_id and ChannelType = 2 and (EventDate between floor(cast(@startDateTime as float)) and floor(cast(@finishDateTime as float))))  ch2
					on ch1.Contrti_id = ch2.Contrti_id and ch1.EventDate = ch2.EventDate 
		) as arh
		unpivot 
		([Val] for ValueRow in (
		[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10] ,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
				,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30] ,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
				,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48]
		)) as unpvt
		) a
		where [Val]>0 and EventDate>=@startDateTime and EventDate<@finishDateTime
		) arh
		where hl.isCa = 1
		group by hl.TI_ID
	)  hti
join Info_Contr_TI ti on hti.TI_ID = ti.ContrTI_ID
order by ti.Contr_PS_ID, ti.Contrti_id;
end
--select * from #archives

drop table #ovs
drop table #linkedOv

go
   grant EXECUTE on usp2_ArchComm_OV_Statistic to [UserCalcService]
go