if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_FindRecommendetPeriodForOV')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_FindRecommendetPeriodForOV
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
--		Август, 2009
--
-- Описание:
--
--		Возвращаем рекомендуемый период для распределения ОВ
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_FindRecommendetPeriodForOV]


	@DayMonthYear DateTime, -- Дата на которое мы запрашиваем
	@ov_ti_id int			-- Идентификатор ОВ
as

begin

	declare
	@StartDate DateTime, 
	@FinishDate DateTime,
	@ov_id int,
	@NextDayMonthYear DateTime



	set @NextDayMonthYear = DateAdd(day,1,@DayMonthYear)
	set @ov_id = (select top 1 OV_ID from dbo.Hard_OV_List where ti_id = @ov_ti_id)

	--Ищем провал в распределении
	if (exists (select OV_ID from dbo.ArchComm_OV_Switches where @ov_id = OV_ID and (StartDateTime <=  @DayMonthYear and FinishDateTime > DateAdd(minute,30,@DayMonthYear)))) begin
		--Если попали на распределенный период 
		set @StartDate = (select dbo.usf2_Utils_DateTimeRoundToHalfHour(Min(FinishDateTime),0) from dbo.ArchComm_OV_Switches where OV_ID = @ov_id and FinishDateTime > @DayMonthYear)
		set @FinishDate =  ISNULL(DateAdd(minute, -31,(select dbo.usf2_Utils_DateTimeRoundToHalfHour(Min(StartDateTime),1) from dbo.ArchComm_OV_Switches where OV_ID = @ov_id and StartDateTime >= @StartDate)), GetDate())
		print 1
	end else begin
		--Мы точно не в периоде распределения
		set @StartDate = ISNULL((select dbo.usf2_Utils_DateTimeRoundToHalfHour(Max(FinishDateTime),0) from dbo.ArchComm_OV_Switches where OV_ID = @ov_id and FinishDateTime <= @DayMonthYear), DateAdd(year, -1, @DayMonthYear))
		set @FinishDate =  ISNULL(DateAdd(minute, -31,(select dbo.usf2_Utils_DateTimeRoundToHalfHour(Min(StartDateTime),1) from dbo.ArchComm_OV_Switches where OV_ID = @ov_id and StartDateTime > @DayMonthYear)), GetDate())
		print 2
	end

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

declare @tiType tinyint, @absentChannelsMask tinyint;
select top 1 @tiType = TIType, @absentChannelsMask = ISNULL(AbsentChannelsMask, 0) from Info_TI where TI_ID = @ov_ti_id;

--select @TI_ID, @tiType, @absentChannelsMask
	if ((@absentChannelsMask & 1) = 0)
		insert @archives
		exec usp2_Arch_30_Select @ov_ti_id, @StartDate,@FinishDate,1,@tiType,null,null,0, 0, 0
		
	if (((@absentChannelsMask / 2)  & 1) = 0)
		insert @archives
		exec usp2_Arch_30_Select @ov_ti_id, @StartDate,@FinishDate,2,@tiType,null,null,0, 0, 0

	--select * from @archives

	--Создаем промежуточную таблицу с данными за этот период
	create table #TmpResult (EventDate DateTime,[Val] float);
	
	insert into #TmpResult
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
				from (select * from @archives where TI_ID = @ov_ti_id and ChannelType = 1) a1   
				full outer join (select * from @archives where TI_ID = @ov_ti_id and ChannelType = 2) a2 on a2.TI_ID = a1.TI_ID and a2.EventDate = a1.EventDate
			) as arh
			unpivot 
			([Val] for ValueRow in (
			[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10] ,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
					,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30] ,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
					,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48]
			)) as unpvt
	where DateAdd(minute,(SUBSTRING(ValueRow,5,2)-1) * 30,EventDate) between @StartDate and @FinishDate
	order by EventDate

	--select @StartDate, @FinishDate
	--select * from #TmpResult
	
	--Рассматриваем случай когда на дату запроса у нас 0 или мы попали в распределенную область
	if ((select top 1 [Val]  from #TmpResult where EventDate between @DayMonthYear and DateAdd(minute, 29,@DayMonthYear))=0) begin
	--Когда просто в области 0
		set @StartDate = ISNULL((select Min(EventDate) from #TmpResult where [Val]<>0 and EventDate>@DayMonthYear),@DayMonthYear)
		set @FinishDate = ISNULL(DateAdd(minute,-30,(select Min(EventDate) from #TmpResult where [Val]=0 and EventDate between @StartDate and @FinishDate)),@FinishDate)
	end else begin
	--Случай когда у нас начинаются с ненулевых значений
		set @StartDate = ISNULL(DateAdd(minute,30,(select Max(EventDate) from #TmpResult where [Val]=0 and EventDate<@DayMonthYear)),@StartDate)
		set @FinishDate = ISNULL(DateAdd(minute,-30,(select Min(EventDate) from #TmpResult where [Val]=0 and EventDate>@DayMonthYear)),@FinishDate) 
	end
	
	drop table #TmpResult

	select @StartDate as RecommendetStartDate, @FinishDate as RecommendetFinishDate
	
	
end
go
   grant EXECUTE on usp2_ArchComm_FindRecommendetPeriodForOV to [UserCalcService]
go