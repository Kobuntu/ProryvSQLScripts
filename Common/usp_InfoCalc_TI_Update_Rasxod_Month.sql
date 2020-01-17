if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InfoCalc_TI_Update_Rasxod_Month')
          and type in ('P','PC'))
   drop procedure usp2_InfoCalc_TI_Update_Rasxod_Month
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2009
--
-- Описание:
--
--		Обновяем расход за месяц по группе точек (малые ТИ)
--
-- ======================================================================================

create proc [dbo].[usp2_InfoCalc_TI_Update_Rasxod_Month]
	@TI_Array varchar(4000),-- Идентификатор ТИ , время, значения АО и АП разделенные запятой
	@DispatchDateTime DateTime, --Время когда запись была вставлена
	@CUS_ID tinyint,  --Идентификатор пользователя
	@ChannelType tinyint, -- Номер канала
	@PlanFact tinyint,		--  План, факт
	@IsWs bit, --Учитывать отмену зимнего времени или писать как есть
	@BaseOffsetClientFromServer int, --Базовое смещение клиента относительно сервера в минутах
	@User_ID varchar(22) --Идентификатор пользователя
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@MonthYear DateTime,
@FVAL float(53), --расход
@old_TI int,
@Channel int

---ТИ, данные которые надо сохранить по этим ТИ
select usf.TI_ID as [ti_id], usf.EventDate,usf.Float1 as [Float1], fl.TI_id as [ti_id_old], @ChannelType as [ChannelType]
		into #tis
		from usf2_Utils_iter_floatlist_to_table(@TI_Array) usf
		left join dbo.InfoCalc_TI_Month_Rasxod_Month fl 
		on usf.TI_ID = fl.TI_ID and usf.EventDate = fl.MonthYear and fl.ChannelType = @ChannelType and fl.PlanFact = @PlanFact

--Таблица, которую пишем 
create table #source 
(
		[ti_id] int,
		[ChannelType] tinyint,
		[EventDate] DateTime, 
		[CalendarFloat] decimal(38,10),
		[IsCoeffTransformationDisabled] bit,
		PRIMARY KEY CLUSTERED([ti_id], [ChannelType], [EventDate])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)

create table #calendar 
(
	TI_ID int, CalendarDayDate DateTime, DayFloat float(53), CD_UN varchar(22),
	PRIMARY KEY CLUSTERED([ti_id], CalendarDayDate)
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)

create table #daySummByTi
(
	TI_ID int, SumCalendar decimal(38,10), CalendarAverage decimal(38,10), CountDay int, TotalMonth decimal(38,10)
	,PRIMARY KEY CLUSTERED([ti_id])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)

--Заполняем суммами за день
insert into #calendar 
select t.TI_ID, usf.CalendarDayDate, 
--Если нет календарного дня то просто распределяем равномерно
ISNULL(d.VAL_01 + d.VAL_02 + d.VAL_03 + d.VAL_04 + d.VAL_05 + d.VAL_06 + d.VAL_07 + d.VAL_08 + d.VAL_09 + d.VAL_10 +
d.VAL_11 + d.VAL_12 + d.VAL_13 + d.VAL_14 + d.VAL_15 + d.VAL_16 + d.VAL_17 + d.VAL_18 + d.VAL_19 + d.VAL_20 +
d.VAL_21 + d.VAL_22 + d.VAL_23 + d.VAL_24 + d.VAL_25 + d.VAL_26 + d.VAL_27 + d.VAL_28 + d.VAL_29 + d.VAL_30 +
d.VAL_31 + d.VAL_32 + d.VAL_33 + d.VAL_34 + d.VAL_35 + d.VAL_36 + d.VAL_37 + d.VAL_38 + d.VAL_39 + d.VAL_40 +
d.VAL_41 + d.VAL_42 + d.VAL_43 + d.VAL_44 + d.VAL_45 + d.VAL_46 + d.VAL_47 + d.VAL_48 
+ case when @IsWs = 1 and usf.CalendarDayDate = '20141026' then d.VAL_03 + d.VAL_04 else 0 end, 
1 + case when @IsWs = 1 and usf.CalendarDayDate = '20141026' then 0.041666666666667 else 0 end) as DayFloat, ISNULL(dsc.CD_UN, '') as CD_UN
from #tis t
cross apply usf2_Utils_days_to_table(t.EventDate)  usf
outer apply
(
select top 1 * from dbo.InfoCalc_Character_Days_In_Calendar_Description dsc 
where dsc.CalendarDayDate = usf.CalendarDayDate and dsc.TI_ID = t.ti_id and dsc.ChannelType = @ChannelType 
and PlanFact = @PlanFact
) dsc
left join dbo.InfoCalc_Character_Days_Types d on  d.TI_ID = dsc.TI_ID and d.ChannelType = @ChannelType
	and d.CD_UN = dsc.CD_UN


--Считае удельный вес каждого дня, ложим свой расход в свой день
insert into #daySummByTi
select t.*, ti.Float1 from #tis ti
join 
(
select TI_ID, Sum(DayFloat) as SumCalendar, case when Count(DayFloat)=0 then 0 else Sum(DayFloat) /  Count(DayFloat) 
end as CalendarAverage, Count(DayFloat) as CountDay
from #calendar
group by TI_ID
) t
on ti.ti_id = t.TI_ID

--select * from #calendar
--select * from #daySummByTi

declare @Arch30WsValuesTable Arch30WsValuesType; --Переходная таблица отмены зимнего времени 2014 г.

declare
@CurrentDayFloat decimal(38,10),
@averageDayFloat decimal(38,10),
@totalMonth decimal(38,10),
@SumCalendar decimal(38,10),
@CalendarAverage decimal(38,10),
@countHhInDay int,
@countDay int,
@wsDelta float(53);

--select * from #calendar

declare 
@TI_ID int, @CalendarDayDate DateTime, @DayFloat float(53),
@VAL_01 float(53),@VAL_02 float(53),@VAL_03 float(53),@VAL_04 float(53),@VAL_05 float(53),@VAL_06 float(53),@VAL_07 float(53),@VAL_08 float(53),@VAL_09 float(53),@VAL_10 float(53),  
@VAL_11 float(53),@VAL_12 float(53),@VAL_13 float(53),@VAL_14 float(53),@VAL_15 float(53),@VAL_16 float(53),@VAL_17 float(53),@VAL_18 float(53),@VAL_19 float(53),@VAL_20 float(53),  
@VAL_21 float(53),@VAL_22 float(53),@VAL_23 float(53),@VAL_24 float(53),@VAL_25 float(53),@VAL_26 float(53),@VAL_27 float(53),@VAL_28 float(53),@VAL_29 float(53),@VAL_30 float(53), 
@VAL_31 float(53),@VAL_32 float(53),@VAL_33 float(53),@VAL_34 float(53),@VAL_35 float(53),@VAL_36 float(53),@VAL_37 float(53),@VAL_38 float(53),@VAL_39 float(53),@VAL_40 float(53),  
@VAL_41 float(53),@VAL_42 float(53),@VAL_43 float(53),@VAL_44 float(53),@VAL_45 float(53),@VAL_46 float(53),@VAL_47 float(53),@VAL_48 float(53), @IsCoeffTransformationDisabled bit;

declare ti cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TI_ID, SumCalendar, CalendarAverage, TotalMonth, CountDay from #daySummByTi
open ti;
FETCH NEXT FROM ti into @TI_ID,@SumCalendar,@CalendarAverage, @totalMonth, @countDay
WHILE @@FETCH_STATUS = 0
begin
	set @IsCoeffTransformationDisabled = (select top 1 IsCoeffTransformationDisabled from Info_TI where ti_ID = @TI_ID);

	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct 
	t.TI_ID, t.[CalendarDayDate], t.DayFloat,
	d.VAL_01,d.VAL_02,d.VAL_03,d.VAL_04,d.VAL_05,d.VAL_06,d.VAL_07,d.VAL_08,d.VAL_09,d.VAL_10,  
	d.VAL_11,d.VAL_12,d.VAL_13,d.VAL_14,d.VAL_15,d.VAL_16,d.VAL_17,d.VAL_18,d.VAL_19,d.VAL_20,  
	d.VAL_21,d.VAL_22,d.VAL_23,d.VAL_24,d.VAL_25,d.VAL_26,d.VAL_27,d.VAL_28,d.VAL_29,d.VAL_30, 
	d.VAL_31,d.VAL_32,d.VAL_33,d.VAL_34,d.VAL_35,d.VAL_36,d.VAL_37,d.VAL_38,d.VAL_39,d.VAL_40,  
	d.VAL_41,d.VAL_42,d.VAL_43,d.VAL_44,d.VAL_45,d.VAL_46,d.VAL_47,d.VAL_48 from #calendar t 
	left join dbo.InfoCalc_Character_Days_In_Calendar_Description dsc on dsc.CalendarDayDate = t.CalendarDayDate
	left join dbo.InfoCalc_Character_Days_Types d  
	on d.TI_ID = t.TI_ID and d.ChannelType = @ChannelType and t.CD_UN = d.CD_UN and t.[CalendarDayDate] = dsc.[CalendarDayDate]
	where t.TI_ID = @TI_ID
	open t;
	FETCH NEXT FROM t into @TI_ID,@CalendarDayDate,@DayFloat, 
	@VAL_01,@VAL_02,@VAL_03,@VAL_04,@VAL_05,@VAL_06,@VAL_07,@VAL_08,@VAL_09,@VAL_10,  
	@VAL_11,@VAL_12,@VAL_13,@VAL_14,@VAL_15,@VAL_16,@VAL_17,@VAL_18,@VAL_19,@VAL_20,  
	@VAL_21,@VAL_22,@VAL_23,@VAL_24,@VAL_25,@VAL_26,@VAL_27,@VAL_28,@VAL_29,@VAL_30, 
	@VAL_31,@VAL_32,@VAL_33,@VAL_34,@VAL_35,@VAL_36,@VAL_37,@VAL_38,@VAL_39,@VAL_40,  
	@VAL_41,@VAL_42,@VAL_43,@VAL_44,@VAL_45,@VAL_46,@VAL_47,@VAL_48
	WHILE @@FETCH_STATUS = 0
	BEGIN

		if (@SumCalendar * @totalMonth)=0 set @CurrentDayFloat = 0;
		else set @CurrentDayFloat = ISNULL(@DayFloat, @CalendarAverage) / @SumCalendar * @totalMonth;

		if (@IsWs=1 and @CalendarDayDate = '20141026') begin

			set @averageDayFloat = @CurrentDayFloat/50;

			set @VAL_03 = case when @VAL_03 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_03/@DayFloat * @CurrentDayFloat end;
			set @VAL_04 = case when @VAL_04 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_04/@DayFloat * @CurrentDayFloat end;

			--Добавляем переходную получасовку
			insert into @Arch30WsValuesTable (TI_ID, ChannelType, EventDate, DataSourceType, CAL_03, CAL_04, DispatchDateTime, CUS_ID, ValidStatus)
			values (@TI_ID, @ChannelType, '20141026', 0, @VAL_03, @VAL_04,@DispatchDateTime, 0, 0);
		end else begin 

			set @averageDayFloat = @CurrentDayFloat/48;
			set @VAL_03 = case when @VAL_03 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_03/@DayFloat * @CurrentDayFloat end;
			set @VAL_04 = case when @VAL_04 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_04/@DayFloat * @CurrentDayFloat end;

		end

		--select @averageDayFloat

		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,60 + @BaseOffsetClientFromServer, @CalendarDayDate), @VAL_03, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,90 + @BaseOffsetClientFromServer, @CalendarDayDate), @VAL_04, @IsCoeffTransformationDisabled);
		
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,@BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_01 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_01/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,30 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_02 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_02/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,120 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_05 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_05/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,150 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_06 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_06/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,180 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_07 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_07/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,210 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_08 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_08/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,240 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_09 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_09/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,270 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_10 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_10/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,300 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_11 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_11/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,330 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_12 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_12/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,360 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_13 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_13/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,390 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_14 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_14/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,420 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_15 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_15/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,450 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_16 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_16/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,480 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_17 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_17/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,510 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_18 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_18/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,540 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_19 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_19/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,570 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_20 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_20/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,600 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_21 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_21/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,630 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_22 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_22/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,660 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_23 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_23/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,690 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_24 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_24/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,720 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_25 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_25/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,750 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_26 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_26/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,780 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_27 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_27/@DayFloat * @CurrentDayFloat  end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,810 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_28 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_28/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,840 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_29 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_29/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,870 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_30 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_30/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,900 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_31 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_31/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,930 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_32 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_32/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,960 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_33 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_33/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,990 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_34 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_34/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1020 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_35 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_35/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1050 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_36 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_36/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1080 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_37 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_37/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1110 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_38 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_38/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1140 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_39 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_39/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1170 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_40 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_40/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1200 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_41 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_41/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1230 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_42 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_42/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1260 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_43 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_43/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1290 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_44 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_44/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1320 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_45 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_45/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1350 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_46 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_46/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1380 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_47 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_47/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		insert into #source values (@TI_ID, @ChannelType, DateAdd(n,1410 + @BaseOffsetClientFromServer, @CalendarDayDate), 
		case when @VAL_48 is null then @averageDayFloat when @DayFloat=0 then 0 else @VAL_48/@DayFloat * @CurrentDayFloat end, @IsCoeffTransformationDisabled);
		FETCH NEXT FROM t into @TI_ID,@CalendarDayDate,@DayFloat,
			@VAL_01,@VAL_02,@VAL_03,@VAL_04,@VAL_05,@VAL_06,@VAL_07,@VAL_08,@VAL_09,@VAL_10,  
			@VAL_11,@VAL_12,@VAL_13,@VAL_14,@VAL_15,@VAL_16,@VAL_17,@VAL_18,@VAL_19,@VAL_20,  
			@VAL_21,@VAL_22,@VAL_23,@VAL_24,@VAL_25,@VAL_26,@VAL_27,@VAL_28,@VAL_29,@VAL_30, 
			@VAL_31,@VAL_32,@VAL_33,@VAL_34,@VAL_35,@VAL_36,@VAL_37,@VAL_38,@VAL_39,@VAL_40,  
			@VAL_41,@VAL_42,@VAL_43,@VAL_44,@VAL_45,@VAL_46,@VAL_47,@VAL_48 
	END
	CLOSE t
	DEALLOCATE t

	FETCH NEXT FROM ti into @TI_ID,@SumCalendar,@CalendarAverage, @totalMonth, @countDay
END
CLOSE ti
DEALLOCATE ti

	drop table #calendar
	drop table #daySummByTi
	
	--Делим на коэфф. трансформации
	update #source 
	set CalendarFloat = CalendarFloat / cast(isnull((select top 1 COEFI*COEFU 
												from Info_Transformators
												where TI_ID=s.TI_ID and EventDate between StartDateTime and isnull(FinishDateTime, '21000101')), 1) as float(53))
	from #source s
	where s.IsCoeffTransformationDisabled=0

	--Поворачиваем таблицу на 90 градусов
	select PivotTable.* 
		into #Tmp
		from 
		(
			select ti_id, ChannelType as Channel, Cast(floor(cast(EventDate as float)) as DateTime) as EventDate, DateDiff(n,floor(cast(EventDate as float)),EventDate) as HalfHours, CalendarFloat
			from #source
		) AS SourceTable
		PIVOT
		(
			avg([CalendarFloat]) FOR [HalfHours] IN ([0],[30],[60],[90],[120],[150],[180],[210],[240],[270],[300],[330],[360],[390],[420],[450],[480],[510],[540],[570],[600],[630],[660],[690],[720],[750],[780],[810],[840],[870],[900],[930],[960],[990],[1020],[1050],[1080],[1110],[1140],[1170],[1200],[1230],[1260],[1290],[1320],[1350],[1380],[1410])
		) AS PivotTable
		
		--select * from #tmp
		--select * from @Arch30WsValuesTable
		--return
	
	drop table #source
	BEGIN TRY  BEGIN TRANSACTION

	--select * from #Tmp

	---------------Обновляем таблицу архивов получасовок-------------------------------------------------------	  
	MERGE ArchCalc_30_Month_Values WITH (HOLDLOCK) as a
	using #tmp fl
	on a.TI_ID = fl.TI_ID and a.EventDate = fl.EventDate and a.ChannelType = fl.Channel and PlanFact = @PlanFact
	WHEN MATCHED THEN
	update set
	---Обновляем поля котрые уже существуют
		VAL_01 = (case when not fl.[0]    is null then fl.[0]    else VAL_01 end),
		VAL_02 = (case when not fl.[30]   is null then fl.[30]   else VAL_02 end),
		VAL_03 = (case when not fl.[60]   is null then fl.[60]   else VAL_03 end),
		VAL_04 = (case when not fl.[90]   is null then fl.[90]   else VAL_04 end),
		VAL_05 = (case when not fl.[120]  is null then fl.[120]  else VAL_05 end),
		VAL_06 = (case when not fl.[150]  is null then fl.[150]  else VAL_06 end),
		VAL_07 = (case when not fl.[180]  is null then fl.[180]  else VAL_07 end),
		VAL_08 = (case when not fl.[210]  is null then fl.[210]  else VAL_08 end),
		VAL_09 = (case when not fl.[240]  is null then fl.[240]  else VAL_09 end),
		VAL_10 = (case when not fl.[270]  is null then fl.[270]  else VAL_10 end),
		VAL_11 = (case when not fl.[300]  is null then fl.[300]  else VAL_11 end), 
		VAL_12 = (case when not fl.[330]  is null then fl.[330]  else VAL_12 end),
		VAL_13 = (case when not fl.[360]  is null then fl.[360]  else VAL_13 end),
		VAL_14 = (case when not fl.[390]  is null then fl.[390]  else VAL_14 end),
		VAL_15 = (case when not fl.[420]  is null then fl.[420]  else VAL_15 end),
		VAL_16 = (case when not fl.[450]  is null then fl.[450]  else VAL_16 end),
		VAL_17 = (case when not fl.[480]  is null then fl.[480]  else VAL_17 end),
		VAL_18 = (case when not fl.[510]  is null then fl.[510]  else VAL_18 end),
		VAL_19 = (case when not fl.[540]  is null then fl.[540]  else VAL_19 end),
		VAL_20 = (case when not fl.[570]  is null then fl.[570]  else VAL_20 end),
		VAL_21 = (case when not fl.[600]  is null then fl.[600]  else VAL_21 end), 
		VAL_22 = (case when not fl.[630]  is null then fl.[630]  else VAL_22 end),
		VAL_23 = (case when not fl.[660]  is null then fl.[660]  else VAL_23 end),
		VAL_24 = (case when not fl.[690]  is null then fl.[690]  else VAL_24 end),
		VAL_25 = (case when not fl.[720]  is null then fl.[720]  else VAL_25 end),
		VAL_26 = (case when not fl.[750]  is null then fl.[750]  else VAL_26 end),
		VAL_27 = (case when not fl.[780]  is null then fl.[780]  else VAL_27 end),
		VAL_28 = (case when not fl.[810]  is null then fl.[810]  else VAL_28 end),
		VAL_29 = (case when not fl.[840]  is null then fl.[840]  else VAL_29 end),
		VAL_30 = (case when not fl.[870]  is null then fl.[870]  else VAL_30 end),
		VAL_31 = (case when not fl.[900]  is null then fl.[900]  else VAL_31 end),
		VAL_32 = (case when not fl.[930]  is null then fl.[930]  else VAL_32 end),
		VAL_33 = (case when not fl.[960]  is null then fl.[960]  else VAL_33 end),
		VAL_34 = (case when not fl.[990]  is null then fl.[990]  else VAL_34 end),
		VAL_35 = (case when not fl.[1020] is null then fl.[1020] else VAL_35 end),
		VAL_36 = (case when not fl.[1050] is null then fl.[1050] else VAL_36 end),
		VAL_37 = (case when not fl.[1080] is null then fl.[1080] else VAL_37 end),
		VAL_38 = (case when not fl.[1110] is null then fl.[1110] else VAL_38 end),
		VAL_39 = (case when not fl.[1140] is null then fl.[1140] else VAL_39 end),
		VAL_40 = (case when not fl.[1170] is null then fl.[1170] else VAL_40 end),
		VAL_41 = (case when not fl.[1200] is null then fl.[1200] else VAL_41 end),
		VAL_42 = (case when not fl.[1230] is null then fl.[1230] else VAL_42 end),
		VAL_43 = (case when not fl.[1260] is null then fl.[1260] else VAL_43 end),
		VAL_44 = (case when not fl.[1290] is null then fl.[1290] else VAL_44 end),
		VAL_45 = (case when not fl.[1320] is null then fl.[1320] else VAL_45 end),
		VAL_46 = (case when not fl.[1350] is null then fl.[1350] else VAL_46 end),
		VAL_47 = (case when not fl.[1380] is null then fl.[1380] else VAL_47 end),
		VAL_48 = (case when not fl.[1410] is null then fl.[1410] else VAL_48 end),
		DispatchDateTime = @DispatchDateTime,
		CUS_ID=@CUS_ID	WHEN NOT MATCHED THEN
	---Добавляем поля которых нет 
		insert	--ArchCalc_30_Month_Values
		(
			  [TI_ID]
			  ,[EventDate]
			  ,[ChannelType]
			  ,[PlanFact]
			  ,[VAL_01]
			  ,[VAL_02]
			  ,[VAL_03]
			  ,[VAL_04]
			  ,[VAL_05]
			  ,[VAL_06]
			  ,[VAL_07]
			  ,[VAL_08]
			  ,[VAL_09]
			  ,[VAL_10]
			  ,[VAL_11]
			  ,[VAL_12]
			  ,[VAL_13]
			  ,[VAL_14]
			  ,[VAL_15]
			  ,[VAL_16]
			  ,[VAL_17]
			  ,[VAL_18]
			  ,[VAL_19]
			  ,[VAL_20]
			  ,[VAL_21]
			  ,[VAL_22]
			  ,[VAL_23]
			  ,[VAL_24]
			  ,[VAL_25]
			  ,[VAL_26]
			  ,[VAL_27]
			  ,[VAL_28]
			  ,[VAL_29]
			  ,[VAL_30]
			  ,[VAL_31]
			  ,[VAL_32]
			  ,[VAL_33]
			  ,[VAL_34]
			  ,[VAL_35]
			  ,[VAL_36]
			  ,[VAL_37]
			  ,[VAL_38]
			  ,[VAL_39]
			  ,[VAL_40]
			  ,[VAL_41]
			  ,[VAL_42]
			  ,[VAL_43]
			  ,[VAL_44]
			  ,[VAL_45]
			  ,[VAL_46]
			  ,[VAL_47]
			  ,[VAL_48]
			  ,[ValidStatus]
			  ,[DispatchDateTime]
			  ,[CUS_ID]
			  ,[Status]
		) 
		values (fl.ti_id, fl.EventDate,fl.Channel, @PlanFact, 
		fl.[0],   
		fl.[30],  
		fl.[60],  
		fl.[90],  
		fl.[120], 
		fl.[150], 
		fl.[180], 
		fl.[210], 
		fl.[240], 
		fl.[270], 
		fl.[300], 
		fl.[330], 
		fl.[360], 
		fl.[390], 
		fl.[420], 
		fl.[450], 
		fl.[480], 
		fl.[510], 
		fl.[540], 
		fl.[570], 
		fl.[600], 
		fl.[630], 
		fl.[660], 
		fl.[690], 
		fl.[720], 
		fl.[750], 
		fl.[780], 
		fl.[810], 
		fl.[840], 
		fl.[870], 
		fl.[900], 
		fl.[930], 
		fl.[960], 
		fl.[990], 
		fl.[1020],
		fl.[1050],
		fl.[1080],
		fl.[1110],
		fl.[1140],
		fl.[1170],
		fl.[1200],
		fl.[1230],
		fl.[1260],
		fl.[1290],
		fl.[1320],
		fl.[1350],
		fl.[1380],
		fl.[1410], 0, @DispatchDateTime, @CUS_ID,0);
		
		--Пишем в таблицу отмены зимнего времени 2014г.
		if (exists (select top 1 1 from @Arch30WsValuesTable)) exec usp2_WriteArch30WsValues @Arch30WsValuesTable, 0
		

		------Пишем в журнал событий-------------------------------------
		declare @ZamerDateTime DateTime, @EventParam tinyint, @Float1 float; 

		set @EventParam = 137 --

		IF Cursor_Status('variable', 'cc_') > 0 begin 
			CLOSE cc_
			DEALLOCATE cc_
		end

		declare cc_ cursor for select Convert(int, ti_id), Convert(datetime, [EventDate]) as ZamerDateTime, [Float1]
		from #tis t
		
		open cc_;
		FETCH NEXT FROM cc_ into @ti_ID, @ZamerDateTime,@Float1
		WHILE @@FETCH_STATUS = 0
		BEGIN
		--@CUS_ID as cus_id,@DispatchDateTime as E`ventDateTime,@EventParam as EventParam,@CommentString,
			if not exists (select TI_ID,ChannelType,EventDate,[User_ID],EventDateTime from dbo.Expl_User_Journal_Replace_30_Virtual vv 
			where vv.ti_id = @ti_id and vv.ChannelType = @ChannelType and vv.EventDate = convert(smalldatetime, @ZamerDateTime) and vv.[User_ID] = @User_ID and vv.EventDateTime = @DispatchDateTime) 
			begin 
				insert dbo.Expl_User_Journal_Replace_30_Virtual (TI_ID, ChannelType, EventDate, [User_ID], EventDateTime, EventParam, CUS_ID, ZamerDateTime, CommentString)
				select @ti_ID, @ChannelType,convert(smalldatetime, @ZamerDateTime) as EventDate, @User_ID, @DispatchDateTime, @EventParam, @CUS_ID, 
				DateAdd(minute, -30, DateAdd(month, 1, @ZamerDateTime)), 'Значение(Вт*ч): ' + ltrim(str(@Float1, 28, 3))
			end else begin 
				update dbo.Expl_User_Journal_Replace_30_Virtual
				set  ti_ID = @ti_ID, ChannelType = @ChannelType,EventDate = convert(smalldatetime, @ZamerDateTime), 
				[User_ID] = @User_ID, EventDateTime =@DispatchDateTime, EventParam = @EventParam, CUS_ID = @CUS_ID, 
				ZamerDateTime = DateAdd(minute, -30, DateAdd(month, 1, @ZamerDateTime)), CommentString = 'Значение(Вт*ч):' + ltrim(str(@Float1, 28, 3))
				where Expl_User_Journal_Replace_30_Virtual.ti_id = @ti_id and Expl_User_Journal_Replace_30_Virtual.ChannelType = @ChannelType and Expl_User_Journal_Replace_30_Virtual.EventDate = convert(smalldatetime, @ZamerDateTime)
					and Expl_User_Journal_Replace_30_Virtual.[User_ID] = @User_ID and Expl_User_Journal_Replace_30_Virtual.EventDateTime = @DispatchDateTime 
			end
			FETCH NEXT FROM cc_ into @ti_ID, @ZamerDateTime,@Float1
		END
		CLOSE cc_
		DEALLOCATE cc_

		drop table #tis

	COMMIT
	END TRY	
	BEGIN CATCH
		drop table #TMP
		IF @@TRANCOUNT > 0 ROLLBACK 
		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 10 
		SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH
end

go
   grant EXECUTE on usp2_InfoCalc_TI_Update_Rasxod_Month to [UserCalcService]
go