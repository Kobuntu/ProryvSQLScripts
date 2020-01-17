if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_MonthValues_to_table')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_MonthValues_to_table
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
--		Функция возвращает таблицу получасовок распределнных по расходу за месяц;
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_MonthValues_to_table] (
		@TI_ID   int, 
		@ChannelType tinyint, -- Номер канала
		@PlanFact tinyint,		--  План, факт
		@FVAL float, --расход
		@MonthYear DateTime
)
      RETURNS @tbl TABLE (
		[ti_id] int,
		[CD_UN] varchar(22),
		[CalendarDayDate]DateTime, --Дата календаря
		[DayFloat] float, -- Календарный расход на данный день
		[CurrentDayFloat] float, --Удельный вес данного дня относительно других дней (%)
		--Получасовки
		[00] float,[01] float,[02] float,[03] float,[04] float,[05] float,[06] float,[07] float,[08] float,[09] float,
		[10] float,[11] float,[12] float,[13] float,[14] float,[15] float,[16] float,[17] float,[18] float,[19] float,
		[20] float,[21] float,[22] float,[23] float,[24] float,[25] float,[26] float,[27] float,[28] float,[29] float,
		[30] float,[31] float,[32] float,[33] float,[34] float,[35] float,[36] float,[37] float,[38] float,[39] float,
		[40] float,[41] float,[42] float,[43] float,[44] float,[45] float,[46] float,[47] float
		PRIMARY KEY CLUSTERED ([CalendarDayDate], [ti_id])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
) AS
   BEGIN
      DECLARE 
			@CalendarAverage float,
			@CurrentAverage float,
			@SumCalendar float

      --Заполняем суммами за день
insert @tbl ([ti_id],[CalendarDayDate], [DayFloat], [CD_UN])
select @TI_ID,usf.CalendarDayDate,ISNULL( --Если нет календарного дня то просто распределяем равномерно
d.VAL_01 + d.VAL_02 + d.VAL_03 + d.VAL_04 + d.VAL_05 + d.VAL_06 + d.VAL_07 + d.VAL_08 + d.VAL_09 + d.VAL_10 +
d.VAL_11 + d.VAL_12 + d.VAL_13 + d.VAL_14 + d.VAL_15 + d.VAL_16 + d.VAL_17 + d.VAL_18 + d.VAL_19 + d.VAL_20 +
d.VAL_21 + d.VAL_22 + d.VAL_23 + d.VAL_24 + d.VAL_25 + d.VAL_26 + d.VAL_27 + d.VAL_28 + d.VAL_29 + d.VAL_30 +
d.VAL_31 + d.VAL_32 + d.VAL_33 + d.VAL_34 + d.VAL_35 + d.VAL_36 + d.VAL_37 + d.VAL_38 + d.VAL_39 + d.VAL_40 +
d.VAL_41 + d.VAL_42 + d.VAL_43 + d.VAL_44 + d.VAL_45 + d.VAL_46 + d.VAL_47 + d.VAL_48,1), ISNULL(dsc.CD_UN, '')
from 
(select * from dbo.InfoCalc_Character_Days_Types 
where TI_ID = @TI_ID and ChannelType = @ChannelType) d
join dbo.InfoCalc_Character_Days_In_Calendar_Description dsc
on  d.CD_UN = dsc.CD_UN and PlanFact = @PlanFact 
right join 
usf2_Utils_days_to_table(@MonthYear)  usf
on dsc.CalendarDayDate = usf.CalendarDayDate


--Расчитываем расход за день

set @CalendarAverage = (select case when Count(DayFloat)=0 then 0 else Sum(DayFloat) /  Count(DayFloat) end from  @tbl where not DayFloat is null) -- Средний расход календаря

update @tbl set DayFloat=@CalendarAverage  where DayFloat is null
set @SumCalendar = (select Sum(DayFloat) from  @tbl) -- Средний расход календаря

update @tbl set [CurrentDayFloat] = case when (@SumCalendar * @FVAL)=0 then 0 else ((
case 
	when DayFloat is null then @CalendarAverage 
	else DayFloat end) / @SumCalendar * @FVAL ) end
--Обновляем получасовки
update @tbl set 
[00] = (case when dd.VAL_01 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_01/[DayFloat] * [CurrentDayFloat] end),
[01] = (case when dd.VAL_02 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_02/[DayFloat] * [CurrentDayFloat] end),
[02] = (case when dd.VAL_03 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_03/[DayFloat] * [CurrentDayFloat] end),
[03] = (case when dd.VAL_04 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_04/[DayFloat] * [CurrentDayFloat] end),
[04] = (case when dd.VAL_05 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_05/[DayFloat] * [CurrentDayFloat] end),
[05] = (case when dd.VAL_06 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_06/[DayFloat] * [CurrentDayFloat] end),
[06] = (case when dd.VAL_07 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_07/[DayFloat] * [CurrentDayFloat] end),
[07] = (case when dd.VAL_08 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_08/[DayFloat] * [CurrentDayFloat] end),
[08] = (case when dd.VAL_09 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_09/[DayFloat] * [CurrentDayFloat] end),
[09] = (case when dd.VAL_10 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_10/[DayFloat] * [CurrentDayFloat] end),
[10] = (case when dd.VAL_11 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_11/[DayFloat] * [CurrentDayFloat] end),
[11] = (case when dd.VAL_12 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_12/[DayFloat] * [CurrentDayFloat] end),
[12] = (case when dd.VAL_13 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_13/[DayFloat] * [CurrentDayFloat] end),
[13] = (case when dd.VAL_14 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_14/[DayFloat] * [CurrentDayFloat] end),
[14] = (case when dd.VAL_15 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_15/[DayFloat] * [CurrentDayFloat] end),
[15] = (case when dd.VAL_16 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_16/[DayFloat] * [CurrentDayFloat] end),
[16] = (case when dd.VAL_17 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_17/[DayFloat] * [CurrentDayFloat] end),
[17] = (case when dd.VAL_18 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_18/[DayFloat] * [CurrentDayFloat] end),
[18] = (case when dd.VAL_19 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_19/[DayFloat] * [CurrentDayFloat] end),
[19] = (case when dd.VAL_20 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_20/[DayFloat] * [CurrentDayFloat] end),
[20] = (case when dd.VAL_21 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_21/[DayFloat] * [CurrentDayFloat] end),
[21] = (case when dd.VAL_22 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_22/[DayFloat] * [CurrentDayFloat] end),
[22] = (case when dd.VAL_23 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_23/[DayFloat] * [CurrentDayFloat] end),
[23] = (case when dd.VAL_24 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_24/[DayFloat] * [CurrentDayFloat] end),
[24] = (case when dd.VAL_25 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_25/[DayFloat] * [CurrentDayFloat] end),
[25] = (case when dd.VAL_26 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_26/[DayFloat] * [CurrentDayFloat] end),
[26] = (case when dd.VAL_27 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_27/[DayFloat] * [CurrentDayFloat] end),
[27] = (case when dd.VAL_28 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_28/[DayFloat] * [CurrentDayFloat] end),
[28] = (case when dd.VAL_29 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_29/[DayFloat] * [CurrentDayFloat] end),
[29] = (case when dd.VAL_30 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_30/[DayFloat] * [CurrentDayFloat] end),
[30] = (case when dd.VAL_31 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_31/[DayFloat] * [CurrentDayFloat] end),
[31] = (case when dd.VAL_32 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_32/[DayFloat] * [CurrentDayFloat] end),
[32] = (case when dd.VAL_33 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_33/[DayFloat] * [CurrentDayFloat] end),
[33] = (case when dd.VAL_34 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_34/[DayFloat] * [CurrentDayFloat] end),
[34] = (case when dd.VAL_35 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_35/[DayFloat] * [CurrentDayFloat] end),
[35] = (case when dd.VAL_36 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_36/[DayFloat] * [CurrentDayFloat] end),
[36] = (case when dd.VAL_37 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_37/[DayFloat] * [CurrentDayFloat] end),
[37] = (case when dd.VAL_38 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_38/[DayFloat] * [CurrentDayFloat] end),
[38] = (case when dd.VAL_39 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_39/[DayFloat] * [CurrentDayFloat] end),
[39] = (case when dd.VAL_40 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_40/[DayFloat] * [CurrentDayFloat] end),
[40] = (case when dd.VAL_41 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_41/[DayFloat] * [CurrentDayFloat] end),
[41] = (case when dd.VAL_42 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_42/[DayFloat] * [CurrentDayFloat] end),
[42] = (case when dd.VAL_43 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_43/[DayFloat] * [CurrentDayFloat] end),
[43] = (case when dd.VAL_44 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_44/[DayFloat] * [CurrentDayFloat] end),
[44] = (case when dd.VAL_45 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_45/[DayFloat] * [CurrentDayFloat] end),
[45] = (case when dd.VAL_46 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_46/[DayFloat] * [CurrentDayFloat] end),
[46] = (case when dd.VAL_47 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_47/[DayFloat] * [CurrentDayFloat] end),
[47] = (case when dd.VAL_48 is null then [CurrentDayFloat]/48 when DayFloat=0 then 0 else dd.VAL_48/[DayFloat] * [CurrentDayFloat] end)
from @tbl tt
left join 
(select t.ti_id, t.cd_un, t.[CalendarDayDate],
d.VAL_01,d.VAL_02,d.VAL_03,d.VAL_04,d.VAL_05,d.VAL_06,d.VAL_07,d.VAL_08,d.VAL_09,d.VAL_10,  
d.VAL_11,d.VAL_12,d.VAL_13,d.VAL_14,d.VAL_15,d.VAL_16,d.VAL_17,d.VAL_18,d.VAL_19,d.VAL_20,  
d.VAL_21,d.VAL_22,d.VAL_23,d.VAL_24,d.VAL_25,d.VAL_26,d.VAL_27,d.VAL_28,d.VAL_29,d.VAL_30, 
d.VAL_31,d.VAL_32,d.VAL_33,d.VAL_34,d.VAL_35,d.VAL_36,d.VAL_37,d.VAL_38,d.VAL_39,d.VAL_40,  
d.VAL_41,d.VAL_42,d.VAL_43,d.VAL_44,d.VAL_45,d.VAL_46,d.VAL_47,d.VAL_48
from 
(select * from @tbl where not CD_UN = '') t
	left join 
	(select * from dbo.InfoCalc_Character_Days_Types where TI_ID = @TI_ID and ChannelType = @ChannelType) d
	on d.CD_UN = t.CD_UN) dd
 on tt.CD_UN = dd.CD_UN and tt.[CalendarDayDate] = dd.[CalendarDayDate]
      RETURN
   END
go
grant select on usf2_Utils_MonthValues_to_table to [UserCalcService]
go