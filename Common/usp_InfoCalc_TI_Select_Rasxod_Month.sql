if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InfoCalc_TI_Select_Rasxod_Month')
          and type in ('P','PC'))
   drop procedure usp2_InfoCalc_TI_Select_Rasxod_Month
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
--		Берем расход за месяц по группе точек (малые ТИ)
--
-- ======================================================================================

create proc [dbo].[usp2_InfoCalc_TI_Select_Rasxod_Month]

@TI_Array varchar(4000),-- Идентификатор ТИ 
@CalendarDayDate DateTime, --Время когда запись была вставлена
@PlanFact tinyint, -- Номер канала
@SummerOrWinter tinyint, --Зимнее (0) или летнее(1) время 
@OffsetFromMoscow int --Смещение относительно Москвы в минутах


as

declare

@all_ti_id int, 

@VALMonth1 float,

@VALMonth2 float,

@MonthYear DateTime,

@datestart Datetime,

@dateend Datetime,

@DayInMonth int,

@cnlAP int,

@cnlAO int


begin

set @CalendarDayDate = DateAdd(dd,-Day(@CalendarDayDate)+1,@CalendarDayDate);

set @CalendarDayDate = DateAdd(HH,-DatePart(hh,@CalendarDayDate),@CalendarDayDate);

set @CalendarDayDate = DateAdd(n,-DatePart(n,@CalendarDayDate),@CalendarDayDate);

set @CalendarDayDate = DateAdd(ss,-DatePart(ss,@CalendarDayDate),@CalendarDayDate);

with Month_Rasxod(TI_ID, PlanFact, MonthYear, ChannelType,VALMonth) 
as (
select TI_ID, PlanFact, MonthYear, ChannelType,VALMonth  from dbo.InfoCalc_TI_Month_Rasxod_Month
where PlanFact = @PlanFact and Month(MonthYear) = Month(@CalendarDayDate) and Year(MonthYear) = Year(@CalendarDayDate) 
),

Character_Days(TI_ID,ChannelType,PlanFact,CalendarDayDate)
as (
select TI_ID,ChannelType,PlanFact,CalendarDayDate from dbo.InfoCalc_Character_Days_In_Calendar_Description
where PlanFact = @PlanFact and Month(CalendarDayDate) = Month(@CalendarDayDate) and Year(CalendarDayDate) = Year(@CalendarDayDate) 
)

-----------Определяем список ТИ для которых описаны характерные дни, также берем расход за месяц-------------------------


select distinct allti.TInumber,ValAP = m.VALMonth,ValAO = m2.VALMonth,ValRP = m3.VALMonth,ValRO = m4.VALMonth,MonthYear = @CalendarDayDate,CnlAP = usf.ChannelType,CnlAO = usf2.ChannelType,CnlRP = usf3.ChannelType,CnlRO = usf4.ChannelType from 

(select TInumber from usf2_Utils_iter_intlist_to_table(@TI_Array)) [allti]

--join 
--
--(select ti_id, AIATSCode,AOATSCode,RIATSCode,ROATSCode from Info_TI) ti on ti.ti_id = [allti].TInumber

---Для АП

left join Month_Rasxod m

on m.TI_ID = allti.TInumber and m.ChannelType = 1--ti.AIATSCode 

left join Character_Days usf

on usf.TI_ID = allti.TInumber and usf.ChannelType = 1--ti.AIATSCode

---Для АО

left join Month_Rasxod m2

on m2.TI_ID = allti.TInumber and m2.ChannelType = 2 

left join Character_Days usf2

on usf2.TI_ID = allti.TInumber and usf2.ChannelType = 2


---Для РП

left join Month_Rasxod m3

on m3.TI_ID = allti.TInumber and m3.ChannelType = 3 

left join Character_Days usf3

on usf3.TI_ID = allti.TInumber and usf3.ChannelType = 3

---Для РО

left join Month_Rasxod m4

on m4.TI_ID = allti.TInumber and m4.ChannelType = 4 

left join Character_Days usf4

on usf4.TI_ID = allti.TInumber and usf4.ChannelType = 4

end

go
   grant EXECUTE on usp2_InfoCalc_TI_Select_Rasxod_Month to [UserCalcService]
go