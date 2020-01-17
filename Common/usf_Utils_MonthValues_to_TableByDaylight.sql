if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_MonthValues_to_TableByDaylight')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_MonthValues_to_TableByDaylight
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
--		Функция преобразует таблицу получасовок малых точек распределенных на месяц 
--		в таблицу со смещением учитывающим переход на летнее время и смещение часового пояса
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_MonthValues_to_TableByDaylight] (
		@TI_ID   int, 
		@ChannelType tinyint, -- Номер канала
		@PlanFact tinyint,		--  План, факт
		@FVAL float, --расход
		@MonthYear DateTime,
		@SummerOrWinter tinyint, --Зимнее (0) или летнее(1) время 
		@OffsetFromMoscow int --Смещение относительно Москвы в минутах
)	
	RETURNS @tbl TABLE 
(
		[ti_id] int,
		[ChannelType] tinyint,
		[EventDate] DateTime, 
		[CalendarFloat] float
		,PRIMARY KEY CLUSTERED([EventDate], [ti_id])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)
AS
BEGIN
		--create clustered index t_clustered on @tbl (ti_id,ChannelType,EventDate);
		--Удаляем из даты часы, минуты, секунды, дни
		set @MonthYear = DateAdd(s,-DatePart(s,@MonthYear),DateAdd(n,-DatePart(n,@MonthYear),DateAdd(hh,-DatEPart(hh,@MonthYear),DateAdd(d,-Day(@MonthYear )+1,@MonthYear))))

		INSERT @tbl 
		select @TI_ID, @ChannelType, DateAdd(n,cast(ValueRow as int)* 30 - @OffsetFromMoscow, CalendarDayDate) as EventDate, CalendarFloat from 
		(
			select * from usf2_Utils_MonthValues_to_table(@TI_ID,@ChannelType,@PlanFact,@FVAL,@MonthYear)
		)с
		unpivot ( [CalendarFloat] for ValueRow in ([00],[01],[02],[03],[04],[05],[06],[07],[08],[09],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47])) unp

		declare 
		@m int
		set @m = DatePart(month, @MonthYear);

		--Приводим дату и значения к зимнему времени
		if (@SummerOrWinter=1 and (@m = 3 or @m = 10)) begin -- если переход на лето или на зиму

			declare 
			@DayLight DateTime,
			@HalfHourBeforFloat float,
			@HalfHourAfterFloat float

			if (@m = 3) begin --Переход на лето

				set @DayLight = dbo.usf2_Utils_GetDaylightSavingsTimeStart(@MonthYear)

				set @HalfHourBeforFloat = (select top 1 CalendarFloat from @tbl where EventDate = @DayLight)
				set @HalfHourAfterFloat = (select top 1 CalendarFloat from @tbl where EventDate = DateAdd(n,30,@DayLight))

				--Раскидываем час который выпадает на переход
				update @tbl 
				set CalendarFloat = CalendarFloat + @HalfHourBeforFloat 
				where EventDate =  DateAdd(n,-30,@DayLight);

				update @tbl 
				set CalendarFloat = CalendarFloat + @HalfHourAfterFloat 
				where EventDate =  DateAdd(n,60,@DayLight);

				delete from @tbl where EventDate =  @DayLight or EventDate =  DateAdd(n,30,@DayLight);

				update @tbl set EventDate =  DateAdd(n,-60,EventDate) where EventDate > @DayLight

			end else begin -- переход на зиму

				
				set @DayLight = dbo.usf2_Utils_GetDaylightSavingsTimeEnd(@MonthYear)

				set @HalfHourBeforFloat = (select top 1 CalendarFloat from @tbl where EventDate = @DayLight) / 2
				set @HalfHourAfterFloat = (select top 1 CalendarFloat from @tbl where EventDate = DateAdd(n,30,@DayLight)) / 2

				--select @DayLight,@HalfHourBeforFloat,@HalfHourAfterFloat
					
				update @tbl 
				set CalendarFloat = @HalfHourBeforFloat 
				where EventDate =  @DayLight;

				update @tbl 
				set CalendarFloat = @HalfHourBeforFloat 
				where EventDate =  DateAdd(n,30,@DayLight);

				update @tbl set EventDate =  DateAdd(n,-60,EventDate) where EventDate < DateAdd(n,60,@DayLight)

				insert into @tbl values (@TI_ID, @ChannelType, @DayLight, @HalfHourBeforFloat)
				insert into @tbl values (@TI_ID, @ChannelType, DateAdd(n,30,@DayLight), @HalfHourAfterFloat)

			end

		end else begin --Если это обычный месяц
			
			set @DayLight = dbo.usf2_Utils_GetDaylightSavingsTimeEnd(@MonthYear)
			
			if (@SummerOrWinter=1 and (@m > 3 and @m < 10))	begin
				update @tbl set EventDate =  DateAdd(n,-60,EventDate) where EventDate < DateAdd(n,60,@DayLight)
			end

		end 

		RETURN
END

go
   grant select on usf2_Utils_MonthValues_to_TableByDaylight to [UserCalcService]
go