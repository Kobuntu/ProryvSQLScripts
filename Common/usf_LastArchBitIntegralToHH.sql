if exists (select 1
          from sysobjects
          where  id = object_id('usf2_LastArchBitIntegralToHH')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_LastArchBitIntegralToHH
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2011
--
-- Описание:
--
-- Формируем список получасовок из интегральных данных
-- ======================================================================================
create FUNCTION [dbo].[usf2_LastArchBitIntegralToHH] (

	@TI_ID [dbo].[TI_ID_TYPE], --Идентификатор ТИ
	@ChannelType [dbo].[TI_CHANNEL_TYPE],
	@EventDateTime smalldatetime, -- Время данных
	@VAL float, --Значение
	@PrevDateTime DateTime, -- Время предыдущего значения
	@PrevValue float -- Само предыдущее значение
)	
	RETURNS @tbl TABLE 
(
		[EventDate] dateTime,
		[EventDateID] int,
		[VAL] float

)
AS
BEGIN

declare 
@RoundPrevDateTime DateTime,
@NumberHalfHours int,
@AverValue float,
@RoundEventDateTime DateTime

set  @RoundEventDateTime = dbo.usf2_Utils_DateTimeRoundToHalfHour(DateAdd(n, -30,  @EventDateTime), 1);

--Берем предыдущее значение
--select top 1 @PrevDateTime = dbo.usf2_Utils_CorrectDateTimeByDaylight(EventDateTime, 1, 0, 0), @PrevValue = Data from dbo.ArchBit_Integrals_1
--where TI_ID = @TI_ID and EventDateTime < @RoundEventDateTime and ChannelType = @ChannelType
--order by EventDateTime desc
	
if (@PrevDateTime is not null and @VAL > @PrevValue) BEGIN

	set @RoundPrevDateTime = dbo.usf2_Utils_DateTimeRoundToHalfHour(@PrevDateTime, 1);

	set @NumberHalfHours = (DateDiff(n, @RoundPrevDateTime, @RoundEventDateTime)) / 30 + 1;

	set @AverValue = (@VAL - @PrevValue) / @NumberHalfHours;

	insert into @tbl 
	select Floor(cast([dt] as float)), DateDiff(n, Floor(cast([dt] as float)), [dt]), @AverValue from dbo.usf2_Utils_HHByPeriod( @RoundPrevDateTime , @RoundEventDateTime)

end

	
		RETURN
END
go
grant select on usf2_LastArchBitIntegralToHH to [UserCalcService]
go