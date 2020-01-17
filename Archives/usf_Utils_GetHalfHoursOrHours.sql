if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_GetHalfHoursOrHours')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_GetHalfHoursOrHours
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
--		Возвращаем часовку или получасовку
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_GetHalfHoursOrHours]
(
@Val1 float,
@Val0 float,
@DiscreteType tinyint,
@StartDate DateTime,
@Valid bigint,
@NumberHalfHour int
)
RETURNS @tbl TABLE (
		[dt] [smalldatetime] NOT NULL,
		[ValidStatus] [bit] NULL,
		[Value] [float] NULL
)
AS
BEGIN
	declare 
	@Value float

	if (@DiscreteType = 0) begin --Получасовка
		set @Value = @Val1
		set @StartDate = DateAdd(minute,(@NumberHalfHour - 1) * 30,@StartDate)
		set @Valid = dbo.sfclr_Utils_BitOperations2(@Valid,(@NumberHalfHour - 1))
	end else begin	--Часовка
		set @Value = @Val1 + ISNULL(@Val0,0)
		set @StartDate = DateAdd(minute,(@NumberHalfHour - 1) * 30 - 30 ,@StartDate)
		set @Valid = dbo.sfclr_Utils_BitOperations2(@Valid,(@NumberHalfHour - 2)) | dbo.sfclr_Utils_BitOperations2(@Valid,(@NumberHalfHour - 1))
	end

	insert @tbl ([dt],[ValidStatus], [Value]) 
	values (@StartDate,@Valid, @Value)


	return
END

go
grant select on usf2_Utils_GetHalfHoursOrHours to [UserCalcService]
go