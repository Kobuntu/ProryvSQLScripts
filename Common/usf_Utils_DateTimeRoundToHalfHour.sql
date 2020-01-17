set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_DateTimeRoundToHalfHour')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_DateTimeRoundToHalfHour
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2009
--
-- Описание:
--
--		Округляем время до ближайшей получасовки
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Utils_DateTimeRoundToHalfHour]
(	
	@EventDateTime DateTime, --Дата, которую округляем
	@IsLess bit  --округлить необходимо в меньшую сторону
)
RETURNS DateTime
WITH SCHEMABINDING
AS
begin

declare @min int,@result DateTime

set @min = DatePart(minute, @EventDateTime);
set @result = DateAdd(hour,DatePart(hour,@EventDateTime),floor(cast(@EventDateTime as float)));

if (@min <> 0 AND @min <> 30) begin
	if (@IsLess = 1) begin

		if (@min > 30) set @min = 30;
        else set @min = 0;

	end else begin

		if (@min < 30) set @min = 30;
        else begin
			set @min = 0; 
			set @result = DateAdd(hour,1,@result)
		end 
	end
end

return DateAdd(minute,@min,@result)
end

go
grant EXECUTE on usf2_Utils_DateTimeRoundToHalfHour to [UserCalcService]
go
grant EXECUTE on usf2_Utils_DateTimeRoundToHalfHour to [UserSlave61968Service]
go