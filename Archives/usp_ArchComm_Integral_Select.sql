if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Integral_Select')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Integral_Select
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
--		Декабрь, 2008
--
-- Описание:
--
--		Выбираем значения барабанов
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_Integral_Select]

	@TI_ID int,
	@DateStart datetime,
	@DateEnd datetime,
	@ChannelType tinyint,
	@IsValidateOnly bit,
	@IsStartAndEndDateOnly bit = 0,
	@IsAutoRead bit = null
	

as
begin

set transaction isolation level read uncommitted
set nocount on

if (@IsAutoRead is null) set @IsAutoRead = 0;

	if (@IsStartAndEndDateOnly=0) begin
		select 	t1.EventDateTime,
				t1.Data as Data,	
				t1.Status,
				t1.IntegralType

		from ArchComm_Integrals t1
		
		where t1.TI_ID = @TI_ID and ChannelType=@ChannelType and (EventDateTime between @DateStart and @DateEnd)
		--Фильтр на +- 2 часа, убран 07.03.2013
		--and DateDiff(n, floor(cast(EventDateTime as float)), EventDateTime) < 150
		and (@IsAutoRead = 0 OR IntegralType = 0)
		order by t1.EventDateTime 
	end else begin
		declare 
		@EventDateTimeStart DateTime,
		@EventDateTimeEnd DateTime

		select @EventDateTimeStart = Min(EventDateTime), @EventDateTimeEnd= Max(EventDateTime) from ArchComm_Integrals where TI_ID = @TI_ID and ChannelType=@ChannelType
				and (EventDateTime between @DateStart and @DateEnd)

		select 	t1.EventDateTime,
				t1.Data as Data,	
				t1.Status,
				t1.IntegralType 

		from ArchComm_Integrals t1
		
		where t1.TI_ID = @TI_ID and ChannelType=@ChannelType
		and (EventDateTime=@EventDateTimeStart  or (EventDateTime between DateAdd(minute, -150, @EventDateTimeEnd) and @EventDateTimeEnd))
		and (@IsAutoRead = 0 OR IntegralType = 0)
		order by t1.EventDateTime 
	end

end
go
   grant EXECUTE on usp2_ArchComm_Integral_Select to [UserCalcService]
go