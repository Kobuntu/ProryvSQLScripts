if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetBalancePSData')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetBalancePSData
go
/****** Object:  StoredProcedure [dbo].[usp2_Info_GetBalancePSData]    Script Date: 09/25/2008 12:46:17 ******/
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
--		Сентябрь, 2008
--
-- Описание:
--
--		Данные необходимые для расчетов балансов ПС 
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetBalancePSData]
(	
	@balanceId varchar(22),
	@dTStart datetime,
	@dTEnd datetime,
	@TypeArchTable tinyint
)
AS
declare
@str varchar(1000)
begin
set @str = dbo.usf2_Info_GetTIList(@balanceId)


CREATE TABLE #Tmp3(
	[TI_ID] int, 
	[ChannelType] tinyint,
	Summ float,
	Valid tinyint,
	IsNullableOrAbsentStatus tinyint,
	[CAReplaced] tinyint,
	[ManualEntered] tinyint
	);

	insert into #Tmp3
	exec usp2_Info_GetArhValuesForDatePeriod  @str, 1, @dTStart, @dTEnd,@TypeArchTable
	select * from #Tmp3
	/*select cast(0 as int) as TI_ID, 
	cast(0 as tinyint) as ChannelType ,
	cast(0 as float) as Summ,
	cast(0 as tinyint) as Valid,
	cast(0 as tinyint) as IsNullableOrAbsentStatus,
	cast(0 as tinyint) as CAReplaced,
	cast(0 as tinyint) as ManualEntered
*/

end
go
   grant exec on usp2_Info_GetBalancePSData to [UserCalcService]
go