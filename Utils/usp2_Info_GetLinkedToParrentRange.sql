if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetLinkedToParrentRange')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetLinkedToParrentRange
go
/****** Object:  StoredProcedure [dbo].[usp2_InfoFormulaSelect]    Script Date: 10/02/2008 22:40:23 ******/
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
--		Октябрь, 2013
--
-- Описание:
--
--		Поиск периода привязки к родителю		
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetLinkedToParrentRange]
(	
	@Id int = 0,
	@typeHierarchy tinyint,
	@closedPeriod_id uniqueidentifier = null,
	@parrentId int = null,
	@stringId nvarchar(255) = null
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	declare @tpStart DateTime, @tpEnd DateTime;

	if (@closedPeriod_id is null) select top 1 @tpStart = StartDateTime, @tpEnd = FinishDateTime 
			from Info_Section_Description2 where Section_ID = @parrentId and TP_ID = @Id
			order by StartDateTime desc
		else select top 1 @tpStart = StartDateTime, @tpEnd = FinishDateTime 
			from Info_Section_Description_Closed where Section_ID = @parrentId and TP_ID = @Id and ClosedPeriod_ID = @closedPeriod_id
			order by StartDateTime desc

	if (@typeHierarchy = 8) begin--сечение
		select ISNULL(@tpStart, DateAdd(day, -Day(GETDATE()) + 1, floor(cast(GETDATE() as float)))) as StartDateTime, @tpEnd as FinishDateTime 
	end else if (@typeHierarchy = 9) begin --Формула нашей стороны
		if (@closedPeriod_id is null) select top 1 StartDateTime, 
			case when @tpEnd is null or (FinishDateTime is not null and @tpEnd is not null and FinishDateTime <= @tpEnd) then FinishDateTime else @tpEnd end as FinishDateTime
			from Info_TP2_OurSide_Formula_List where TP_ID = @Id and Formula_UN = @stringId
			order by StartDateTime desc
		else select top 1 StartDateTime, 
			case when @tpEnd is null or (FinishDateTime is not null and @tpEnd is not null and FinishDateTime <= @tpEnd) then FinishDateTime else @tpEnd end as FinishDateTime
			from Info_TP2_OurSide_Formula_List_Closed where TP_ID = @Id and Formula_UN = @stringId and ClosedPeriod_ID = @closedPeriod_id
			order by StartDateTime desc
	end else if (@typeHierarchy = 10) begin --Формула контрольной стороны
		select top 1 StartDateTime, FinishDateTime 
			from Info_TP2_Contr_Formula_List where TP_ID = @Id and Formula_UN = @stringId
			order by StartDateTime desc
	end;

end
go
   grant EXECUTE on usp2_Info_GetLinkedToParrentRange to [UserCalcService]
go