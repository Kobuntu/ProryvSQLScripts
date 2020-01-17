--Необходим такой индекс
--CREATE NONCLUSTERED INDEX [IX_Dict_TI_Journal_Event_Codes] ON [dbo].[Dict_TI_Journal_Event_Codes]
--(
--	[Event61968Domain_ID] ASC,
--	[Event61968DomainPart_ID] ASC,
--	[Event61968Type_ID] ASC,
--	[Event61968Index_ID] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
--GO

set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Event61968_ToLowLevelCode')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Event61968_ToLowLevelCode
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июнь, 2012
--
-- Описание:
--
--		Конвертируем коды событий 61968 (>1000) в события нижнего уровня
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_Event61968_ToLowLevelCode]
(	
	@EventCode int,
	@Event61968Domain_ID tinyint,
	@Event61968DomainPart_ID tinyint,
	@Event61968Type_ID tinyint,
	@Event61968Index_ID int
)
RETURNS int
AS
begin
if (@EventCode < 1000 OR @Event61968Domain_ID is null) return @EventCode;

return ISNULL((select top 1 EventCode from dbo.Dict_TI_Journal_Event_Codes 
where Event61968Domain_ID = @Event61968Domain_ID 
and Event61968DomainPart_ID=@Event61968DomainPart_ID
and Event61968Type_ID=@Event61968Type_ID
and Event61968Index_ID=@Event61968Index_ID), @EventCode);

end
go
grant EXECUTE on usf2_Event61968_ToLowLevelCode to [UserCalcService]
go
