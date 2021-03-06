if exists (select 1
          from sysobjects
          where  id = object_id('vw_JournalReplace')
          and type in ('V'))
   drop view vw_JournalReplace
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2018
--
-- Описание:
--
--		Журнал замещений 
--
-- ======================================================================================
create view [dbo].[vw_JournalReplace] 
--WITH SCHEMABINDING
AS

SELECT 
j.TI_ID,ti.PS_ID,j.ChannelType, j.[EventDateTime], j.EventDate, j.[User_ID],
ti.TIName as TiName,
ps.StringName as PsName,
[u].[UserFullName] AS [UserName],
j.EventParam,
[dbo].[usf2_JournalReplace_EventParamToString](j.EventParam) as EventParamString,
j.CommentString,
j.ZamerDateTime,
case j.ChannelType 
	when 1 then 'АП'
	when 2 then 'АО'
	when 3 then 'РП'
	when 4 then 'РО'
end as ChannelName
FROM [dbo].[Expl_User_Journal_Replace_30_Virtual] AS [j] WITH (NOLOCK)
JOIN [dbo].[Info_TI] AS [ti] WITH (NOLOCK) ON [ti].[TI_ID] = [j].[TI_ID]
JOIN [dbo].[Dict_PS] AS [ps] WITH (NOLOCK) ON [ps].[PS_ID] = [ti].[PS_ID]
JOIN [dbo].[Expl_Users] AS [u] WITH (NOLOCK) ON [u].[User_ID] = [j].[User_ID]

GO

   grant select on vw_JournalReplace to [UserCalcService]
go