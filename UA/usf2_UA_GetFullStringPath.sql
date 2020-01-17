set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UA_GetFullStringPath')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UA_GetFullStringPath
go

--Устарела, не используется