---Устарела, не используется
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchCalc_30_Month_Values_Select')
          and type in ('P','PC'))
   drop procedure usp2_ArchCalc_30_Month_Values_Select
go
