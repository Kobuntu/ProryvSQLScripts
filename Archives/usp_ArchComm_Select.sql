---Устарела, не используется
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Select')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Select
go
