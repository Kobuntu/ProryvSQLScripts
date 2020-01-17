--Процедура устарела и не используется
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetSection2')
          and type in ('P','PC'))
 drop procedure usp2_Info_GetSection2
go
