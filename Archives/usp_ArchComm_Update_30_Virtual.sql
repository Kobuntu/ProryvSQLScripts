--Устарело, не нужно
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Update_30_Virtual')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Update_30_Virtual
go

