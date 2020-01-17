---Устарела, не используется (эта была последняя процедура для выборки из XML)
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_XML_Select2')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_XML_Select2
go
