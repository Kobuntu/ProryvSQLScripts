--Ф-ия устарела и не используется
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetFormulasParams')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetFormulasParams
go

