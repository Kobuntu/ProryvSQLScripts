--Устарело, ненужно
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ListTabl_To_Arch_30')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ListTabl_To_Arch_30
go
