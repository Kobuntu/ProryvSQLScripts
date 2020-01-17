--ПРоцедура уходит
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetJuridicalPersonContractForTI')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetJuridicalPersonContractForTI
go
