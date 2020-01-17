if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetFirstPSBalance')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetFirstPSBalance
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		август, 2009
--
-- Описание:
--
-- Возвращаем первый необходимый баланс для ПС 
-- смотрим на ForAutoUse если null то первый попавшийся
-- из тех в котором есть точки
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_GetFirstPSBalance] (
			@PS_ID int
)	
	RETURNS BALANSEPS_UN_TYPE
AS
BEGIN
declare 
@BalancePS_UN BALANSEPS_UN_TYPE;


set @BalancePS_UN = (select top 1 bl.BalancePS_UN
	from
	( 
		select distinct PS_ID, ibl.BalancePS_UN, ISNULL(ibl.ForAutoUse, 0) as ForAutoUse from  dbo.Info_Balance_PS_List_2 ibl
		inner join dbo.Info_Balance_PS_Description_2 ibd on ibl.BalancePS_UN = ibd.BalancePS_UN
		where ibl.PS_ID = @PS_ID
	) bl
	order by ForAutoUse desc);

RETURN @BalancePS_UN;

END;
go
grant exec on usf2_Info_GetFirstPSBalance to [UserCalcService]
go