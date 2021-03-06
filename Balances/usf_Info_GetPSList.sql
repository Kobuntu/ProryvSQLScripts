if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetPSList')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetPSList
go


/****** Object:  UserDefinedFunction [dbo].[usf2_Info_GetTIList]    Script Date: 09/25/2008 17:23:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2008
--
-- Описание:
--
--		Возвращаем список ПС 
--		для всех ПС участвующих в указанном балансе МСК
--
-- ======================================================================================


create FUNCTION [dbo].[usf2_Info_GetPSList]
(	
	@balanceHierLev3ID varchar(22)
)
RETURNS varchar(2000)
AS
begin
declare
@Res varchar(2000),
@PS_ID varchar(22)
set @Res = '';
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select BalancePS_UN
		 from dbo.Info_Balance_HierLev3_Description
		 where Balance_HierLev3_UN=@balanceHierLev3ID and not BalancePS_UN  is Null
	open t;
	FETCH NEXT FROM t into @PS_ID
	WHILE @@FETCH_STATUS = 0
	BEGIN
	set @Res = @Res + @PS_ID+ ',0;'
	FETCH NEXT FROM t into @PS_ID
	end;
	CLOSE t
	DEALLOCATE t
	return @Res

end
go
   grant exec on usf2_Info_GetPSList to [UserCalcService]
go