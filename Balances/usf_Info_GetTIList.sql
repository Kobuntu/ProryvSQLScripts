if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetTIList')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetTIList
go
/****** Object:  UserDefinedFunction [dbo].[usf2_Info_GetFormulasNeededForMainFormula]    Script Date: 09/18/2008 11:53:14 ******/
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
--		Возвращаем список ТИ и номера каналов в виде строки 'ТИ + номер канала;'
--		для всех дачиков участвующих в указанном балансе 
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetTIList]
(	
	@balancePSID varchar(22)
)
RETURNS varchar(1000)
AS
begin
declare
@Res varchar(1000),
@TI_ID int,
@ChannelType int,
@Ch int
set @Res = '';
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select TI_ID,ChannelType from dbo.Info_Balance_PS_Description where BalancePS_UN=@balancePSID
	open t;
	FETCH NEXT FROM t into @TI_ID,@ChannelType
	WHILE @@FETCH_STATUS = 0
	BEGIN

	set @Res = @Res + cast(@TI_ID as varchar)+ ',' + cast(@ChannelType as varchar) + ';'
	FETCH NEXT FROM t into @TI_ID,@ChannelType
	end;
	CLOSE t
	DEALLOCATE t
	return @Res

end
go
   grant exec on usf2_Info_GetTIList to [UserCalcService]
go