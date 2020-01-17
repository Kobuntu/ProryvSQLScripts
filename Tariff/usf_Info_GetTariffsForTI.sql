if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetTariffsForTI')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetTariffsForTI
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
--		Май, 2011
--
-- Описание:
--
--		Возвращаем список привязаных тарифов для ТИ в виде строки
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetTariffsForTI]
(	
	@TI_ID int
)
RETURNS varchar(1000)
AS
begin
declare
@Res varchar(1000),
@Tariff_ID int

set @Res = '';

select @Res = @Res + cast(Tariff_ID as varchar)+ ','+ convert(varchar, StartDateTime,120)+','+ ISNULL(convert(varchar, FinishDateTime,120), '') + ';'
 from dbo.DictTariffs_ToTI  where TI_ID = @TI_ID	order by StartDateTime
						
if (LEN(@Res) > 0) return substring(@Res, 0, LEN(@Res) + 1);						
	
return @Res

end
go
   grant exec on usf2_Info_GetTariffsForTI to [UserCalcService]
go