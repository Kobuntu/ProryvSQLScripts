set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_IsOVOn')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_IsOVOn
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2008
--
-- Описание:
--
--		Определяем замещала ли точка за указанный промежуток времени другую точку
--		(для интегрального акта)
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Utils_IsOVOn]
(	
	@TI_ID int, --Идентификатор ТИ
	@IsCA bit,  --Эта точка КА
	@DTStart datetime, --Начало периода
	@DTEnd datetime  --Окончание периода
)
RETURNS bit
AS
begin
	--Если эта точка ФСК
	if (@IsCA=0) begin
		if exists (select TI_ID, OV_ID from Hard_OV_List where TI_ID=@TI_ID) return 1
	--Если эта точка КА
	end else begin
		if ((select top 1 (IsOV) from dbo.Info_Contr_TI where ContrTI_ID = @TI_ID)=1 or exists (select ContrTI_ID from dbo.ArchComm_Contr_OV_Switches where ContrTI_ID = @TI_ID and (StartDateTime <=  @DTEnd and FinishDateTime > @DTStart ) and ContrTI_ID<>OV_ContrTI_ID)) return 1
	end
	return 0
end


go
grant EXECUTE on usf2_Utils_IsOVOn to [UserCalcService]
go