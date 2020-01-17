if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetBalancePSParams')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetBalancePSParams
go
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
--		Август, 2009
--
-- Описание:
--
--		Данные необходимые для расчетов балансов ПС 2 и для достоверности точек
--
-- ======================================================================================
create proc [dbo].[usp2_Info_GetBalancePSParams]
(	
	@balanceIds varchar(max),	
	@datestart datetime,
	@dateend datetime
)

AS
declare
@Num int,
@TI_ID int,
@ChannelType int

begin

	declare 
	@PS_ID int,
	@BalancePS_UN varchar(22)

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	--Перебираем все ПС
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select STRnumber from usf2_Utils_iter_strlist_to_table(@balanceIds)
	open t;
	FETCH NEXT FROM t into @BalancePS_UN
	WHILE @@FETCH_STATUS = 0
	BEGIN
		set @PS_ID = (select PS_ID from [dbo].[Info_Balance_PS_List_2] where BalancePS_UN = @BalancePS_UN)
		select @BalancePS_UN as BalancePS_UN, (select top 1 PSType from Dict_PS where PS_ID = @PS_ID) as PSType, @PS_ID as PS_ID;

		--Выбираем точки в балансе, и параметры этих точек
		exec dbo.usp2_Info_GetMetersParams2 @BalancePS_UN, @datestart,@dateend
		--Выбираем точки которые относятся к ПС но не входят в баланс
		select TI_ID from info_ti
		where PS_ID = @PS_ID and TIType<>6 and not TI_ID in  --Отсеиваем точки ЕПП (6 тип)
			(	select TI_ID 
				from dbo.Info_Balance_PS_Description_2
				where BalancePS_UN=@BalancePS_UN and not TI_ID is null
			)
		
	FETCH NEXT FROM t into @BalancePS_UN
	end;
	CLOSE t
	DEALLOCATE t

end
go
   grant exec on usp2_Info_GetBalancePSParams to [UserCalcService]
go