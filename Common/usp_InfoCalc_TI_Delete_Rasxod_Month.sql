if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InfoCalc_TI_Delete_Rasxod_Month')
          and type in ('P','PC'))
   drop procedure usp2_InfoCalc_TI_Delete_Rasxod_Month
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2009
--
-- Описание:
--
--		Удаляем  расход за месяц по группе точек (малые ТИ)
--
-- ======================================================================================

create proc [dbo].[usp2_InfoCalc_TI_Delete_Rasxod_Month]
	@TI_Array varchar(4000),-- Список ТИ в виде: Идентификатор ТИ , 0;
	@MonthYear DateTime, -- Время за которое надо удалить значения
	@ChannelType tinyint, -- Номер канала
	@PlanFact tinyint,		--  План, факт
	@IsWs bit, --Учитывать отмену зимнего времени или писать как есть
	@BaseOffsetClientFromServer int, --Базовое смещение клиента относительно сервера в минутах
	@User_ID varchar(22) --Идентификатор пользователя
as

begin

	set @MonthYear = floor(cast(DateAdd(day, -Day(@MonthYear) + 1, @MonthYear) as float))
-----Удалить расход за месяц----------------
	Delete  from InfoCalc_TI_Month_Rasxod_Month 
	from InfoCalc_TI_Month_Rasxod_Month usf
	where usf.ChannelType = @ChannelType and Month(MonthYear)=Month(@MonthYear) and Year(MonthYear)=Year(@MonthYear) and PlanFact = @PlanFact
	and usf.TI_ID IN (select TInumber from usf2_Utils_iter_intlist_to_table(@TI_Array))
-----Удалить 30 минутки----------------
	Update	ArchCalc_30_Month_Values set 
	VAL_01 = (case when fl.[0]   =-2 then null else VAL_01 end),
	VAL_02 = (case when fl.[30]  =-2 then null else VAL_02 end),
	VAL_03 = (case when fl.[60]  =-2 then null else VAL_03 end),
	VAL_04 = (case when fl.[90]  =-2 then null else VAL_04 end),
	VAL_05 = (case when fl.[120] =-2 then null else VAL_05 end),
	VAL_06 = (case when fl.[150] =-2 then null else VAL_06 end),
	VAL_07 = (case when fl.[180] =-2 then null else VAL_07 end),
	VAL_08 = (case when fl.[210] =-2 then null else VAL_08 end),
	VAL_09 = (case when fl.[240] =-2 then null else VAL_09 end),
	VAL_10 = (case when fl.[270]  =-2 then null else VAL_10 end), 
	VAL_11 = (case when fl.[300]  =-2 then null else VAL_11 end),
	VAL_12 = (case when fl.[330]  =-2 then null else VAL_12 end),
	VAL_13 = (case when fl.[360]  =-2 then null else VAL_13 end),
	VAL_14 = (case when fl.[390]  =-2 then null else VAL_14 end),
	VAL_15 = (case when fl.[420]  =-2 then null else VAL_15 end),
	VAL_16 = (case when fl.[450]  =-2 then null else VAL_16 end),
	VAL_17 = (case when fl.[480]  =-2 then null else VAL_17 end),
	VAL_18 = (case when fl.[510]  =-2 then null else VAL_18 end),
	VAL_19 = (case when fl.[540]  =-2 then null else VAL_19 end),
	VAL_20 = (case when fl.[570]  =-2 then null else VAL_20 end), 
	VAL_21 = (case when fl.[600]  =-2 then null else VAL_21 end),
	VAL_22 = (case when fl.[630]  =-2 then null else VAL_22 end),
	VAL_23 = (case when fl.[660]  =-2 then null else VAL_23 end),
	VAL_24 = (case when fl.[690]  =-2 then null else VAL_24 end),
	VAL_25 = (case when fl.[720]  =-2 then null else VAL_25 end),
	VAL_26 = (case when fl.[750]  =-2 then null else VAL_26 end),
	VAL_27 = (case when fl.[780]  =-2 then null else VAL_27 end),
	VAL_28 = (case when fl.[810]  =-2 then null else VAL_28 end),
	VAL_29 = (case when fl.[840]  =-2 then null else VAL_29 end),
	VAL_30 = (case when fl.[870]  =-2 then null else VAL_30 end),
	VAL_31 = (case when fl.[900]  =-2 then null else VAL_31 end),
	VAL_32 = (case when fl.[930]  =-2 then null else VAL_32 end),
	VAL_33 = (case when fl.[960]  =-2 then null else VAL_33 end),
	VAL_34 = (case when fl.[990]  =-2 then null else VAL_34 end),
	VAL_35 = (case when fl.[1020] =-2 then null else VAL_35 end),
	VAL_36 = (case when fl.[1050] =-2 then null else VAL_36 end),
	VAL_37 = (case when fl.[1080] =-2 then null else VAL_37 end),
	VAL_38 = (case when fl.[1110] =-2 then null else VAL_38 end),
	VAL_39 = (case when fl.[1140] =-2 then null else VAL_39 end),
	VAL_40 = (case when fl.[1170] =-2 then null else VAL_40 end),
	VAL_41 = (case when fl.[1200] =-2 then null else VAL_41 end),
	VAL_42 = (case when fl.[1230] =-2 then null else VAL_42 end),
	VAL_43 = (case when fl.[1260] =-2 then null else VAL_43 end),
	VAL_44 = (case when fl.[1290] =-2 then null else VAL_44 end),
	VAL_45 = (case when fl.[1320] =-2 then null else VAL_45 end),
	VAL_46 = (case when fl.[1350] =-2 then null else VAL_46 end),
	VAL_47 = (case when fl.[1380] =-2 then null else VAL_47 end),
	VAL_48 = (case when fl.[1410] =-2 then null else VAL_48 end)
	from 
	(
		select PivotTable.* 
		from 
		(
			select TI_ID, Cast(floor(cast(EventDateTime as float)) as DateTime) as EventDate, DateDiff(n,floor(cast(EventDateTime as float)),EventDateTime) as HalfHours, Val
			from
			(
				select TInumber as TI_ID, DateAdd(n,@BaseOffsetClientFromServer, usf.dt) as EventDateTime, -2 as Val from usf2_Utils_iter_intlist_to_table(@TI_Array)
				cross apply usf2_Utils_HalfHoursByMonth(@MonthYear)  usf
			) v
		) AS SourceTable
		PIVOT
		(
			avg([Val]) FOR [HalfHours] IN ([0],[30],[60],[90],[120],[150],[180],[210],[240],[270],[300],[330],[360],[390],[420],[450],[480],[510],[540],[570],[600],[630],[660],[690],[720],[750],[780],[810],[840],[870],[900],[930],[960],[990],[1020],[1050],[1080],[1110],[1140],[1170],[1200],[1230],[1260],[1290],[1320],[1350],[1380],[1410])
		) AS PivotTable
	) fl
	where ArchCalc_30_Month_Values.TI_ID=fl.TI_ID and ArchCalc_30_Month_Values.ChannelType=@ChannelType and ArchCalc_30_Month_Values.PlanFact = @PlanFact and ArchCalc_30_Month_Values.EventDate = fl.EventDate

	--Удаляем основные данные в промежуточной таблице
	if (@IsWs = 1 and @MonthYear = '20141001') begin
		update ArchBit_30_Values_WS set VAL_03 = null, VAL_04 = null
		where TI_ID in (select distinct TInumber from usf2_Utils_iter_intlist_to_table(@TI_Array)) and EventDate = '20141026' and ChannelType = @ChannelType and DataSource_ID = 0
	end

	------Пишем в журнал событий-------------------------------------
		declare @EventParam tinyint, @Float1 float, @ti_ID int, @DispatchDateTime datetime; 

		set @DispatchDateTime = GetDate();
		set @EventParam = 138; -- Удаление данных по малой ТИ

		IF Cursor_Status('variable', 'cc_') > 0 begin 
			CLOSE cc_
			DEALLOCATE cc_
		end

		declare cc_ cursor for select TInumber from usf2_Utils_iter_intlist_to_table(@TI_Array) usf
		
		open cc_;
		FETCH NEXT FROM cc_ into @ti_ID
		WHILE @@FETCH_STATUS = 0
		BEGIN
		--@CUS_ID as cus_id,@DispatchDateTime as E`ventDateTime,@EventParam as EventParam,@CommentString,
			if not exists (select TI_ID,ChannelType,EventDate,[User_ID],EventDateTime from dbo.Expl_User_Journal_Replace_30_Virtual vv 
			where vv.ti_id = @ti_id and vv.ChannelType = @ChannelType and vv.EventDate = convert(smalldatetime, @MonthYear) and vv.[User_ID] = @User_ID and vv.EventDateTime = @DispatchDateTime) 
			begin 
				insert dbo.Expl_User_Journal_Replace_30_Virtual (TI_ID, ChannelType, EventDate, [User_ID], EventDateTime, EventParam, CUS_ID, ZamerDateTime, CommentString)
				select @ti_ID, @ChannelType,convert(smalldatetime, @MonthYear) as EventDate, @User_ID, @DispatchDateTime, @EventParam, 0, 
				DateAdd(minute, -30, DateAdd(month, 1, @MonthYear)), ''
			end else begin 
				update dbo.Expl_User_Journal_Replace_30_Virtual
				set  ti_ID = @ti_ID, ChannelType = @ChannelType,EventDate = convert(smalldatetime, @MonthYear), 
				[User_ID] = @User_ID, EventDateTime =@DispatchDateTime, EventParam = @EventParam, CUS_ID = 0, 
				ZamerDateTime = DateAdd(minute, -30, DateAdd(month, 1, @MonthYear)), CommentString = ''
				where Expl_User_Journal_Replace_30_Virtual.ti_id = @ti_id and Expl_User_Journal_Replace_30_Virtual.ChannelType = @ChannelType and Expl_User_Journal_Replace_30_Virtual.EventDate = convert(smalldatetime, @MonthYear)
					and Expl_User_Journal_Replace_30_Virtual.[User_ID] = @User_ID and Expl_User_Journal_Replace_30_Virtual.EventDateTime = @DispatchDateTime 
			end
			FETCH NEXT FROM cc_ into @ti_ID
		END
		CLOSE cc_
		DEALLOCATE cc_
end

go
   grant EXECUTE on usp2_InfoCalc_TI_Delete_Rasxod_Month to [UserCalcService]
go