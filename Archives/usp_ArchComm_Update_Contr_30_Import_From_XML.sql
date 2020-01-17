if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Update_Contr_30_Import_From_XML')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Update_Contr_30_Import_From_XML
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
--		Сентябрь, 2009
--
-- Описание:
--
--		Замещение данных КА (ручной ввод)
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_Update_Contr_30_Import_From_XML]
	@TI_Array varchar(4000),-- Идентификатор ТИ (строго 10 цифр) , время (строго 16 цифр), значение (любое количество цифр), номер канала между ними , в конце обязательна ;
	@DispatchDateTime DateTime, --Время когда запись была вставлена
	@EventParam tinyint, --Признак замещения (0- неопределено,1- ручной ввод,2- КА,3-Данные измерений)
	@CUS_ID tinyint, --Номер ЦУС сформировавший эту запись
	@UserName varchar(64), --Имя пользователя
	@CommentString varchar(1000) --Строка комментариев
	

as
begin
--Создаем промежуточную таблицу с данными которыми будем замещать
	create table #TmpResult ([TI_ID] int,
		[ChannelType] int, --Номер канала
		[oldday] DateTime null, --Поле по которому определяем есть уже такие данные в архивной таблице 30 минуток
		[CalendarDayDate] DateTime, --Дата календаря
		--Получасовки
		[k1] float,[k2] float,[k3] float,[k4] float,[k5] float,[k6] float,[k7] float,[k8] float,[k9] float,[k10] float,
		[k11] float,[k12] float,[k13] float,[k14] float,[k15] float,[k16] float,[k17] float,[k18] float,[k19] float,[k20] float,
		[k21] float,[k22] float,[k23] float,[k24] float,[k25] float,[k26] float,[k27] float,[k28] float,[k29] float,[k30] float,
		[k31] float,[k32] float,[k33] float,[k34] float,[k35] float,[k36] float,[k37] float,[k38] float,[k39] float,[k40] float,
		[k41] float,[k42] float,[k43] float,[k44] float,[k45] float,[k46] float,[k47] float,[k48] float,
		NotNullStatus bigint, -- Побитово выставляем биты для тех получасовок которые присутствуют
		ValidStatus bigint
		);
	insert into #TmpResult
	select a.ti_id,a.ChannelType,a.oldday,a.[CalendarDayDate], 
					a.k1,a.k2,a.k3,a.k4,a.k5,a.k6, a.k7, a.k8, a.k9, a.k10,
					a.k11, a.k12, a.k13, a.k14, a.k15, a.k16, a.k17, a.k18, a.k19, a.k20,
					a.k21, a.k22, a.k23, a.k24, a.k25, a.k26, a.k27, a.k28, a.k29, a.k30,
					a.k31, a.k32, a.k33, a.k34, a.k35, a.k36, a.k37, a.k38, a.k39, a.k40,
					a.k41, a.k42, a.k43, a.k44, a.k45, a.k46, a.k47, a.k48,
					a.NotNullStatus, v.ValidStatus
	from 
				usf2_ListTabl_To_ArchComm_Contr_30_Import_From_XML_Values(@TI_Array) a
				left join 
				usf2_ListTabl_To_ArchComm_Contr_30_Import_From_XML_Validate(@TI_Array) v
				on a.[TI_ID] = v.[TI_ID] and a.[ChannelType] = v.[ChannelType] and a.[CalendarDayDate] = v.[CalendarDayDate]

	BEGIN TRY  BEGIN TRANSACTION
		
		----Обновляем таблицу архивных значений-------------------------------------
		Update	ArchComm_Contr_30_Import_From_XML set 
		VAL_01 = (case when not fl.k1 is null then fl.k1 else VAL_01 end),
		VAL_02 = (case when not fl.k2 is null then fl.k2 else VAL_02 end),
		VAL_03 = (case when not fl.k3 is null then fl.k3 else VAL_03 end),
		VAL_04 = (case when not fl.k4 is null then fl.k4 else VAL_04 end),
		VAL_05 = (case when not fl.k5 is null then fl.k5 else VAL_05 end),
		VAL_06 = (case when not fl.k6 is null then fl.k6 else VAL_06 end),
		VAL_07 = (case when not fl.k7 is null then fl.k7 else VAL_07 end),
		VAL_08 = (case when not fl.k8 is null then fl.k8 else VAL_08 end),
		VAL_09 = (case when not fl.k9 is null then fl.k9 else VAL_09 end),
		VAL_10 = (case when not fl.k10 is null then fl.k10 else VAL_10 end), 
		VAL_11 = (case when not fl.k11 is null then fl.k11 else VAL_11 end),
		VAL_12 = (case when not fl.k12 is null then fl.k12 else VAL_12 end),
		VAL_13 = (case when not fl.k13 is null then fl.k13 else VAL_13 end),
		VAL_14 = (case when not fl.k14 is null then fl.k14 else VAL_14 end),
		VAL_15 = (case when not fl.k15 is null then fl.k15 else VAL_15 end),
		VAL_16 = (case when not fl.k16 is null then fl.k16 else VAL_16 end),
		VAL_17 = (case when not fl.k17 is null then fl.k17 else VAL_17 end),
		VAL_18 = (case when not fl.k18 is null then fl.k18 else VAL_18 end),
		VAL_19 = (case when not fl.k19 is null then fl.k19 else VAL_19 end),
		VAL_20 = (case when not fl.k20 is null then fl.k20 else VAL_20 end), 
		VAL_21 = (case when not fl.k21 is null then fl.k21 else VAL_21 end),
		VAL_22 = (case when not fl.k22 is null then fl.k22 else VAL_22 end),
		VAL_23 = (case when not fl.k23 is null then fl.k23 else VAL_23 end),
		VAL_24 = (case when not fl.k24 is null then fl.k24 else VAL_24 end),
		VAL_25 = (case when not fl.k25 is null then fl.k25 else VAL_25 end),
		VAL_26 = (case when not fl.k26 is null then fl.k26 else VAL_26 end),
		VAL_27 = (case when not fl.k27 is null then fl.k27 else VAL_27 end),
		VAL_28 = (case when not fl.k28 is null then fl.k28 else VAL_28 end),
		VAL_29 = (case when not fl.k29 is null then fl.k29 else VAL_29 end),
		VAL_30 = (case when not fl.k30 is null then fl.k30 else VAL_30 end),
		VAL_31 = (case when not fl.k31 is null then fl.k31 else VAL_31 end),
		VAL_32 = (case when not fl.k32 is null then fl.k32 else VAL_32 end),
		VAL_33 = (case when not fl.k33 is null then fl.k33 else VAL_33 end),
		VAL_34 = (case when not fl.k34 is null then fl.k34 else VAL_34 end),
		VAL_35 = (case when not fl.k35 is null then fl.k35 else VAL_35 end),
		VAL_36 = (case when not fl.k36 is null then fl.k36 else VAL_36 end),
		VAL_37 = (case when not fl.k37 is null then fl.k37 else VAL_37 end),
		VAL_38 = (case when not fl.k38 is null then fl.k38 else VAL_38 end),
		VAL_39 = (case when not fl.k39 is null then fl.k39 else VAL_39 end),
		VAL_40 = (case when not fl.k40 is null then fl.k40 else VAL_40 end),
		VAL_41 = (case when not fl.k41 is null then fl.k41 else VAL_41 end),
		VAL_42 = (case when not fl.k42 is null then fl.k42 else VAL_42 end),
		VAL_43 = (case when not fl.k43 is null then fl.k43 else VAL_43 end),
		VAL_44 = (case when not fl.k44 is null then fl.k44 else VAL_44 end),
		VAL_45 = (case when not fl.k45 is null then fl.k45 else VAL_45 end),
		VAL_46 = (case when not fl.k46 is null then fl.k46 else VAL_46 end),
		VAL_47 = (case when not fl.k47 is null then fl.k47 else VAL_47 end),
		VAL_48 = (case when not fl.k48 is null then fl.k48 else VAL_48 end),
		DispatchDateTime = @DispatchDateTime,
		CUS_ID = @CUS_ID,
		--ContrReplaceStatus = case when @EventParam=2 then ContrReplaceStatus | NotNullStatus else 0 end,
		ManualEnterStatus  = ISNULL(ArchComm_Contr_30_Import_From_XML.ManualEnterStatus,0) | NotNullStatus,
		ValidStatus = (ArchComm_Contr_30_Import_From_XML.ValidStatus & (~NotNullStatus)) | fl.ValidStatus
		from 
		(
			select * from #TmpResult where not [oldday] is null
		) fl
		where ArchComm_Contr_30_Import_From_XML.ContrTI_ID=fl.TI_ID and ArchComm_Contr_30_Import_From_XML.ChannelType=fl.ChannelType and ArchComm_Contr_30_Import_From_XML.EventDate = fl.CalendarDayDate

	---Добавляем поля которых нет 
		insert	ArchComm_Contr_30_Import_From_XML
		(
			ContrTI_ID,EventDate,ChannelType,[VAL_01],[VAL_02],[VAL_03]
			  ,[VAL_04]
			  ,[VAL_05]
			  ,[VAL_06]
			  ,[VAL_07]
			  ,[VAL_08]
			  ,[VAL_09]
			  ,[VAL_10]
			  ,[VAL_11]
			  ,[VAL_12]
			  ,[VAL_13]
			  ,[VAL_14]
			  ,[VAL_15]
			  ,[VAL_16]
			  ,[VAL_17]
			  ,[VAL_18]
			  ,[VAL_19]
			  ,[VAL_20]
			  ,[VAL_21]
			  ,[VAL_22]
			  ,[VAL_23]
			  ,[VAL_24]
			  ,[VAL_25]
			  ,[VAL_26]
			  ,[VAL_27]
			  ,[VAL_28]
			  ,[VAL_29]
			  ,[VAL_30]
			  ,[VAL_31]
			  ,[VAL_32]
			  ,[VAL_33]
			  ,[VAL_34]
			  ,[VAL_35]
			  ,[VAL_36]
			  ,[VAL_37]
			  ,[VAL_38]
			  ,[VAL_39]
			  ,[VAL_40]
			  ,[VAL_41]
			  ,[VAL_42]
			  ,[VAL_43]
			  ,[VAL_44]
			  ,[VAL_45]
			  ,[VAL_46]
			  ,[VAL_47]
			  ,[VAL_48]
			  ,[ValidStatus]
			  ,[DispatchDateTime]
			  ,[Status]
			  --,[ContrReplaceStatus]
			  ,[ManualEnterStatus]
			  ,[CUS_ID])
		select fl.ti_id, EventDate = fl.CalendarDayDate,fl.ChannelType, 
		fl.k1,fl.k2,fl.k3,fl.k4,fl.k5,fl.k6, fl.k7, fl.k8, fl.k9, fl.k10,
		 fl.k11, fl.k12, fl.k13, fl.k14, fl.k15, fl.k16, fl.k17, fl.k18, fl.k19, fl.k20,
		 fl.k21, fl.k22, fl.k23, fl.k24, fl.k25, fl.k26, fl.k27, fl.k28, fl.k29, fl.k30,
		 fl.k31, fl.k32, fl.k33, fl.k34, fl.k35, fl.k36, fl.k37, fl.k38, fl.k39, fl.k40,
		 fl.k41, fl.k42, fl.k43, fl.k44, fl.k45, fl.k46, fl.k47, fl.k48,
		ValidStatus = fl.ValidStatus, DispatchDateTime = @DispatchDateTime,[Status] =0,
		--ContrReplaceStatus = case when @EventParam=2 then NotNullStatus else 0 end,
		[ManualEnterStatus] = NotNullStatus, 
		CUS_ID=@CUS_ID 
		from 
		(
			select * from #TmpResult where [oldday] is null
		) fl
		drop table #TmpResult
--	----Пишем в журнал событий-------------------------------------
--		declare
--		@ti_ID int, @ChannelType int, @User_ID varchar(22), @ZamerDateTime DateTime
--		IF Cursor_Status('variable', 'cc_') > 0 begin 
--			CLOSE cc_
--			DEALLOCATE cc_
--		end
--
--		declare cc_ cursor for select Convert(int, ti_id), Convert(int, floor([Float2] / 10)) as ChannelType, u.[User_ID] ,Convert(datetime, [EventDate]) as ZamerDateTime
--		from usf2_Utils_iter_floatlist_to_table(@TI_Array) t
--		left join 
--		dbo.Expl_Users u on u.[UserName] = @UserName 
--
--		open cc_;
--		FETCH NEXT FROM cc_ into @ti_ID, @ChannelType,@User_ID, @ZamerDateTime
--		WHILE @@FETCH_STATUS = 0
--		BEGIN
--		--@CUS_ID as cus_id,@DispatchDateTime as EventDateTime,@EventParam as EventParam,@CommentString,
--			if not exists (select TI_ID,ChannelType,EventDate,[User_ID],EventDateTime from dbo.Expl_User_Journal_Replace_30_Virtual vv 
--			where vv.ti_id = @ti_id and vv.ChannelType = @ChannelType and vv.EventDate = convert(smalldatetime, @ZamerDateTime) and vv.[User_ID] = @User_ID and vv.EventDateTime = @DispatchDateTime) begin 
--				insert dbo.Expl_User_Journal_Replace_30_Virtual (TI_ID, ChannelType, EventDate, [User_ID], EventDateTime, EventParam, CommentString, CUS_ID, ZamerDateTime)
--				select @ti_ID, @ChannelType,convert(smalldatetime, @ZamerDateTime) as EventDate, @User_ID, @DispatchDateTime, @EventParam, @CommentString, @CUS_ID, convert(smalldatetime, @ZamerDateTime) as  ZamerDateTime
--			end else begin 
--				update dbo.Expl_User_Journal_Replace_30_Virtual
--				set  ti_ID = @ti_ID, ChannelType = @ChannelType,EventDate = convert(smalldatetime, @ZamerDateTime), [User_ID] = @User_ID, EventDateTime =@DispatchDateTime, EventParam = @EventParam, CommentString = @CommentString, CUS_ID = @CUS_ID, ZamerDateTime = @ZamerDateTime 
--				where Expl_User_Journal_Replace_30_Virtual.ti_id = @ti_id and Expl_User_Journal_Replace_30_Virtual.ChannelType = @ChannelType and Expl_User_Journal_Replace_30_Virtual.EventDate = convert(smalldatetime, @ZamerDateTime)
--					and Expl_User_Journal_Replace_30_Virtual.[User_ID] = @User_ID and Expl_User_Journal_Replace_30_Virtual.EventDateTime = @DispatchDateTime 
--			end
--			FETCH NEXT FROM cc_ into @ti_ID, @ChannelType,@User_ID, @ZamerDateTime
--		END
--		CLOSE cc_
--		DEALLOCATE cc_

	COMMIT	
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK 

	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 10 
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH
	
end

go
   grant EXECUTE on usp2_ArchComm_Update_Contr_30_Import_From_XML to [UserCalcService]
go
