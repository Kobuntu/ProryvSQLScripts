if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchTech_Get_Table_By_ProfilePeriod')
          and type in ('P','PC'))
   drop procedure usp2_ArchTech_Get_Table_By_ProfilePeriod
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2010
--
-- Описание:
--
--		Возвращаем архивные данные в зависимости от типа таблицы
--
-- ======================================================================================
create proc [dbo].[usp2_ArchTech_Get_Table_By_ProfilePeriod] (
		@TI_ID   int, --Идентификатор ТИ
		@ChannelType tinyint, -- Номер канала
		@EventDate smalldatetime,
		@TechProfilePeriod int,  -- Тип таблицы с которой берем данные
		@TechProfilePeriodPrev int output
)
as

      declare @tbl table (
		[EventDate] datetime NOT NULL primary key clustered WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF),
		[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
		[ChannelType] [dbo].[TI_CHANNEL_TYPE] NOT NULL,
		[TechProfilePeriod] int NOT NULL,
		[ValidStatus] [bigint] NULL,
		[VAL_01] [float] NULL,
		[VAL_02] [float] NULL,
		[VAL_03] [float] NULL,
		[VAL_04] [float] NULL,
		[VAL_05] [float] NULL,
		[VAL_06] [float] NULL,
		[VAL_07] [float] NULL,
		[VAL_08] [float] NULL,
		[VAL_09] [float] NULL,
		[VAL_10] [float] NULL,
		[VAL_11] [float] NULL,
		[VAL_12] [float] NULL,
		[VAL_13] [float] NULL,
		[VAL_14] [float] NULL,
		[VAL_15] [float] NULL,
		[VAL_16] [float] NULL,
		[VAL_17] [float] NULL,
		[VAL_18] [float] NULL,
		[VAL_19] [float] NULL,
		[VAL_20] [float] NULL,
		[VAL_21] [float] NULL,
		[VAL_22] [float] NULL,
		[VAL_23] [float] NULL,
		[VAL_24] [float] NULL,
		[VAL_25] [float] NULL,
		[VAL_26] [float] NULL,
		[VAL_27] [float] NULL,
		[VAL_28] [float] NULL,
		[VAL_29] [float] NULL,
		[VAL_30] [float] NULL,
		[VAL_31] [float] NULL,
		[VAL_32] [float] NULL,
		[VAL_33] [float] NULL,
		[VAL_34] [float] NULL,
		[VAL_35] [float] NULL,
		[VAL_36] [float] NULL,
		[VAL_37] [float] NULL,
		[VAL_38] [float] NULL,
		[VAL_39] [float] NULL,
		[VAL_40] [float] NULL,
		[VAL_41] [float] NULL,
		[VAL_42] [float] NULL,
		[VAL_43] [float] NULL,
		[VAL_44] [float] NULL,
		[VAL_45] [float] NULL,
		[VAL_46] [float] NULL,
		[VAL_47] [float] NULL,
		[VAL_48] [float] NULL,
		[VAL_49] [float] NULL,
		[VAL_50] [float] NULL,
		[VAL_51] [float] NULL,
		[VAL_52] [float] NULL,
		[VAL_53] [float] NULL,
		[VAL_54] [float] NULL,
		[VAL_55] [float] NULL,
		[VAL_56] [float] NULL,
		[VAL_57] [float] NULL,
		[VAL_58] [float] NULL,
		[VAL_59] [float] NULL,
		[VAL_60] [float] NULL,
		[IsTechProfilePeriodChanged] tinyint NOT NULL
		
)
 
--Округляем с точностью до часа
set @EventDate = DateAdd(hour, DatePart(hour, @EventDate), floor(cast(@EventDate as float)))

if (@TechProfilePeriod is null) BEGIN
	set @TechProfilePeriod = 1;
end

set @TechProfilePeriodPrev = @TechProfilePeriod;
declare @isFirst bit;
set @isFirst = 1;

while @isFirst=1 OR @TechProfilePeriodPrev <> @TechProfilePeriod begin

	set @isFirst = 0;
	IF (@TechProfilePeriod = 1) BEGIN
		insert into @tbl ([EventDate],[TI_ID],[ChannelType],[TechProfilePeriod], [ValidStatus]
									,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10]
									,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
									,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30]
									,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
									,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48], [VAL_49] ,[VAL_50]
									,[VAL_51] ,[VAL_52] ,[VAL_53] ,[VAL_54] ,[VAL_55] ,[VAL_56] ,[VAL_57] ,[VAL_58], [VAL_59] ,[VAL_60]
									,IsTechProfilePeriodChanged)
		select t1.EventDate, @TI_ID as ti_id, @ChannelType as ChannelType, @TechProfilePeriod as TechProfilePeriod, t1.ValidStatus,
									t1.val_01 ,t1.val_02 ,t1.val_03 ,t1.val_04 ,t1.val_05 ,t1.val_06 ,t1.val_07 ,t1.val_08 ,t1.val_09 ,t1.val_10 ,
									t1.val_11 ,t1.val_12 ,t1.val_13 ,t1.val_14 ,t1.val_15 ,t1.val_16 ,t1.val_17 ,t1.val_18 ,t1.val_19 ,t1.val_20 ,
									t1.val_21 ,t1.val_22 ,t1.val_23 ,t1.val_24 ,t1.val_25 ,t1.val_26 ,t1.val_27 ,t1.val_28 ,t1.val_29 ,t1.val_30 ,
									t1.val_31 ,t1.val_32 ,t1.val_33 ,t1.val_34 ,t1.val_35 ,t1.val_36 ,t1.val_37 ,t1.val_38 ,t1.val_39 ,t1.val_40 ,
									t1.val_41 ,t1.val_42 ,t1.val_43 ,t1.val_44 ,t1.val_45 ,t1.val_46 ,t1.val_47 ,t1.val_48 ,t1.val_49 ,t1.val_50 ,
									t1.val_51 ,t1.val_52 ,t1.val_53 ,t1.val_54 ,t1.val_55 ,t1.val_56 ,t1.val_57 ,t1.val_58 ,t1.val_59 ,t1.val_60
									,case when @TechProfilePeriod <> @TechProfilePeriodPrev then 1 else 0 end
							from dbo.ArchTech_1Min_Values t1 where TI_ID = @TI_ID and ChannelType=@ChannelType and EventDate = @EventDate
						
	END ELSE IF (@TechProfilePeriod = 2) BEGIN
		insert into @tbl (EventDate,[TI_ID],[ChannelType],[TechProfilePeriod], [ValidStatus]
									,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10]
									,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
									,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30]
									,IsTechProfilePeriodChanged)
		select t1.EventDate, @TI_ID as ti_id, @ChannelType as ChannelType, @TechProfilePeriod as TechProfilePeriod, t1.ValidStatus,
									t1.val_01 ,t1.val_02 ,t1.val_03 ,t1.val_04 ,t1.val_05 ,t1.val_06 ,t1.val_07 ,t1.val_08 ,t1.val_09 ,t1.val_10 ,
									t1.val_11 ,t1.val_12 ,t1.val_13 ,t1.val_14 ,t1.val_15 ,t1.val_16 ,t1.val_17 ,t1.val_18 ,t1.val_19 ,t1.val_20 ,
									t1.val_21 ,t1.val_22 ,t1.val_23 ,t1.val_24 ,t1.val_25 ,t1.val_26 ,t1.val_27 ,t1.val_28 ,t1.val_29 ,t1.val_30 
									,case when @TechProfilePeriod <> @TechProfilePeriodPrev then 1 else 0 end
							from dbo.ArchTech_2Min_Values t1 where TI_ID = @TI_ID and ChannelType=@ChannelType and EventDate = @EventDate
	END ELSE IF (@TechProfilePeriod = 3) BEGIN
		insert into @tbl (EventDate,[TI_ID],[ChannelType],[TechProfilePeriod], [ValidStatus]
									,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10]
									,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
									,IsTechProfilePeriodChanged)
		select t1.EventDate, @TI_ID as ti_id, @ChannelType as ChannelType, @TechProfilePeriod as TechProfilePeriod, t1.ValidStatus,
									t1.val_01 ,t1.val_02 ,t1.val_03 ,t1.val_04 ,t1.val_05 ,t1.val_06 ,t1.val_07 ,t1.val_08 ,t1.val_09 ,t1.val_10 ,
									t1.val_11 ,t1.val_12 ,t1.val_13 ,t1.val_14 ,t1.val_15 ,t1.val_16 ,t1.val_17 ,t1.val_18 ,t1.val_19 ,t1.val_20
									,case when @TechProfilePeriod <> @TechProfilePeriodPrev then 1 else 0 end 
							from dbo.ArchTech_3Min_Values t1 where TI_ID = @TI_ID and ChannelType=@ChannelType and EventDate = @EventDate
	END ELSE IF (@TechProfilePeriod = 5) BEGIN
		insert into @tbl (EventDate,[TI_ID],[ChannelType],[TechProfilePeriod], [ValidStatus]
									,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10]
									,[VAL_11] ,[VAL_12] 
									,IsTechProfilePeriodChanged)
		select t1.EventDate, @TI_ID as ti_id, @ChannelType as ChannelType, @TechProfilePeriod as TechProfilePeriod, t1.ValidStatus,
									t1.val_01 ,t1.val_02 ,t1.val_03 ,t1.val_04 ,t1.val_05 ,t1.val_06 ,t1.val_07 ,t1.val_08 ,t1.val_09 ,t1.val_10,
									t1.val_11 ,t1.val_12
									,case when @TechProfilePeriod <> @TechProfilePeriodPrev then 1 else 0 end 
							from dbo.ArchTech_5Min_Values t1 where TI_ID = @TI_ID and ChannelType=@ChannelType and EventDate = @EventDate
	END ELSE IF (@TechProfilePeriod = 10) BEGIN
		insert into @tbl (EventDate,[TI_ID],[ChannelType],[TechProfilePeriod], [ValidStatus]
									,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06]
									,IsTechProfilePeriodChanged)
		select t1.EventDate, @TI_ID as ti_id, @ChannelType as ChannelType, @TechProfilePeriod as TechProfilePeriod, t1.ValidStatus,
									t1.val_01 ,t1.val_02 ,t1.val_03 ,t1.val_04 ,t1.val_05 ,t1.val_06
									,case when @TechProfilePeriod <> @TechProfilePeriodPrev then 1 else 0 end 
							from dbo.ArchTech_10Min_Values t1 where TI_ID = @TI_ID and ChannelType=@ChannelType and EventDate = @EventDate
	END ELSE IF (@TechProfilePeriod = 15) BEGIN
		insert into @tbl (EventDate,[TI_ID],[ChannelType],[TechProfilePeriod], [ValidStatus]
									,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] 
									,IsTechProfilePeriodChanged)
		select t1.EventDate, @TI_ID as ti_id, @ChannelType as ChannelType, @TechProfilePeriod as TechProfilePeriod, t1.ValidStatus,
									t1.val_01 ,t1.val_02 ,t1.val_03 ,t1.val_04 
									,case when @TechProfilePeriod <> @TechProfilePeriodPrev then 1 else 0 end
							from dbo.ArchTech_15Min_Values t1 where TI_ID = @TI_ID and ChannelType=@ChannelType and EventDate = @EventDate
	END;

	if (not exists(select top 1 1 from @tbl)) begin
			--Нет минуток, пробуем читать 3х минутки и т.д.
			if (@TechProfilePeriod = 1) set @TechProfilePeriod = 2;
			else if (@TechProfilePeriod = 2) set @TechProfilePeriod = 3;
			else if (@TechProfilePeriod = 3) set @TechProfilePeriod = 5;
			else if (@TechProfilePeriod = 5) set @TechProfilePeriod = 10;
			else if (@TechProfilePeriod = 10) set @TechProfilePeriod = 15;
			else if (@TechProfilePeriod = 15) set @TechProfilePeriod = 1;
	end else break; --Есть данные, выходим
end;
set @TechProfilePeriodPrev = @TechProfilePeriod;
select * from @tbl

go
   grant EXECUTE on usp2_ArchTech_Get_Table_By_ProfilePeriod to [UserCalcService]
go

 