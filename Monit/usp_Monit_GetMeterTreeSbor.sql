if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Monit_GetMeterTreeSbor')
          and type in ('P','PC'))
   drop procedure usp2_Monit_GetMeterTreeSbor
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
--		май, 2012
--
-- Описание:
--
--		Строим деревья сбора счетчика в пределах TopLimit штук
--
-- ======================================================================================
create proc [dbo].[usp2_Monit_GetMeterTreeSbor]
	@TopLimit int, -- Предел количества деревьев
	@TI_ID int, -- идентификатор точки
	@datestart DateTime, -- Период
	@dateend DateTime
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@Meter_ID int,
@Concentrator_ID int;

set @Meter_ID = (select top 1 METER_ID from dbo.Info_Meters_TO_TI imtt where imtt.TI_ID = @TI_ID 
						and StartDateTime <= @dateend 
						and (FinishDateTime is null or FinishDateTime >= @datestart)
				order by StartDateTime);

declare
@s_id uniqueidentifier,
@edt DateTime,
@mrt int,
@c_id int,
@counter int,
@IsResultComplete bit,
@TITreeList varchar(800),	
@Key_ID varchar(1024),
@isFirst bit;	

set @IsResultComplete = 0;
set @counter = 0;
set @isFirst = 1;

CREATE TABLE #resultTable(
	[Key_ID] nvarchar(400), --Ключ по которому определяем уникальность данного пути
	[TITreeList] nvarchar(4000),
	[EventDateTime] [datetime],
	[MeterReadTimeout] int null,
	--Информация по последнему концентратору в цепочке
	[Concentrator_ID] int,
	[LinkNumber] int,
	[StringName] nvarchar(255),
	--Информация по УСПД
	[USPD_ID] int default(-1),
	[E422_ID] int,
	[PS_ID] int NULL, -- ПС на котрой сидит концентратор
	[Slave61968System_ID] int NULL, -- 61968 с которой собирается напрямую счетчик
	[Count] int, -- Количество данной топологии на нашем периоде
	[CommonE422Port] int NULL,
	PRIMARY KEY CLUSTERED([Key_ID], [USPD_ID])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)

--Смотрим на каком уровне собирается счетчик
if (exists(select top (1) 1 from [dbo].[Hard_MetersE422_Links] where [Meter_ID] = @Meter_ID)) begin
--Это уровень Е422
	if (exists(select top 1 1 from JournalDataCollect_Concentrators_Meters_Tree where Meter_ID = @Meter_ID and EventDateTime between @datestart and @dateend)) begin
		declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TreeElement_ID, EventDateTime, MeterReadTimeout, Concentrator_ID 
		from JournalDataCollect_Concentrators_Meters_Tree --Все концентрторы на которых собирался счетчик
			where 
			Meter_ID = @Meter_ID
			and EventDateTime between @datestart and @dateend 
			--and TreeElement_ID <> ParentTreeElement_ID
			order by EventDateTime desc;
			 open t;
			FETCH NEXT FROM t into @s_id,@edt,@mrt, @Concentrator_ID
			WHILE @IsResultComplete = 0
			BEGIN
				set @TITreeList = '';
				set @Key_ID = '';
		
					--Это рекурсивный запрос построения дерева
					WITH SecondaryHierarchyTreeSbor (Concentrator_ID, EventDateTime, MeterReadTimeout, MeterSerialNumber, Meter_ID, ParentTreeElement_ID, TreeElement_ID, PLCMeterStatusFlags,  rn)
					as
					(
						SELECT c.Concentrator_ID, c.EventDateTime, c.MeterReadTimeout, c.MeterSerialNumber, c.Meter_ID, c.ParentTreeElement_ID, c.TreeElement_ID, c.PLCMeterStatusFlags, 1 as rn
						FROM [dbo].[JournalDataCollect_Concentrators_Meters_Tree] c
						where c.Concentrator_ID = @Concentrator_ID and 
						c.EventDateTime = @edt and c.TreeElement_ID = @s_id  --and PLCMeterStatusFlags = 0
						and (@isFirst = 1 OR (@isFirst = 0 and c.PLCMeterStatusFlags = 0))

						UNION ALL --Ниже идет рекурсия
				
						SELECT child.Concentrator_ID, child.EventDateTime, child.MeterReadTimeout, child.MeterSerialNumber, child.Meter_ID, child.ParentTreeElement_ID, child.TreeElement_ID, child.PLCMeterStatusFlags, c.rn + 1
						FROM [dbo].[JournalDataCollect_Concentrators_Meters_Tree] child
						join SecondaryHierarchyTreeSbor c on c.Concentrator_ID = child.Concentrator_ID
						and  c.ParentTreeElement_ID = child.TreeElement_ID 
						and child.EventDateTime = c.EventDateTime
						and child.Meter_ID is not null
						--and child.Concentrator_ID = @Concentrator_ID
						where c.MeterSerialNumber <> child.MeterSerialNumber
					 )

					--Теперь строим путь по данной записи
					select 
					--Это ключ, в этой строке уникальный путь сбора точки
					@Key_ID = @Key_ID + cast(TI_ID as varchar) +',', 
					--Это более детальная информация по дереву (флаг, ключ для схемы)
					@TITreeList = @TITreeList + cast(TI_ID as varchar) 
						+',' + cast(ISNULL(PLCMeterStatusFlags,0) as varchar) 
						+ ',' + cast(Concentrator_ID as varchar) 
						+ ',' + cast(TreeElement_ID as varchar(36))
						+ ',' + cast(MeterReadTimeout as varchar) + ';',
					@c_id = Concentrator_ID
					from SecondaryHierarchyTreeSbor h
					cross apply
								(
									select top (1) * 
									from dbo.Info_Meters_TO_TI
									where METER_ID = h.Meter_ID
										and StartDateTime <= @dateend 
										and (FinishDateTime is null or FinishDateTime >= @datestart)
									order by StartDateTime desc
								) imtt;
			
					--print @TITreeList + ' ' + cast(@edt as varchar);
					--Проверяем есть ли уже такое дерево в нашем конечном результате, если нет то добавляем
					if ( Len(@Key_ID) > 0 )	BEGIN 
						if (not exists(select top 1 1 from #resultTable where [KEY_ID] = @Key_ID)) BEGIN
							insert into #resultTable([Key_ID], TITreeList, EventDateTime, MeterReadTimeout, Concentrator_ID, LinkNumber, StringName, PS_ID, [Count], E422_ID) 
							select @Key_ID, @TITreeList, @edt,@mrt, Concentrator_ID, LinkNumber, StringName, cc.PS_ID, 1, tochannel.E422_ID
							from dbo.Hard_Concentrators hc
							join dbo.Hard_E422CommChannels_Links tochannel on tochannel.E422_ID = hc.E422_ID
							join dbo.Hard_CommChannels cc on cc.CommChannel_ID = tochannel.CommChannel_ID
							where hc.Concentrator_ID = @c_id;
							set @counter = @counter + 1;
						 END ELSE BEGIN
							update #resultTable set [Count] = [Count] + 1 where [KEY_ID] = @Key_ID
						 END 
					end;
						
					if (@counter >= @TopLimit OR @@FETCH_STATUS <> 0)
						set @IsResultComplete = 1;
	
					set @isFirst = 0;
				FETCH NEXT FROM t into @s_id,@edt,@mrt, @Concentrator_ID
			end;
			CLOSE t
			DEALLOCATE t
		end else begin
		insert into #resultTable([Key_ID], TITreeList, EventDateTime, MeterReadTimeout, Concentrator_ID, LinkNumber, StringName, PS_ID, [Count], E422_ID) 
		--Это обычный E422
			select cast(@TI_ID as varchar)+','+cast(ml.E422_ID as varchar), cast(@TI_ID as varchar) 
				+',' + cast(0 as varchar) 
				+ ',' + cast(0 as varchar) 
				+ ',7D4DAE2E-630E-48B6-8FBD-' + REPLICATE('0', 12 - len(@TI_ID)) + str(@TI_ID, len(@TI_ID))
				+ ',' + cast(0 as varchar) + ';'
				,null
				,0, ISNULL(ml.Concentrator_ID, 0), ISNULL(LinkNumber,0), hc.StringName, PS_ID, 1, ml.E422_ID
			from dbo.Hard_MetersE422_Links ml
			join dbo.Hard_E422CommChannels_Links hl on hl.E422_ID = ml.E422_ID
			join dbo.Hard_CommChannels cc on cc.CommChannel_ID = hl.CommChannel_ID
			left join dbo.Hard_Concentrators hc on hc.Concentrator_ID = ml.Concentrator_ID
			where ml.Meter_ID = @Meter_ID;
		end
   end else begin
		declare @USPD_ID int, @PLCMeterStatusFlags tinyint
		set @USPD_ID = (select top 1 USPD_ID from [dbo].[Hard_MetersUSPD_Links] where [Meter_ID] = @Meter_ID);
		if (@USPD_ID is not null) begin
			if ((select top 1 USPDType from Hard_USPD where USPD_ID = @USPD_ID) = 23) begin
				--Это УСПД M225PLC2
				declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TreeElement_ID, EventDateTime, MeterReadTimeout, USPD_ID,PLCMeterStatusFlags 
				from JournalDataCollect_USPD_Incotex_M225PLC2_Meters_Tree --Все концентраторы на которых собирался счетчик
					where Meter_ID = @Meter_ID	and EventDateTime between @datestart and @dateend 
					order by PLCMeterStatusFlags, EventDateTime desc, MeterReadTimeout;
				open t;
				FETCH NEXT FROM t into @s_id,@edt,@mrt, @Concentrator_ID,@PLCMeterStatusFlags
				WHILE @IsResultComplete = 0
				BEGIN

					set @TITreeList = '';
					set @Key_ID = '';
		
						--Это рекурсивный запрос построения дерева
						WITH SecondaryHierarchyTreeSbor (USPD_ID, EventDateTime, MeterReadTimeout, MeterSerialNumber, Meter_ID, ParentTreeElement_ID, TreeElement_ID, PLCMeterStatusFlags,  rn)
						as
						(
							SELECT c.USPD_ID, c.EventDateTime, c.MeterReadTimeout, c.MeterSerialNumber, c.Meter_ID, c.ParentTreeElement_ID, c.TreeElement_ID, c.PLCMeterStatusFlags, 1 as rn
							FROM [dbo].[JournalDataCollect_USPD_Incotex_M225PLC2_Meters_Tree] c
							where c.USPD_ID = @Concentrator_ID and 
							c.EventDateTime = @edt and c.TreeElement_ID = @s_id  --and PLCMeterStatusFlags = 0
							and (@isFirst = 1 OR (@isFirst = 0 and c.PLCMeterStatusFlags = 0))
							
							UNION ALL --Ниже идет рекурсия
				
							SELECT child.USPD_ID, child.EventDateTime, child.MeterReadTimeout, child.MeterSerialNumber, child.Meter_ID, child.ParentTreeElement_ID, child.TreeElement_ID, child.PLCMeterStatusFlags, c.rn + 1
							FROM [dbo].[JournalDataCollect_USPD_Incotex_M225PLC2_Meters_Tree] child
							join SecondaryHierarchyTreeSbor c on c.USPD_ID = child.USPD_ID
							and  c.ParentTreeElement_ID = child.TreeElement_ID 
							and child.EventDateTime = c.EventDateTime
							and child.Meter_ID is not null
							--and child.Concentrator_ID = @Concentrator_ID
							where c.MeterSerialNumber <> child.MeterSerialNumber
							)

						--Теперь строим путь по данной записи
						select 
						--Это ключ, в этой строке уникальный путь сбора точки
						@Key_ID = @Key_ID + cast(TI_ID as varchar) +',', 
						--Это более детальная информация по дереву (флаг, ключ для схемы)
						@TITreeList = @TITreeList + cast(TI_ID as varchar) 
							+',' + cast(ISNULL(PLCMeterStatusFlags,0) as varchar) 
							+ ',' + cast(USPD_ID as varchar) 
							+ ',' + cast(TreeElement_ID as varchar(36))
							+ ',' + cast(MeterReadTimeout as varchar) + ';',
						@c_id = USPD_ID
						from SecondaryHierarchyTreeSbor h
						cross apply
						(
							select top (1) * 
							from dbo.Info_Meters_TO_TI
							where METER_ID = h.Meter_ID
								and StartDateTime <= @dateend 
								and (FinishDateTime is null or FinishDateTime >= @datestart)
							order by StartDateTime desc
						) imtt;
			
						--print @TITreeList + ' ' + cast(@edt as varchar);
						--Проверяем есть ли уже такое дерево в нашем конечном результате, если нет то добавляем
						if ( Len(@Key_ID) > 0 )	BEGIN 
							if (not exists(select top 1 1 from #resultTable where [KEY_ID] = @Key_ID and USPD_ID = @c_id)) BEGIN
								insert into #resultTable([Key_ID], TITreeList, EventDateTime, MeterReadTimeout, USPD_ID, [Count], PS_ID) 
								select @Key_ID, @TITreeList, @edt,@mrt, @c_id, 1, 
								(select top 1 PS_ID from dbo.Hard_USPDCommChannels_Links hl 
								join dbo.Hard_CommChannels cc on cc.CommChannel_ID = hl.CommChannel_ID
								where hl.USPD_ID = @c_id);
								set @counter = @counter + 1;
								END ELSE BEGIN
								update #resultTable set [Count] = [Count] + 1 where [KEY_ID] = @Key_ID and USPD_ID = @c_id
								END 
						end;
						
						if (@counter >= @TopLimit OR @@FETCH_STATUS <> 0)
							set @IsResultComplete = 1;
	
						set @isFirst = 0;
					FETCH NEXT FROM t into @s_id,@edt,@mrt, @Concentrator_ID,@PLCMeterStatusFlags
				end;
				CLOSE t
				DEALLOCATE t
			end else begin
				--Это обычный УСПД
				insert into #resultTable([Key_ID], TITreeList, EventDateTime, MeterReadTimeout, Concentrator_ID, LinkNumber, StringName, PS_ID, [Count], USPD_ID, CommonE422Port) 
				select cast(@TI_ID as varchar)+','+cast(ml.USPD_ID as varchar), cast(@TI_ID as varchar) 
					+',' + cast(0 as varchar) 
					+ ',' + cast(0 as varchar) 
					+ ',7D4DAE2E-630E-48B6-8FBD-' + REPLICATE('0', 12 - len(@TI_ID)) + str(@TI_ID, len(@TI_ID))
					+ ',' + cast(0 as varchar) + ';'
					,null
					,0, 0, 0, '', PS_ID, 1, ml.USPD_ID, ml.CommonE422Port
				from dbo.Hard_MetersUSPD_Links ml
				join dbo.Hard_USPDCommChannels_Links hl on hl.USPD_ID = ml.USPD_ID
				join dbo.Hard_CommChannels cc on cc.CommChannel_ID = hl.CommChannel_ID
				where ml.Meter_ID = @Meter_ID;
			end
		end else if (exists(select top (1) 1 from [dbo].[Master61968_SlaveSystems_EndDeviceAssets_To_Meters] where [Meter_ID] = @Meter_ID)) begin
		----Это уровень 61968
			insert into #resultTable([Key_ID], TITreeList, EventDateTime, MeterReadTimeout, Concentrator_ID, LinkNumber, StringName, Slave61968System_ID, [Count]) 
			select cast(@TI_ID as varchar), cast(@TI_ID as varchar) 
				+',' + cast(0 as varchar) 
				+ ',' + cast(0 as varchar) 
				+ ',7D4DAE2E-630E-48B6-8FBD-' + REPLICATE('0', 12 - len(@TI_ID)) + str(@TI_ID, len(@TI_ID))
 				+ ',' + cast(0 as varchar) + ';'
				,null
				,0, 0, 0, '', a.Slave61968System_ID, 1
			from [dbo].[Master61968_SlaveSystems_EndDeviceAssets_To_Meters] mtm
			join [dbo].[Master61968_SlaveSystems_EndDeviceAssets] a on a.Slave61968EndDeviceAsset_ID = mtm.Slave61968EndDeviceAsset_ID
			where mtm.Meter_ID = @Meter_ID;
		end;
end;


select r.* from #resultTable r
where r.TITreeList is not null and len(r.TITreeList) > 0
order by r.EventDateTime desc

drop table #resultTable;

end
go
   grant EXECUTE on usp2_Monit_GetMeterTreeSbor to [UserCalcService]
go



