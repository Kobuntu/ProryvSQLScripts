if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UA_RefsTable')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UA_RefsTable
go

/****** Object:  UserDefinedFunction [dbo].[iter_intlist_to_table]    Script Date: 09/17/2008 15:58:42 ******/
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
--		Июнь 2014
--
-- Описание:
--
--		Построение дерева дочерних узлов (или всех узлов) OPC UA
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_UA_RefsTable]
(
	@NodeID bigint, --Список идентификаторов узлов
	@IsForward bit, --Раскручиваемое направление (от Destination к Source или иначе )
	@MaxNodeLevel int, -- Максимальный уровень раскрутки
	@Lev int
)
RETURNS @ret TABLE
(
	[UAServer_ID] int NOT NULL, --Сервер
	[FromNode_ID] bigint NOT NULL, -- От чего идем
	[ToNode_ID] bigint NOT NULL, -- К чему идем
	[UAIsForward] bit not null, --Информативно был переворот или нет
	UAReferenceType int NULL, --
	Lev int NOT NULL----, --Уровень вложенности
	PRIMARY KEY CLUSTERED ([FromNode_ID], [ToNode_ID], [UAIsForward]) WITH (IGNORE_DUP_KEY = ON)
)
as
begin
	
	if (@Lev >= @MaxNodeLevel) return 

	if (@IsForward = 1) begin
		insert into @ret
		select UAServer_ID,
		DestinationUANode_ID, SourceUANode_ID, [UAIsForward],
		dbo.usf2_UA_DetectType(UAReferenceType) as UAReferenceType, @Lev
		from UA_Refs c 
		where SourceUANode_ID = @NodeID and DestinationUANode_ID is not null

		--Наследники или родители (в зависимости от направления)
		insert into @ret
		select f.UAServer_ID, 
		f.FromNode_ID, f.ToNode_ID, f.[UAIsForward], f.UAReferenceType, f.Lev
		from @ret t 
		cross apply usf2_UA_RefsTable(t.FromNode_ID, @IsForward, @MaxNodeLevel,  t.Lev + 1) f
		
	end else begin
		insert into @ret
		select UAServer_ID,
		DestinationUANode_ID, SourceUANode_ID, [UAIsForward], 
		dbo.usf2_UA_DetectType(UAReferenceType) as UAReferenceType, @Lev
		from UA_Refs c 
		where DestinationUANode_ID = @NodeID  and SourceUANode_ID is not null

		--Наследники или родители (в зависимости от направления)
		insert into @ret
		select f.UAServer_ID, 
		f.FromNode_ID, f.ToNode_ID, f.[UAIsForward], f.UAReferenceType, f.Lev
		from @ret t 
		cross apply usf2_UA_RefsTable(t.ToNode_ID, @IsForward, @MaxNodeLevel, t.Lev + 1) f

	end
	return
end
go
grant select on usf2_UA_RefsTable to [UserCalcService]
go   