if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTree_Update')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTree_Update
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ======================================================================================
-- �����:
--
--		������
--
-- ���� ��������:
--
--		2014
--
-- ��������:
--
--		��������� ������� � ������ ��������� ��������
--
-- ======================================================================================


create proc [dbo].[usp2_FreeHierarchyTree_Update] 
(
@FreeHierTree_ID	int,
@ParentFreeHierItem_ID	int,
@StringName	nvarchar(255),
@FreeHierItemType	tinyint,
@Expanded	bit,
@ObjectStringID nvarchar(255),
@IncludeObjectChildren bit,
@SqlSelectString_ID int,
@FreeHierItem_ID int
)
AS
BEGIN
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

	--����� �������� �������� ���������� ��, ����� � ��� ���� �������� ��� ����� � ������� ���� ����������, ����� ������ ��� ��������/������ ���������� �.�. ������ ��������� ��� ������.
	--����� ������� ������ �������
	--����� �������� ������ ������� � ������� ��
	
	
	if (not exists (select 1 from Dict_FreeHierarchyTree where FreeHierTree_ID= @FreeHierTree_ID and FreeHierItem_ID=@FreeHierItem_ID))
	begin
	  RAISERROR ('������������� ���� ����������� � ���� ������, �������� ������%s',16, 1, '');
	  return ;
	end
	
	
		
	if (@ParentFreeHierItem_ID is not null and not exists (select 1 from Dict_FreeHierarchyTree where FreeHierTree_ID= @FreeHierTree_ID and FreeHierItem_ID=@ParentFreeHierItem_ID))
	begin
	  RAISERROR ('��������� ������������ ���� ����������� � ���� ������, �������� �� ����������� �������������. ����� ���� ������ �������� � �������� ������������%s',16, 1, '');
	  return ;
	end
	
	declare @HierID hierarchyid, @ObjectID int
	IF isnull(@ObjectStringID,'')=''
	BEGIN
		SET @ObjectStringID = NULL
		SET @ObjectID = NULL
	END
	
	
	declare @OldFreeHierItemType tinyint
	
	select @HierID = HierID, @OldFreeHierItemType=FreeHierItemType from Dict_FreeHierarchyTree where FreeHierItem_ID= @FreeHierItem_ID
	
	
	--�������� ��� �� ��� ��������� - ��������� �� ���������� �� ���� �������� ���� �� ��������� ������, ��� �� �� ���� ������� � ������� ���������� �����
	if @FreeHierItemType=4
	begin
		declare @ps_ID int
		select @ps_ID = Dict_PS.PS_ID 
		from Dict_FreeHierarchyTree_Description 
		join Dict_PS on Dict_PS.PS_ID=Dict_FreeHierarchyTree_Description.PS_ID
		where 
		Dict_FreeHierarchyTree_Description.FreeHierItem_ID=@FreeHierItem_ID 
		and Dict_Ps.HierLev3_ID is null
	
		--���� � �� ��� �������� � ��� �������������� �������� ��� �� �� ���� ��� �������� ��������� ������ ������
		--�� ��������� ���������
		if @ps_ID is not null and (isnull(@ObjectID,0)<>@ps_ID or @OldFreeHierItemType!= @FreeHierItemType)
		begin
			if not exists (select 1 from Dict_FreeHierarchyTree_Description where PS_ID is not null and PS_ID=@ps_ID and FreeHierItem_ID=@FreeHierItem_ID)
			begin
				RAISERROR ('��������� ������� ��������� ������ �� ���� ������, �.�. � ���� �� ��������� ������ � ������� ������%s',16, 1, '');
				return ;
			end
		end
		
	end
	
	
	
	IF (@FreeHierItemType<> 6 and @FreeHierItemType<> 14 and @FreeHierItemType<> 29 and isnull(@ObjectStringID,'')!='') SET @ObjectID = convert(INT, @ObjectStringID)

	IF (@SqlSelectString_ID<=0) SET @SqlSelectString_ID = NULL

	IF (@ParentFreeHierItem_ID<=0) SET @ParentFreeHierItem_ID = NULL

	
	--��������� �������
	UPDATE Dict_FreeHierarchyTree 
	SET	StringName = @StringName, 
		FreeHierItemType = @FreeHierItemType, 
		Expanded = @Expanded
	FROM
		Dict_FreeHierarchyTree
	WHERE
		HierID = @HierID 
		AND	FreeHierTree_ID = @FreeHierTree_ID
		
	--��������� ������ �� �������		
	IF NOT EXISTS(SELECT 1 FROM	 Dict_FreeHierarchyTree_Description	 WHERE	 FreeHierItem_ID = @FreeHierItem_ID) 
			INSERT INTO dbo.Dict_FreeHierarchyTree_Description (
					FreeHierItem_ID
 					, IncludeObjectChildren
					, HierLev1_ID
					, HierLev2_ID
					, HierLev3_ID
					, PS_ID
					, TI_ID
					, Formula_UN
					, Section_ID
					, TP_ID
					, USPD_ID
					, XMLSystem_ID
					, JuridicalPersonContract_ID
					, JuridicalPerson_ID
					, DistributingArrangement_ID
					, BusSystem_ID
					, UANode_ID	
					, OurFormula_UN
					, ForecastObject_UN
					, SqlSelectString_ID
					)
			VALUES( @FreeHierItem_ID, 
					@IncludeObjectChildren,
					CASE WHEN @FreeHierItemType = 1 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 2 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 3 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 4 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 5 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 6 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectStringID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 7 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 8 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 9 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 12 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END,  
					CASE WHEN @FreeHierItemType = 10 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 13 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END,
					CASE WHEN @FreeHierItemType = 18 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 19 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END,
					CASE WHEN @FreeHierItemType = 23 THEN @ObjectID ELSE NULL END,
					CASE WHEN @FreeHierItemType = 14 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectStringID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 29 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectStringID	ELSE	NULL	END, 
					@SqlSelectString_ID )	
		ELSE
		UPDATE Dict_FreeHierarchyTree_Description
		SET
				IncludeObjectChildren = @IncludeObjectChildren, 
				HierLev1_ID		= CASE		WHEN @FreeHierItemType = 1 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				HierLev2_ID		= CASE		WHEN @FreeHierItemType = 2 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				HierLev3_ID		= CASE		WHEN @FreeHierItemType = 3 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				PS_ID			= CASE		WHEN @FreeHierItemType = 4 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				TI_ID			= CASE		WHEN @FreeHierItemType = 5 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				Formula_UN		= CASE		WHEN @FreeHierItemType = 6 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectStringID	ELSE			NULL	END, 
				Section_ID		= CASE		WHEN @FreeHierItemType = 7 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				TP_ID			= CASE		WHEN @FreeHierItemType = 8 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				USPD_ID			= CASE		WHEN @FreeHierItemType = 9 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				XMLSystem_ID	= CASE		WHEN @FreeHierItemType = 12 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				JuridicalPersonContract_ID	= CASE		WHEN @FreeHierItemType = 10 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				JuridicalPerson_ID			= CASE		WHEN @FreeHierItemType = 13 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				DistributingArrangement_ID	= CASE		WHEN @FreeHierItemType = 18 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				BusSystem_ID				= CASE		WHEN @FreeHierItemType = 19 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				UANode_ID				= CASE		WHEN @FreeHierItemType = 23 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				OurFormula_UN	= CASE		WHEN @FreeHierItemType = 14 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectStringID	ELSE			NULL	END, 
				ForecastObject_UN	= CASE		WHEN @FreeHierItemType = 29 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectStringID	ELSE			NULL	END, 
				
				SqlSelectString_ID=@SqlSelectString_ID
		WHERE
			FreeHierItem_ID = @FreeHierItem_ID
			
			
			
			
			
	--�������� ������ ����, ������ ���� ��������� �������� - ��������� ���� ������ � ��������� 	 
	declare @OldParentFreeHierItem_ID int
	 
	--������� ������� ����� ���� 
	select @OldParentFreeHierItem_ID=FreeHierItem_ID
	from Dict_FreeHierarchyTree
	where FreeHierTree_ID=@FreeHierTree_Id
	and @HierID.GetAncestor(1)=HierID
	 
	--���� ���������
	IF (isnull(@OldParentFreeHierItem_ID,-1)<> isnull(@ParentFreeHierItem_ID,-1))
	BEGIN
			---------http://ts-soft.ru/blog/hierarchyid/--------------
		


		if (@FreeHierItem_ID=@ParentFreeHierItem_ID)
		begin
		  RAISERROR ('��������� ��������� ��������� ������� � �������� �������������%s',16, 1, '');
		  return ;
		end


		declare 
		@OldParentHierID hierarchyid 		 
	

		--�������� ����� �� ������� �������� ��� ���� � �������� �������������
		SELECT @OldParentHierID = HierID 
		FROM Dict_FreeHierarchyTree 
		WHERE FreeHierItem_ID = @FreeHierItem_ID -- ������������ ������� ����.. ���� ���� �� ������ ���� OldParentFreeHierItem_ID
		and FreeHierTree_ID=@FreeHierTree_ID 

		if exists (
		select HierID.ToString(), * 
		from Dict_FreeHierarchyTree
		WHERE HierID.IsDescendantOf(@OldParentHierID) = 1
		and FreeHierTree_ID=@FreeHierTree_ID
		and  @ParentFreeHierItem_ID=FreeHierItem_ID)
		begin
		  RAISERROR ('��������� ��������� �������� �������� � �������� �������������%s',16, 1, '');
		  return ;
		end

	

		DECLARE
			@reparented_node AS HIERARCHYID, -- ��� ����, ������� �� ����� ������������� �� ����� ��� ���������
			@new_parent_node AS HIERARCHYID, -- ��� ���� ������ ��������
			@max_child_node AS HIERARCHYID,  -- ��� ���� ������������� ������� ������ ��������
			@new_child_node AS HIERARCHYID;  -- ��� ���� ��� ������ ������� ������ ��������

		-- �������� ��� ����, ������� ����� ������������� �� ����� ��� ���������
		SELECT @reparented_node = @HierID 

		-- �������� ��� ���� ������ ��������
		SELECT @new_parent_node = HierID
		FROM Dict_FreeHierarchyTree
		WHERE FreeHierItem_ID = @ParentFreeHierItem_ID
		and FreeHierTree_ID=@FreeHierTree_ID 


		--�������� ��� �������
		declare @MinLvl int
		select @MinLvl = MIN(Dict_FreeHierarchyTree.HierID.GetLevel())
		from Dict_FreeHierarchyTree
		where 
		FreeHierTree_ID=@FreeHierTree_ID
		
		
		-- �������� ��� ���� ������������� ������� ������ ��������
		SELECT @max_child_node = max(HierID)
		FROM Dict_FreeHierarchyTree
		WHERE 
		(@new_parent_node is null
			and Dict_FreeHierarchyTree.HierID.GetLevel()=@MinLvl and Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID	)
			or
			(Dict_FreeHierarchyTree.HierID.GetAncestor(1) =@new_parent_node  and Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID)	
					
			
		
		--���� �������� �� ������ �� ������ parent=0
		IF (@new_parent_node is null) SET @new_parent_node = hierarchyid ::GetRoot()
			
		--���� ������=0 ���� � ��������� ��� ������ �� ��������� � ������� 1
		if (@max_child_node!=@new_parent_node)
			SET @new_child_node = @new_parent_node.GetDescendant(@max_child_node, NULL);	
		else 
		begin
			SELECT @max_child_node = max(HierID)
			FROM Dict_FreeHierarchyTree
			WHERE 
				(Dict_FreeHierarchyTree.HierID.GetAncestor(1) =@max_child_node  and Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID)				

			SET @new_child_node = @new_parent_node.GetDescendant(@max_child_node, NULL);	
		end		
							
		
		-- ������������� ������ ��� ���� ������ �� ����� ��� ���������
		UPDATE Dict_FreeHierarchyTree
		SET HierID = HierID.GetReparentedValue(@reparented_node, @new_child_node)
		WHERE HierID.IsDescendantOf(@reparented_node) = 1
		and FreeHierTree_ID=@FreeHierTree_ID 
	END
	

	declare @sHierID nvarchar(2000)
	set @sHierID=@new_child_node.ToString()

	declare @routing nvarchar(4000)

	declare @jsonObject nvarchar(max)

	select @jsonObject='Dict_FreeHierarchyTree:'+
			 dbo.usf_ConvertSQL_To_JSON(
			  (SELECT top 1 
				n.*
			   FROM Dict_FreeHierarchyTree n 
			   WHERE n.FreeHierItem_ID=@FreeHierItem_ID
			   FOR XML path, root)
			  )
			  +',Dict_FreeHierarchyTree_Description:'+
			  dbo.usf_ConvertSQL_To_JSON(
			  (SELECT top 1 
				n.*
			   FROM Dict_FreeHierarchyTree_Description n 
			   WHERE n.FreeHierItem_ID=@FreeHierItem_ID
			   FOR XML path, root)
			  )



	set @routing =  'Dict_FreeHierarchyTree.Update.' + convert(varchar(200),@FreeHierTree_ID)+'.'+@sHierID 
	exec spclr_MQ_TryPostMessage @jsonObject, @routing, null

	
	select @FreeHierItem_ID
			
END
go
   grant EXECUTE on usp2_FreeHierarchyTree_Update to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Update to [UserDeclarator]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Update to [UserImportService]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Update to [UserExportService]
go

