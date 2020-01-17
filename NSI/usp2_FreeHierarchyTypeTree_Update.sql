if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTypeTree_Update')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTypeTree_Update
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
--		��������� ������ ��������� ��������
--
-- ======================================================================================


create proc [dbo].[usp2_FreeHierarchyTypeTree_Update] 
(
@FreeHierTree_ID	int,
@ParentFreeHierTree_ID	int,
@StringName	varchar(255),
@ModuleFilter bigint
)
AS
BEGIN
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
	
	if (not exists (select 1 from Dict_FreeHierarchyTypes where FreeHierTree_ID= @FreeHierTree_ID ))
	begin
	  RAISERROR ('������������� ���� ����������� � ���� ������, �������� ������%s',16, 1, '');
	  return ;
	end
		
	if (@ParentFreeHierTree_ID!= null  and not exists (select 1 from Dict_FreeHierarchyTypes where  FreeHierTree_ID=isnull(@ParentFreeHierTree_ID,0)))
	begin
	  RAISERROR ('��������� ������������ ���� ����������� � ���� ������, �������� �� ����������� �������������. ����� ���� ������ �������� � �������� ������������%s',16, 1, '');
	  return ;
	end
	
	declare @HierID hierarchyid	
	
	select @HierID = HierID from Dict_FreeHierarchyTypes where FreeHierTree_ID= @FreeHierTree_ID	
		
	--��������� �������
	UPDATE Dict_FreeHierarchyTypes 
	SET	StringName = @StringName, ModuleFilter= @ModuleFilter
	FROM
		Dict_FreeHierarchyTypes
	WHERE
		 FreeHierTree_ID= @FreeHierTree_ID
			
	--�������� ������ ����, ������ ���� ��������� �������� - ��������� ���� ������ � ��������� 	 
	declare @OldParentFreeHierTree_ID int
	 
	--������� ������� ����� ���� 
	select @OldParentFreeHierTree_ID=FreeHierTree_ID
	from Dict_FreeHierarchyTypes
	where FreeHierTree_ID=@FreeHierTree_Id
	and @HierID.GetAncestor(1)=HierID
	 
	--���� ���������
	IF (isnull(@OldParentFreeHierTree_ID,-1)<> isnull(@ParentFreeHierTree_ID,-1))
	BEGIN
			---------http://ts-soft.ru/blog/hierarchyid/--------------
		
		--�������� ����� �� ������� �������� ��� ���� � �������� �������������
		if (@FreeHierTree_ID=@ParentFreeHierTree_ID)
		begin
		  RAISERROR ('��������� ��������� ��������� ������� � �������� �������������%s',16, 1, '');
		  return ;
		end


		declare 
		@OldParentHierID hierarchyid 	

		SELECT @OldParentHierID = HierID 
		FROM Dict_FreeHierarchyTypes 
		WHERE FreeHierTree_ID = @FreeHierTree_ID --������������ ������� ����, ������� ����� FreeHierTree_ID ������ @OldParentFreeHierTree_ID
	
		if exists (
		select HierID.ToString(), * 
		from Dict_FreeHierarchyTypes
		WHERE HierID.IsDescendantOf(@OldParentHierID) = 1		
		and  @ParentFreeHierTree_ID=FreeHierTree_ID)
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
		FROM Dict_FreeHierarchyTypes
		WHERE FreeHierTree_ID = @ParentFreeHierTree_ID
	
		--�������� ��� �������
		declare @MinLvl int
		select @MinLvl = MIN(Dict_FreeHierarchyTypes.HierID.GetLevel())
		from Dict_FreeHierarchyTypes
		
		
		-- �������� ��� ���� ������������� ������� ������ ��������
		SELECT @max_child_node = max(HierID)
		FROM Dict_FreeHierarchyTypes
		WHERE 
		(@new_parent_node is null and Dict_FreeHierarchyTypes.HierID.GetLevel()=@MinLvl )
			or
			(Dict_FreeHierarchyTypes.HierID.GetAncestor(1) =@new_parent_node)	
							
		--���� �������� �� ������ �� ������ parent=0
		IF (@new_parent_node is null) SET @new_parent_node = hierarchyid ::GetRoot()
			
		--���� ������=0 ���� � ��������� ��� ������ �� ��������� � ������� 1
		if (@max_child_node!=@new_parent_node)
			SET @new_child_node = @new_parent_node.GetDescendant(@max_child_node, NULL);	
		else 
		begin
			SELECT @max_child_node = max(HierID)
			FROM Dict_FreeHierarchyTypes
			WHERE 
				(Dict_FreeHierarchyTypes.HierID.GetAncestor(1) =@max_child_node )				

			SET @new_child_node = @new_parent_node.GetDescendant(@max_child_node, NULL);	
		end									
		
		-- ������������� ������ ��� ���� ������ �� ����� ��� ���������
		UPDATE Dict_FreeHierarchyTypes
		SET HierID = HierID.GetReparentedValue(@reparented_node, @new_child_node)
		WHERE HierID.IsDescendantOf(@reparented_node) = 1
	END
	
	
	select @FreeHierTree_ID
			
END
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Update to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Update to [UserDeclarator]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Update to [UserImportService]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Update to [UserExportService]
go

