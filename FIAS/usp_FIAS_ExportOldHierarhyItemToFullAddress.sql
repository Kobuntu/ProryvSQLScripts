if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp_FIAS_ExportOldHierarhyItemToFullAddress')
          and type in ('P','PC'))
   drop procedure dbo.usp_FIAS_ExportOldHierarhyItemToFullAddress
go

Create procedure [dbo].usp_FIAS_ExportOldHierarhyItemToFullAddress 
@HierLvl_ID int, @ProryvLevel tinyint
as
begin
/*
лучше чтобы город и улицы были привязаны вручную
иначе получается фигня.
а еще и Hierlvl2 куда заносятся не районы и т.д.
а еще лучше 
вообще разрешить экспорт только домов
а остальное на совести пользователя...
*/
	DECLARE 
	@exportedOFFNAME varchar(200),  
	@exportedType varchar(400),
	@newAOGUID uniqueidentifier,
	@result varchar(400),
	@FIASName varchar(400), 
	@FIASGUID uniqueidentifier,
	@FIASPARENTName varchar(400), 
	@FIASPARENTOffName varchar(400),
	@FIASPARENTGUID uniqueidentifier
	
	SET @newAOGUID=newid()
	SET @exportedOFFNAME = CASE
	WHEN @ProryvLevel = 1 THEN (SELECT StringName  FROM 	 Dict_HierLev1  WHERE 	 HierLev1_ID = @HierLvl_ID)	
	WHEN @ProryvLevel = 2 THEN (SELECT StringName  FROM 	 Dict_HierLev2  WHERE 	 HierLev2_ID = @HierLvl_ID)	
	WHEN @ProryvLevel = 3 THEN (SELECT StringName  FROM 	 Dict_HierLev3  WHERE 	 HierLev3_ID = @HierLvl_ID)	
	WHEN @ProryvLevel = 4 THEN (SELECT StringName  FROM 	 Dict_PS  WHERE 	 PS_ID = @HierLvl_ID) 
	ELSE 'нет данных' END

	SET @exportedType = CASE
	WHEN @ProryvLevel = 1 THEN (SELECT StringName  FROM 	 Dict_Hier_Names  WHERE 	 HierLevel=1)	
	WHEN @ProryvLevel = 2 THEN (SELECT StringName  FROM 	  Dict_Hier_Names  WHERE 	 HierLevel=2)
	WHEN @ProryvLevel = 3 THEN (SELECT StringName  FROM 	 Dict_Hier_Names  WHERE 	 HierLevel=3)
	WHEN @ProryvLevel = 4 THEN '' 
	ELSE 'нет данных' END
  
--print @exportedOFFNAME


	declare @FullAddress_ID int
  					

    if isnull(@exportedOFFNAME,'')=''
	begin
		set @result='объект не найден'
	end
	--!=========== Если это ВЕРХНИЙ уровень (ГОРОД-улица) ==============
	else if @ProryvLevel=1 
	BEGIN  
 			--1) ищем ссылки объекта на таблицу FullAddress
 			select @FIASName=FullAddress, @FIASGUID=FIAS_FullAddress.AOGUID 
 			FROM FIAS_FullAddress join FIAS_FullAddressToHierarchy 
 			on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
 			where  ISNULL(HierLev1_ID,0)=@HierLvl_ID
 			if  (@FIASName is not null)
 			begin
				  set @result ='Уже имеется связь указанного объекта с объектом ФИАС: '+@FIASName + ', ID = '+convert(varchar(100),@FIASGUID)
				  set @newAOGUID=@FIASGUID
 			end
 			--2) если ссылок нет - ищем по названию среди уровней <=7 где parent=null
 			else begin  
 			
					  select 
  						@FIASName=FullAddress, 
  						@FIASGUID=FIAS_FullAddress.AOGUID 
				  		FROM FIAS_FullAddress 
					  Where FullAddress like '%'+@exportedOFFNAME+'%' AND OFFNAME like @exportedOFFNAME AND AOLEVEL<=7	and PARENTGUID is null 
	 
 				--3) если есть такое название то добавляем ссылку на него	
					if  (@FIASName is  null)
					begin  			 				  
  						INSERT INTO FIAS_FullAddress (STATSTATUS , COUNTER , HOUSENUM , ESTSTATUS , BUILDNUM , STRUCNUM , STRSTATUS , HOUSEID , HOUSEGUID , AOGUID , PARENTGUID , FORMALNAME , REGIONCODE , AUTOCODE , AREACODE , CITYCODE , CTARCODE , PLACECODE , STREETCODE , EXTRCODE , SEXTCODE , OFFNAME ,
						   POSTALCODE , IFNSFL , TERRIFNSFL , IFNSUL , TERRIFNSUL , OKATO , OKTMO , UPDATEDATE , SHORTNAME , 
						   AOLEVEL , AOID , PREVID , NEXTID , CODE , PLAINCODE , ACTSTATUS , 
						   CENTSTATUS , OPERSTATUS , CURRSTATUS , 
						   STARTDATE , ENDDATE , NORMDOC , LIVESTATUS , FullAddress , ObjectLat , ObjectLng , PARENTAOID, IsNewRecord)
  						VALUES
							(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @newAOGUID, NULL, @exportedOFFNAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @exportedOFFNAME, 
							NULL, NULL, NULL, NULL, NULL, NULL, NULL, getdate(), @exportedType, 
							1, newid(), NULL, NULL, NULL, NULL, 1, 
							NULL, NULL, 0, 
							getdate(), '2079-01-01', NULL, NULL, @exportedType+' '+@exportedOFFNAME, NULL, NULL, NULL,1)			
  						
					 end
					 else 
						set @newAOGUID=@FIASGUID
				
  					
					select @FullAddress_ID= MAX(FullAddress_ID) from FIAS_FullAddressToHierarchy
					set @FullAddress_ID=isnull(@FullAddress_ID,0)+1
	
					--и добавляем связь
  					if not exists(select top 1 1 from FIAS_FullAddressToHierarchy where AOGUID=@newAOGUID and ISNULL(HierLev1_ID,0)=@HierLvl_ID )
  					insert into FIAS_FullAddressToHierarchy(FullAddress_ID,AOGUID, HierLev1_ID) values (@FullAddress_ID,@newAOGUID,@HierLvl_ID)
  					 					 	
  					
  					set @result = 'Объект экспортирован в ФИАС и добавлена связь с ним.' 	
				   
 			end 	
	END
	ELSE if  @ProryvLevel=2
	BEGIN
			 --для остальных объектов должен быть связан родительский объект иначе не добавляем
			 /*
			 находим родительский объект ФИАС через привязку.. если нет, привязываем родительский
			 +- 1 уровень наверх, вниз..нет
			 ставим уровень AOLEvel = родительскому
			 */
			 --1) находим родительский объект
			 declare @HierLev1_ID int
			 select @HierLev1_ID = HierLev1_ID from Dict_HierLev2 where HierLev2_ID=@HierLvl_ID

			 --2) находим AOGUID из FullAddress для родительского объекта, если нет - создаем этой же процедурой и снова находим
			 select @FIASPARENTName=FullAddress, @FIASPARENTGUID=FIAS_FullAddress.AOGUID 
			 FROM FIAS_FullAddress join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
			 where isnull(HierLev1_ID,0)=@HierLev1_ID 
			 
			 if  (@FIASPARENTGUID is  null)
			 begin
 				exec [usp_FIAS_ExportOldHierarhyItemToFullAddress] @HierLev1_ID,1   
 				select @FIASPARENTName=FullAddress, @FIASPARENTGUID=FIAS_FullAddress.AOGUID 
 				FROM FIAS_FullAddress join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
 				where isnull(HierLev1_ID,0)=@HierLev1_ID 
			 end

			 if @FIASPARENTName is not null
			 begin

			--по аналогии с уровнем 1
			--1) ищем ссылки объекта на таблицу FullAddress
 				select @FIASName=FullAddress, @FIASGUID=FIAS_FullAddress.AOGUID 
 				FROM FIAS_FullAddress 
 				join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
 				where isnull(HierLev2_ID,0)=@HierLvl_ID 
 				if  (@FIASName is not null)
 				begin
					set @result ='Уже имеется связь указанного объекта с объектом ФИАС: '+@FIASName + ', ID = '+convert(varchar(100),@FIASGUID)
					set @newAOGUID=@FIASGUID
 				end
 				
 				--2) если ссылок нет - ищем по названию СРЕДИ ДОЧЕРНИХ уровней верхнего уровня
 				else 
				begin  	
					select 
  					@FIASName=FullAddress, 
  					@FIASGUID=AOGUID 
					from	FIAS_FullAddress 
					where FullAddress like '%'+@exportedOFFNAME+'%' AND OFFNAME like @exportedOFFNAME AND PARENTGUID=@FIASPARENTGUID and PARENTGUID is not null
 
 					--3) если есть такое название то
					if  (@FIASName is  null)
					begin
  						
  						INSERT INTO FIAS_FullAddress (STATSTATUS , COUNTER , HOUSENUM , ESTSTATUS , BUILDNUM , STRUCNUM , STRSTATUS , HOUSEID , HOUSEGUID , AOGUID , PARENTGUID , FORMALNAME , REGIONCODE , AUTOCODE , AREACODE , CITYCODE , CTARCODE , PLACECODE , STREETCODE , EXTRCODE , SEXTCODE , OFFNAME ,
							POSTALCODE , IFNSFL , TERRIFNSFL , IFNSUL , TERRIFNSUL , OKATO , OKTMO , UPDATEDATE , SHORTNAME , 
							AOLEVEL , AOID , PREVID , NEXTID , CODE , PLAINCODE , ACTSTATUS , 
							CENTSTATUS , OPERSTATUS , CURRSTATUS , 
							STARTDATE , ENDDATE , NORMDOC , LIVESTATUS , FullAddress , ObjectLat , ObjectLng , PARENTAOID, IsNewRecord)
  						VALUES
							(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @newAOGUID, @FIASPARENTGUID, @exportedOFFNAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @exportedOFFNAME, 
							NULL, NULL, NULL, NULL, NULL, NULL, NULL, getdate(), @exportedType, 
							5, newid(), NULL, NULL, NULL, NULL, 1, 
							NULL, NULL, 0, 
							getdate(), '2079-01-01', NULL, NULL,@FIASPARENTName+', '+ @exportedType+' '+@exportedOFFNAME, NULL, NULL, NULL,1)	
  	
					end 
					 else 
						set @newAOGUID=@FIASGUID
				
  					
					select @FullAddress_ID= MAX(FullAddress_ID) from FIAS_FullAddressToHierarchy
					set @FullAddress_ID=isnull(@FullAddress_ID,0)+1
					
					  					
					--и добавляем связь
  					if not exists(select top 1 1 from FIAS_FullAddressToHierarchy where AOGUID=@newAOGUID and ISNULL(HierLev2_ID,0)=@HierLvl_ID )
  					insert into FIAS_FullAddressToHierarchy(FullAddress_ID,AOGUID, HierLev2_ID) values (@FullAddress_ID,@newAOGUID,@HierLvl_ID)
  					 					 	
  					
  					set @result = 'Объект экспортирован в ФИАС и добавлена связь с ним.'  
					
 				end 

			 end
	END
	---УРОВЕНь 3 все как для 2
	ELSE if  @ProryvLevel=3
	BEGIN
 
		 --1) находим родительский объект
		 declare @HierLev2_ID int
		 select @HierLev2_ID = HierLev2_ID from Dict_HierLev3 where HierLev3_ID=@HierLvl_ID

		 --2) находим AOGUID из FullAddress для родительского объекта, если нет - создаем этой же процедурой и снова находим
		 select @FIASPARENTName=FullAddress, @FIASPARENTGUID=FIAS_FullAddress.AOGUID 
		 FROM FIAS_FullAddress 		 		 
		  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
		  where isnull(HierLev2_ID,0)=@HierLev2_ID
		  
		 if  (@FIASPARENTGUID is  null)
		 begin
 			--если родительский и выше не добавлены - добавляем
 			exec [usp_FIAS_ExportOldHierarhyItemToFullAddress] @HierLev2_ID,2  
 			
 			select @FIASPARENTName=FullAddress, @FIASPARENTGUID=FIAS_FullAddress.AOGUID 
			 FROM FIAS_FullAddress 		 		 
			  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
			  where isnull(HierLev2_ID,0)=@HierLev2_ID
		 end

		 if @FIASPARENTName is not null and @FIASPARENTGUID is not null
		 begin
			--по аналогии с уровнем 1
			--1) ищем ссылки объекта на таблицу FullAddress
 			select @FIASName=FullAddress, @FIASGUID=FIAS_FullAddress.AOGUID FROM FIAS_FullAddress 
 			 join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
 			 where isnull(HierLev3_ID,0)=@HierLvl_ID 
 			
 			if  (@FIASName is not null)
 			begin
				set @result ='Уже имеется связь указанного объекта с объектом ФИАС: '+@FIASName + ', ID = '+convert(varchar(100),@FIASGUID)
				set @newAOGUID=@FIASGUID
 			end	
 		--2) если ссылок нет - ищем по названию СРЕДИ ДОЧЕРНИХ уровней предыдущего уровня
 			else 
			begin  	
				select 
  				@FIASName=FullAddress, 
  				@FIASGUID=AOGUID 
				from	FIAS_FullAddress 
				where FullAddress like '%'+@exportedOFFNAME+'%' AND OFFNAME like @exportedOFFNAME AND PARENTGUID=@FIASPARENTGUID	and PARENTGUID is not null
 
 				--3) если есть такое название то добавляем ссылку на него	
				if  (@FIASName is null)
				begin
  				
  				INSERT INTO FIAS_FullAddress (STATSTATUS , COUNTER , HOUSENUM , ESTSTATUS , BUILDNUM , STRUCNUM , STRSTATUS , HOUSEID , HOUSEGUID , AOGUID , PARENTGUID , FORMALNAME , REGIONCODE , AUTOCODE , AREACODE , CITYCODE , CTARCODE , PLACECODE , STREETCODE , EXTRCODE , SEXTCODE , OFFNAME ,
					POSTALCODE , IFNSFL , TERRIFNSFL , IFNSUL , TERRIFNSUL , OKATO , OKTMO , UPDATEDATE , SHORTNAME , 
					AOLEVEL , AOID , PREVID , NEXTID , CODE , PLAINCODE , ACTSTATUS , 
					CENTSTATUS , OPERSTATUS , CURRSTATUS , 
					STARTDATE , ENDDATE , NORMDOC , LIVESTATUS , FullAddress , ObjectLat , ObjectLng , PARENTAOID, IsNewRecord)
  				VALUES
					(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @newAOGUID, @FIASPARENTGUID, @exportedOFFNAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @exportedOFFNAME, 
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, getdate(), @exportedType, 
					6, newid(), NULL, NULL, NULL, NULL, 1, 
					NULL, NULL, 0, 
					getdate(), '2079-01-01', NULL, NULL, @FIASPARENTName+', '+ @exportedType+' '+@exportedOFFNAME, NULL, NULL, NULL,1)	
   	
				end 
					 else 
						set @newAOGUID=@FIASGUID
				
  					
					select @FullAddress_ID= MAX(FullAddress_ID) from FIAS_FullAddressToHierarchy
					set @FullAddress_ID=isnull(@FullAddress_ID,0)+1
					
					  					
					--и добавляем связь
  					if not exists(select top 1 1 from FIAS_FullAddressToHierarchy where AOGUID=@newAOGUID and ISNULL(HierLev3_ID,0)=@HierLvl_ID )
  					insert into FIAS_FullAddressToHierarchy(FullAddress_ID,AOGUID, HierLev3_ID) values (@FullAddress_ID,@newAOGUID,@HierLvl_ID)
  					 					 	
  					
  					set @result = 'Объект экспортирован в ФИАС и добавлена связь с ним.' 
				
						
			end
		 end
	END

	---УРОВЕНь 4 - ПОДСТАНЦИЯ- аналогично
	--т.к. дома косячно заносились - убираем название улицы из номера
	-- и при поиске сравниваем с номер+корпус+строение
	ELSE if  @ProryvLevel=4
	BEGIN 
	
		 --1) находим родительский объект
		 declare @HierLev3_ID int
		 select @HierLev3_ID = HierLev3_ID from Dict_PS where PS_ID=@HierLvl_ID

		 --2) находим AOGUID из FullAddress для родительского объекта, если нет - создаем этой же процедурой и снова находим
		 select @FIASPARENTName=FullAddress, 
		 @FIASPARENTGUID=FIAS_FullAddress.AOGUID, 
		 @FIASPARENTOffName=OFFNAME
		 FROM FIAS_FullAddress 
		  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
		 where isnull(HierLev3_ID,0)=@HierLev3_ID
		 
		 if  (@FIASPARENTGUID is  null)
		 begin
 			--если родительский и выше не добавлены - добавляем
 			exec [usp_FIAS_ExportOldHierarhyItemToFullAddress] @HierLev3_ID,3 
 			 
			select @FIASPARENTName=FullAddress, 
			 @FIASPARENTGUID=FIAS_FullAddress.AOGUID, 
			 @FIASPARENTOffName=OFFNAME
			 FROM FIAS_FullAddress 
			  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
			 where isnull(HierLev3_ID,0)=@HierLev3_ID
		 end


		 if @FIASPARENTName is not null and @FIASPARENTGUID is not null
		 begin

		 -->заменяем название(могут назвать цифрой уровень и тогда в названиии дома цифра удалится.. считаем что такого не будет)		  
		 if len(@FIASPARENTName)>3
		 begin
			set @exportedOFFNAME=ltrim(rtrim(replace(@exportedOFFNAME,rtrim(@FIASPARENTOffName)+ ' д.','')))
			set @exportedOFFNAME=ltrim(rtrim(replace(@exportedOFFNAME,rtrim(@FIASPARENTOffName),'')))
		 end

		 
		 --по аналогии с уровнем 1
		 --1) ищем ссылки объекта на таблицу FullAddress
 			select @FIASName=FullAddress, @FIASGUID=FIAS_FullAddress.AOGUID FROM FIAS_FullAddress  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
 			where isnull(PS_ID,0)=@HierLvl_ID 
 			if  (@FIASName is not null)
 			begin
				  set @result ='Уже имеется связь указанного объекта с объектом ФИАС: '+@FIASName + ', ID = '+convert(varchar(100),@FIASGUID)
				  set @newAOGUID=@FIASGUID
 			end
 			
 			--2) если ссылок нет - ищем по названию СРЕДИ ДОЧЕРНИХ уровней предыдущего уровня
 			else begin  	
				  select 
  					@FIASName=FullAddress, 
  					@FIASGUID=AOGUID 
				  FROM	FIAS_FullAddress 
				  Where FullAddress like '%'+@exportedOFFNAME+'%' AND OFFNAME like @exportedOFFNAME AND PARENTGUID=@FIASPARENTGUID	and PARENTGUID is not null
 
 			--3) если есть такое название то добавляем ссылку на него	
				  if  (@FIASName is  null)
				  begin		
						declare @PSType int
						select @PSType=PSType from Dict_PS where PS_ID=@HierLvl_ID

						set @exportedType= case
									when  @PSType=2 THEN 'дом'
									when  @PSType=1 THEN 'тр. ПС'
									when  @PSType=0 THEN 'ПС'
									ELSE '' end	
									
  						INSERT INTO FIAS_FullAddress (STATSTATUS , COUNTER , HOUSENUM , ESTSTATUS , BUILDNUM , STRUCNUM , STRSTATUS , HOUSEID , HOUSEGUID , AOGUID , PARENTGUID , FORMALNAME , REGIONCODE , AUTOCODE , AREACODE , CITYCODE , CTARCODE , PLACECODE , STREETCODE , EXTRCODE , SEXTCODE , OFFNAME ,
						   POSTALCODE , IFNSFL , TERRIFNSFL , IFNSUL , TERRIFNSUL , OKATO , OKTMO , UPDATEDATE , SHORTNAME , 
						   AOLEVEL , AOID , PREVID , NEXTID , CODE , PLAINCODE , ACTSTATUS , 
						   CENTSTATUS , OPERSTATUS , CURRSTATUS , 
						   STARTDATE , ENDDATE , NORMDOC , LIVESTATUS , FullAddress , ObjectLat , ObjectLng , PARENTAOID, IsNewRecord)
  						VALUES
						   (NULL, NULL, @exportedOFFNAME, NULL, NULL, NULL, NULL, newid(), newid(), @newAOGUID, @FIASPARENTGUID, @exportedOFFNAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @exportedOFFNAME, 
						   NULL, NULL, NULL, NULL, NULL, NULL, NULL, getdate(), @exportedType, 
						   9999, newid(), NULL, NULL, NULL, NULL, 1, 
						   NULL, NULL, 0, 
						   getdate(), '2079-01-01', NULL, NULL, replace(@FIASPARENTName+', '+ @exportedType+' '+@exportedOFFNAME,'  ',' '), NULL, NULL, NULL,1)	
		  
				  end 
					 else 
						set @newAOGUID=@FIASGUID
				
  					
					select @FullAddress_ID= MAX(FullAddress_ID) from FIAS_FullAddressToHierarchy
					set @FullAddress_ID=isnull(@FullAddress_ID,0)+1
					
					  					
					--и добавляем связь
  					if not exists(select top 1 1 from FIAS_FullAddressToHierarchy where AOGUID=@newAOGUID and ISNULL(PS_ID,0)=@HierLvl_ID )
  					insert into FIAS_FullAddressToHierarchy(FullAddress_ID,AOGUID, PS_ID) values (@FullAddress_ID,@newAOGUID,@HierLvl_ID)
  					 					 	
  					
  					set @result = 'Объект экспортирован в ФИАС и добавлена связь с ним.' 
			  
			  
 			end 
		 end
	END
	select 	@newAOGUID as ID, @result as Result
	--нужно переделать определение Fulladress
end
go

grant EXECUTE on dbo.usp_FIAS_ExportOldHierarhyItemToFullAddress to UserDeclarator
go