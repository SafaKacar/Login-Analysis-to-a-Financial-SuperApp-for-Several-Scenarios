/*USER LOGINS MTD UU-with LoginDeviceOSTypes*/

declare @m		 as int  =  1, --AY GİRİLİR  --'2017-07-28' ilk tarih
		@BaseDay as Date =  CAST(GETDATE() AS DATE)--dateadd(day, 1, eomonth(getdate(), -12))--CAST(GETDATE() AS DATE) -- SON TARİH GİRİLİR
		;
WITH A1 AS
	(
		select
			[DateDaily]
			,MAX(Rank) UU_MTD_UserLogins
		FROM
			(
			SELECT
				   User_Key
				  ,CAST(MinLoginDate AS DATE) [DateDaily]
				  ,Rank() OVER (Partition By [Year],[Month] Order By MinLoginDate,User_Key) Rank
			FROM
				(
					 SELECT
					 	 User_Key
						,YEAR(CreatedAt) [Year]
						,Month(CreatedAt) [Month]
					 	,MIN(CreatedAt) MinLoginDate--*
					 FROM FACT_UserLogins (nolock) Where LogonState = 0 AND CreatedAt >= dateadd(day, 1, eomonth(getdate(), -@m)) AND CreatedAt < @BaseDay
					 GROUP BY User_Key,YEAR(CreatedAt),Month(CreatedAt)
				) K
				Group By User_Key,[Year],[Month],MinLoginDate
			) L
			GROUP BY [DateDaily]
	), A2 AS
	(
		select
			[DateDaily]
			,MAX(CASE WHEN [LoginDeviceOSType] = 0 THEN LoginDeviceOSType_Rank ELSE NULL END)		  UU_MTD_LoginDeviceOSType_Web_UserLogins
   			,MAX(CASE WHEN [LoginDeviceOSType] = 2 THEN LoginDeviceOSType_Rank ELSE NULL END)		  UU_MTD_LoginDeviceOSType_Android_UserLogins
   			,MAX(CASE WHEN [LoginDeviceOSType] = 1 THEN LoginDeviceOSType_Rank ELSE NULL END)		  UU_MTD_LoginDeviceOSType_IOS_UserLogins
   			,MAX(CASE WHEN [LoginDeviceOSType] = 3 THEN LoginDeviceOSType_Rank ELSE NULL END)		  UU_MTD_LoginDeviceOSType_Huawei_UserLogins
		FROM
			(
			SELECT
				   User_Key
				  ,[LoginDeviceOSType]
				  ,RANK() OVER (Partition By [Year],[Month],[LoginDeviceOSType] Order By MinLoginDate,User_Key) LoginDeviceOSType_Rank
				  ,CAST(MinLoginDate AS DATE) [DateDaily]
			FROM
				(
					 SELECT
					 	 User_Key
						,[LoginDeviceOSType]
						,YEAR(CreatedAt) [Year]
						,Month(CreatedAt) [Month]
					 	,MIN(CreatedAt) MinLoginDate
			--			,CreatedAt - LAG(CreatedAt,1) TimeDifference
					 FROM FACT_UserLogins (nolock) Where LogonState = 0 AND CreatedAt >= dateadd(day, 1, eomonth(getdate(), -@m)) AND CreatedAt < @BaseDay
					 GROUP BY User_Key,[LoginDeviceOSType],YEAR(CreatedAt),Month(CreatedAt)
				) K
				Group By User_Key,[LoginDeviceOSType],[Year],[Month],MinLoginDate
				
			) L
			GROUP BY [DateDaily]
	),A3 AS
	(
		SELECT
			CAST(UL.CreatedAt AS DATE) [DateDaily]
			,COUNT(DISTINCT UL.User_Key) DailyUserCount
			,COUNT(DISTINCT L.UserKey) DailyFinanciallyActiveUserCount
			,COUNT(DISTINCT CASE WHEN [LoginDeviceOSType] = 0 THEN UL.User_Key ELSE NULL END) LoginDeviceOSType_Web_DailyUserCount
			,COUNT(DISTINCT CASE WHEN [LoginDeviceOSType] = 2 THEN UL.User_Key ELSE NULL END) LoginDeviceOSType_Android_DailyUserCount
			,COUNT(DISTINCT CASE WHEN [LoginDeviceOSType] = 1 THEN UL.User_Key ELSE NULL END) LoginDeviceOSType_IOS_DailyUserCount
			,COUNT(DISTINCT CASE WHEN [LoginDeviceOSType] = 3 THEN UL.User_Key ELSE NULL END) LoginDeviceOSType_Huawei_DailyUserCount
			,COUNT(UL.User_Key) UserLoginsCounter
			,COUNT(			CASE WHEN [LoginDeviceOSType] = 0 THEN UL.User_Key ELSE NULL END) LoginDeviceOSType_Web_UserLoginsCounter
			,COUNT(			CASE WHEN [LoginDeviceOSType] = 2 THEN UL.User_Key ELSE NULL END) LoginDeviceOSType_Android_UserLoginsCounter
			,COUNT(			CASE WHEN [LoginDeviceOSType] = 1 THEN UL.User_Key ELSE NULL END) LoginDeviceOSType_IOS_UserLoginsCounter
			,COUNT(			CASE WHEN [LoginDeviceOSType] = 3 THEN UL.User_Key ELSE NULL END) LoginDeviceOSType_Huawei_UserLoginsCounter
			,SUM(COUNT(UL.User_Key)) OVER (ORDER BY CAST(UL.CreatedAt AS DATE))														MTD_UserLoginsCounter
			,SUM(COUNT(		CASE WHEN [LoginDeviceOSType] = 0 THEN UL.User_Key ELSE NULL END)) OVER (ORDER BY CAST(UL.CreatedAt AS DATE))	MTD_LoginDeviceOSType_Web_UserLoginsCounter
			,SUM(COUNT(		CASE WHEN [LoginDeviceOSType] = 2 THEN UL.User_Key ELSE NULL END)) OVER (ORDER BY CAST(UL.CreatedAt AS DATE))	MTD_LoginDeviceOSType_Android_UserLoginsCounter
			,SUM(COUNT(		CASE WHEN [LoginDeviceOSType] = 1 THEN UL.User_Key ELSE NULL END)) OVER (ORDER BY CAST(UL.CreatedAt AS DATE))	MTD_LoginDeviceOSType_IOS_UserLoginsCounter
			,SUM(COUNT(		CASE WHEN [LoginDeviceOSType] = 3 THEN UL.User_Key ELSE NULL END)) OVER (ORDER BY CAST(UL.CreatedAt AS DATE))	MTD_LoginDeviceOSType_Huawei_UserLoginsCounter
			,COUNT(DISTINCT CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE)					   THEN UL.User_Key ELSE NULL END) FirstLoginDailyUserCount
			,COUNT(DISTINCT CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE) AND UL.[LoginDeviceOSType] = 0 THEN UL.User_Key ELSE NULL END) FirstLoginLoginDeviceOSType_Web_DailyUserCount
			,COUNT(DISTINCT CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE) AND UL.[LoginDeviceOSType] = 2 THEN UL.User_Key ELSE NULL END) FirstLoginLoginDeviceOSType_Android_DailyUserCount
			,COUNT(DISTINCT CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE) AND UL.[LoginDeviceOSType] = 1 THEN UL.User_Key ELSE NULL END) FirstLoginLoginDeviceOSType_IOS_DailyUserCount
			,COUNT(DISTINCT CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE) AND UL.[LoginDeviceOSType] = 3 THEN UL.User_Key ELSE NULL END) FirstLoginLoginDeviceOSType_Huawei_DailyUserCount
			,COUNT(			CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE)					   THEN UL.User_Key ELSE NULL END) FirstLoginUserLoginsCounter
			,COUNT(			CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE) AND UL.[LoginDeviceOSType] = 0 THEN UL.User_Key ELSE NULL END) FirstLoginLoginDeviceOSType_Web_UserLoginsCounter
			,COUNT(			CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE) AND UL.[LoginDeviceOSType] = 2 THEN UL.User_Key ELSE NULL END) FirstLoginLoginDeviceOSType_Android_UserLoginsCounter
			,COUNT(			CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE) AND UL.[LoginDeviceOSType] = 1 THEN UL.User_Key ELSE NULL END) FirstLoginLoginDeviceOSType_IOS_UserLoginsCounter
			,COUNT(			CASE WHEN CAST(UL.CreatedAt AS DATE) = CAST(U.CreatedAt AS DATE) AND UL.[LoginDeviceOSType] = 3 THEN UL.User_Key ELSE NULL END) FirstLoginLoginDeviceOSType_Huawei_UserLoginsCounter
			,AVG(TimeDifference) DailyAvgTimeDifferenceByDay
			,SUM(SUM(TimeDifference)) OVER (PARTITION BY YEAR(ul.CreatedAt), MONTH(ul.CreatedAt) ORDER BY CAST(UL.CreatedAt AS DATE)) / COUNT(TimeDifference) MTD_AvgTimeDifferenceByDay
			,AVG(DATEDIFF(DAY,U.CreatedAt,UL.CreatedAt)/365.25) AvgUserLifeTimeByYear
			,AVG(CASE WHEN UL.CreatedAt > U.DateOfBirth AND UserType != 0 THEN DATEDIFF(DAY,U.DateOfBirth,UL.CreatedAt)/365.25 ELSE NULL END) AvgUserAgeByLogin
		FROM (SELECT *,DATEDIFF(MINUTE,CreatedAt,LAG(CreatedAt,1)  OVER (Partition By User_Key ORDER BY CreatedAt DESC))/(60.0*24) TimeDifference FROM FACT_UserLogins (Nolock) where CreatedAt >= dateadd(day, 1, eomonth(getdate(), -@m)) AND CreatedAt < @BaseDay) UL
		JOIN DIM_Users (nolock) U ON U.User_Key = UL.User_Key
		LEFT JOIN FACT_Ledger (nolock) L ON L.UserKey = UL.User_Key AND L.CreatedAt >= dateadd(day, 1, eomonth(getdate(), -@m)) AND L.CreatedAt < @BaseDay AND IsCancellation = 0
		WHERE LogonState = 0 AND UL.CreatedAt >= dateadd(day, 1, eomonth(getdate(), -@m)) AND UL.CreatedAt < @BaseDay
		GROUP BY YEAR(ul.CreatedAt), MONTH(ul.CreatedAt),CAST(UL.CreatedAt AS DATE)
	),A4 AS
		(
		select
			[DateDaily]
			,MAX(Rank_FinAktif) UU_MTD_UserLogins_FinanciallyActive
		FROM
			(
			SELECT
				   User_Key
				  ,CAST(MinFinDate AS DATE) [DateDaily]
				  ,Rank() OVER (Partition By [Year],[Month] Order By MinFinDate,User_Key) Rank_FinAktif
			FROM
				(
					 SELECT
					 	 User_Key
						,YEAR(L.CreatedAt)  [Year]
						,Month(L.CreatedAt) [Month]
						,MIN(L.CreatedAt)  MinFinDate
					 FROM FACT_UserLogins (nolock) UL 
					 JOIN FACT_Ledger (nolock) L ON L.UserKey = UL.User_Key AND L.CreatedAt >= dateadd(day, 1, eomonth(getdate(), -@m)) AND L.CreatedAt < @BaseDay AND IsCancellation = 0 
					 Where LogonState = 0 AND UL.CreatedAt >= dateadd(day, 1, eomonth(getdate(), -@m)) AND UL.CreatedAt < @BaseDay
					 GROUP BY User_Key,YEAR(L.CreatedAt),Month(L.CreatedAt)
				) K
				Group By User_Key,[Year],[Month],MinFinDate
			) L
			GROUP BY [DateDaily]
	)

--	INSERT INTO DWH_Workspace..UserLoginAnalysis
	select 
		 CAST(D.CreateDate AS DATE) [Date]
		,ISNULL(DailyUserCount					,0) DailyUserCount					
		,ISNULL(LoginDeviceOSType_Web_DailyUserCount		,0)	LoginDeviceOSType_Web_DailyUserCount		
		,ISNULL(LoginDeviceOSType_Android_DailyUserCount	,0)	LoginDeviceOSType_Android_DailyUserCount	
		,ISNULL(LoginDeviceOSType_IOS_DailyUserCount		,0)	LoginDeviceOSType_IOS_DailyUserCount		
		,ISNULL(LoginDeviceOSType_Huawei_DailyUserCount	,0)	LoginDeviceOSType_Huawei_DailyUserCount
		,ISNULL(UserLoginsCounter				,0)		UserLoginsCounter	
		,ISNULL(LoginDeviceOSType_Web_UserLoginsCounter		,0) LoginDeviceOSType_Web_UserLoginsCounter		
		,ISNULL(LoginDeviceOSType_Android_UserLoginsCounter	,0)	LoginDeviceOSType_Android_UserLoginsCounter	
		,ISNULL(LoginDeviceOSType_IOS_UserLoginsCounter		,0)	LoginDeviceOSType_IOS_UserLoginsCounter
		,ISNULL(LoginDeviceOSType_Huawei_UserLoginsCounter	,0)	LoginDeviceOSType_Huawei_UserLoginsCounter
		,ISNULL(FirstLoginDailyUserCount					 ,0) FirstLoginDailyUserCount					 
		,ISNULL(FirstLoginLoginDeviceOSType_Web_DailyUserCount		 ,0) FirstLoginLoginDeviceOSType_Web_DailyUserCount		
		,ISNULL(FirstLoginLoginDeviceOSType_Android_DailyUserCount	 ,0) FirstLoginLoginDeviceOSType_Android_DailyUserCount	
		,ISNULL(FirstLoginLoginDeviceOSType_IOS_DailyUserCount		 ,0) FirstLoginLoginDeviceOSType_IOS_DailyUserCount		
		,ISNULL(FirstLoginLoginDeviceOSType_Huawei_DailyUserCount	 ,0) FirstLoginLoginDeviceOSType_Huawei_DailyUserCount	
		,ISNULL(FirstLoginUserLoginsCounter					 ,0) FirstLoginUserLoginsCounter					
		,ISNULL(FirstLoginLoginDeviceOSType_Web_UserLoginsCounter	 ,0) FirstLoginLoginDeviceOSType_Web_UserLoginsCounter	
		,ISNULL(FirstLoginLoginDeviceOSType_Android_UserLoginsCounter ,0) FirstLoginLoginDeviceOSType_Android_UserLoginsCounter
		,ISNULL(FirstLoginLoginDeviceOSType_IOS_UserLoginsCounter	 ,0) FirstLoginLoginDeviceOSType_IOS_UserLoginsCounter	
		,ISNULL(FirstLoginLoginDeviceOSType_Huawei_UserLoginsCounter	 ,0) FirstLoginLoginDeviceOSType_Huawei_UserLoginsCounter	
		,UU_MTD_UserLogins
		,UU_MTD_UserLogins_FinanciallyActive
		,UU_MTD_LoginDeviceOSType_Web_UserLogins
		,UU_MTD_LoginDeviceOSType_Android_UserLogins
		,UU_MTD_LoginDeviceOSType_IOS_UserLogins
		,UU_MTD_LoginDeviceOSType_Huawei_UserLogins
		,MTD_UserLoginsCounter
		,MTD_LoginDeviceOSType_Web_UserLoginsCounter
		,MTD_LoginDeviceOSType_Android_UserLoginsCounter
		,MTD_LoginDeviceOSType_IOS_UserLoginsCounter
		,MTD_LoginDeviceOSType_Huawei_UserLoginsCounter
		,DailyAvgTimeDifferenceByDay
		,MTD_AvgTimeDifferenceByDay
		,AvgUserLifeTimeByYear
		,AvgUserAgeByLogin

	INTO DWH_Workspace.[DB\skacar].UserLoginAnalysis
	from A1
	join A2 ON A1.[DateDaily] = A2.[DateDaily]
	join A3 ON A1.[DateDaily] = A3.[DateDaily]
	JOIN A4 ON A1.[DateDaily] = A4.[DateDaily]
	RIGHT JOIN DIM_Date d ON a1.DateDaily = D.CreateDate WHERE D.CreateDate >= dateadd(day, 1, eomonth(getdate(), -@m)) AND D.CreateDate <@BaseDay---->> yukarıya DIM_Date'ten Date alanı beslenir. Aşağıdaki sorguda da NULL MTD'ler her MTD alanı için dolar

--LOOPS >> Hepsi için uygulanmalı -- ESKİ TARİHLERDEKİ BOŞ GÜNLERİ MTD ile doldurmak için
		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE UU_MTD_UserLogins IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.UU_MTD_UserLogins = Z2.UU_MTD_UserLogins    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE UU_MTD_LoginDeviceOSType_Web_UserLogins IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.UU_MTD_LoginDeviceOSType_Web_UserLogins = Z2.UU_MTD_LoginDeviceOSType_Web_UserLogins    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE UU_MTD_LoginDeviceOSType_Android_UserLogins IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.UU_MTD_LoginDeviceOSType_Android_UserLogins = Z2.UU_MTD_LoginDeviceOSType_Android_UserLogins    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE UU_MTD_LoginDeviceOSType_IOS_UserLogins IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.UU_MTD_LoginDeviceOSType_IOS_UserLogins = Z2.UU_MTD_LoginDeviceOSType_IOS_UserLogins    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE UU_MTD_LoginDeviceOSType_Huawei_UserLogins IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.UU_MTD_LoginDeviceOSType_Huawei_UserLogins = Z2.UU_MTD_LoginDeviceOSType_Huawei_UserLogins    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE MTD_UserLoginsCounter IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.MTD_UserLoginsCounter = Z2.MTD_UserLoginsCounter    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE MTD_LoginDeviceOSType_Web_UserLoginsCounter IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.MTD_LoginDeviceOSType_Web_UserLoginsCounter = Z2.MTD_LoginDeviceOSType_Web_UserLoginsCounter    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE MTD_LoginDeviceOSType_Android_UserLoginsCounter IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.MTD_LoginDeviceOSType_Android_UserLoginsCounter = Z2.MTD_LoginDeviceOSType_Android_UserLoginsCounter    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE MTD_LoginDeviceOSType_IOS_UserLoginsCounter IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.MTD_LoginDeviceOSType_IOS_UserLoginsCounter = Z2.MTD_LoginDeviceOSType_IOS_UserLoginsCounter    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end

		--WHILE (SELECT COUNT(DateDaily) FROM DWH_Workspace.[DB\skacar].UserLoginAnalysis WHERE MTD_LoginDeviceOSType_Huawei_UserLoginsCounter IS NULL) != 0
		--BEGIN
		--UPDATE Z1
		--set Z1.MTD_LoginDeviceOSType_Huawei_UserLoginsCounter = Z2.MTD_LoginDeviceOSType_Huawei_UserLoginsCounter    
		--from DWH_Workspace.[DB\skacar].UserLoginAnalysis Z1
		--join DWH_Workspace.[DB\skacar].UserLoginAnalysis Z2 on dateadd(DAY,1,Z2.[DateDaily]) = Z1.[DateDaily] AND YEAR(dateadd(DAY,1,Z2.[DateDaily])) = YEAR(Z1.[DateDaily]) AND MONTH(dateadd(DAY,1,Z2.[DateDaily])) = MONTH(Z1.[DateDaily])
		--end