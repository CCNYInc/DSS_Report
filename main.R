library(tidyverse)
library(DBI)
library(odbc)

output_name = 'DSS Financial February 2022.xlsx'
con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "ccnyss2", Database = "Invoice", Trusted_Connection = "True")
order <-c(1,2,3,4,5,6,7,16,17,8,9,10,11,12,13,14,15)
a <- con %>%  dbGetQuery("
DECLARE @StartDate DATE = '2022-02-01', @EndDate DATE = '2022-02-28';

SELECT DISTINCT
	COALESCE(T.YID, NULL) AS [Youth ID (Fidelity EHR)],
	COALESCE(T.LastName, NULL) AS [Last Name],
	COALESCE(T.FirstName, NULL) AS [First Name],
	COALESCE(T.DOB, NULL) AS [Date of Birth],
	MedicaidNumber AS [Identified Client CIN (Connections)],
	Fund.YouthFundingText AS [Eligibility Code],
	I.[Recipient of Service],
	I.[Service Note ID],
	CAST(I.[Date of Service] AS DATE) AS [Service Date],
	CR.[Code No] AS [Service Code],
	CASE WHEN CR.[Code No] < 9000 THEN I.[Hours] ELSE NULL END AS [Hours],
	CASE WHEN CR.[Code No] < 9000 THEN I.Rate ELSE NULL END AS [Hourly Rate],
	I.[Hours] * I.Rate AS Cost,
	CAST(I.[Pay On Date] AS DATE) AS [Payment Date],
	CASE WHEN CR.[Code No] < 9000 THEN 'Vendor Services' ELSE 'Flex Funds' END AS [Service Type],
	z.CIN AS [Serviced CIN],
	x.CurrentServices as [Serviced Eligibility Code]
   
	
FROM Invoice I
    LEFT OUTER JOIN CodeRate CR ON CR.[ID] = I.Code
	OUTER APPLY
	(
		SELECT TOP 1
			YD.YID,
			DOB,
			LastName,
			FirstName,
			CASE WHEN MedicaidNumber LIKE '[a-z][a-z][0-9][0-9][0-9][0-9][0-9][a-z]' THEN MedicaidNumber ELSE 'Typo Error' END AS MedicaidNumber
		FROM
			FEHR_DW.dbo.ROT_YouthDemographics YD
			INNER JOIN FEHR_DW.dbo.ROT_Enrollment E ON YD.YID = E.YID
			INNER JOIN FEHR_DW.dbo.ROT_ServiceNote SN ON E.EpisodeID = SN.EpisodeID
		WHERE
			SN.ServiceNoteID = I.[Service Note ID]
		ORDER BY
			E.EnrollmentDate DESC
	) T
	OUTER APPLY(
		SELECT * FROM CCA as f
	) Q
	OUTER APPLY(
		SELECT l.ServiceNoteID,
		STUFF((
		SELECT ' | '+ CASE WHEN o.CIN  LIKE '[a-z][a-z][0-9][0-9][0-9][0-9][0-9][a-z]' THEN o.CIN  ELSE 'Typo Error' END
		FROM (
			Select DISTINCT * FROM(
				Select PersonContacted, ServiceNotePersonContactedID, a.ServiceNoteId,CIN 
				FROM 
			FEHR_DW.dbo.ROT_ServiceNotePersonsContacted a 
			LEFT JOIN FEHR_DW.dbo.ROT_ServiceNote as sn ON a.ServiceNoteID = sn.ServiceNoteID
			LEFT JOIN FEHR_DW.dbo.ROT_TeamMembers t ON LEFT( a.EpisodeID, Charindex('|', a.EpisodeID) - 1) = t.YID AND a.PersonContactedFromTeamId = t.TeamID
			LEFT JOIN FEHR_DW.dbo.ROT_FamilyMembers f ON t.YID = f.YID AND t.UID = f.FamilyID
		WHERE 
		TeamMemberType <> 'Formal'
			AND TeamMemberType <> 'Informal'
			AND TeamMemberType <> 'Youth'
			AND IsDeleted = 0
			AND (CIN NOT LIKE '%null%' OR CIN <> '' OR CIN IS NOT NULL)
		UNION
		Select PersonContacted, ServiceNotePersonContactedID, a.ServiceNoteId,MedicaidNumber as CIN
		FROM
			FEHR_DW.dbo.ROT_ServiceNotePersonsContacted a 
			LEFT JOIN FEHR_DW.dbo.ROT_ServiceNote as sn ON a.ServiceNoteID = sn.ServiceNoteID
			LEFT JOIN FEHR_DW.dbo.ROT_TeamMembers t ON LEFT( a.EpisodeID, Charindex('|', a.EpisodeID) - 1) = t.YID AND a.PersonContactedFromTeamId = t.TeamID
			LEFT JOIN FEHR_DW.dbo.ROT_YouthDemographics f ON t.YID = f.YID 
		WHERE 
			TeamMemberType = 'Youth'
			AND IsDeleted = 0
			AND (MedicaidNumber NOT LIKE '%null%' OR MedicaidNumber <> '' OR MedicaidNumber IS NOT NULL)
		)j
		WHERE CIN NOT LIKE '%null%'
		)o
		WHERE o.ServiceNoteId = l.ServiceNoteId
		FOR XML PATH('')),1,3,'')  AS CIN
	
	FROM FEHR_DW.dbo.ROT_ServiceNote as l
	WHERE l.ServiceNoteID = I.[Service Note ID]
	GROUP BY l.ServiceNoteId

	) z
	OUTER APPLY(
		SELECT l.ServiceNoteID,
		STUFF((
		SELECT ' | '+ CASE WHEN o.CurrentServices <> 'null' THEN o.CurrentServices  ELSE 'None' END
		FROM (
			Select DISTINCT * FROM(
				Select PersonContacted, ServiceNotePersonContactedID, a.ServiceNoteId,CurrentServices 
				FROM 
			FEHR_DW.dbo.ROT_ServiceNotePersonsContacted a 
			LEFT JOIN FEHR_DW.dbo.ROT_ServiceNote as sn ON a.ServiceNoteID = sn.ServiceNoteID
			LEFT JOIN FEHR_DW.dbo.ROT_TeamMembers t ON LEFT( a.EpisodeID, Charindex('|', a.EpisodeID) - 1) = t.YID AND a.PersonContactedFromTeamId = t.TeamID
			LEFT JOIN FEHR_DW.dbo.ROT_FamilyMembers f ON t.YID = f.YID AND t.UID = f.FamilyID
		WHERE 
		TeamMemberType <> 'Formal'
			AND TeamMemberType <> 'Informal'
			AND TeamMemberType <> 'Youth'
			AND IsDeleted = 0
			AND (CIN NOT LIKE '%null%' OR CIN <> '' OR CIN IS NOT NULL)
		UNION
		Select PersonContacted, ServiceNotePersonContactedID, a.ServiceNoteId,YouthFundingText as CurrentServices
		FROM
			FEHR_DW.dbo.ROT_ServiceNotePersonsContacted a 
			LEFT JOIN FEHR_DW.dbo.ROT_ServiceNote as sn ON a.ServiceNoteID = sn.ServiceNoteID
			LEFT JOIN FEHR_DW.dbo.ROT_TeamMembers t ON LEFT( a.EpisodeID, Charindex('|', a.EpisodeID) - 1) = t.YID AND a.PersonContactedFromTeamId = t.TeamID
			LEFT JOIN FEHR_DW.dbo.ROT_YouthFunding f ON t.YID = f.YID 
		WHERE 
			TeamMemberType = 'Youth'
			AND IsDeleted = 0
			AND (YouthFundingText NOT LIKE '%null%' OR YouthFundingText <> '' OR YouthFundingText IS NOT NULL)
		)j
		WHERE CIN NOT LIKE '%null%'
		)o
		WHERE o.ServiceNoteId = l.ServiceNoteId
		FOR XML PATH('')),1,3,'')  AS CurrentServices
	
	FROM FEHR_DW.dbo.ROT_ServiceNote as l
	WHERE l.ServiceNoteID = I.[Service Note ID]
	GROUP BY l.ServiceNoteId

	) x
	OUTER APPLY(
		SELECT YouthFundingText FROM FEHR_DW.dbo.ROT_YouthFunding F
		WHERE I.YID = F.YID AND I.[Date of Service] > F.StartDate AND ( I.[Date of Service] < F.EndDate OR F.EndDate IS NULL)
	) Fund

WHERE
    (CONVERT(DATE,I.[Date of Service]) BETWEEN @StartDate AND @EndDate)

	AND (T.YID NOT IN ('76090', '76347', '76379','76263') OR T.YID IS NULL)
	AND I.CCA NOT IN 
	(
		'OMH/SED Gateway-Longview, Inc.'
	)
	AND (I.[Invoice #] != 0 AND I.[Invoice #] != 1)

ORDER BY
	[Service Note ID],
	[Last Name],
	[First Name],
	[Date of Birth],
	[Service Date],
	[Payment Date]
;")
              
a <- a[,order] 

a$`Date of Birth` <- format(as.Date(a$`Date of Birth`, format = "%Y-%m-%d"), "%m-%d-%Y")
a$`Service Date` <-  format(as.Date(a$`Service Date`, format = "%Y-%m-%d"), "%m-%d-%Y")
a$`Payment Date` <-  format(as.Date(a$`Payment Date`, format = "%Y-%m-%d"), "%m-%d-%Y")





library(openxlsx)
wb <- createWorkbook()
addWorksheet(wb, "Data")
writeData(wb,"Data",a)


currency = createStyle(numFmt = "CURRENCY")
date = createStyle(numFmt = "mm/dd/yyyy")

addStyle(wb,"Data",currency,2:100000,14:15,gridExpand = T)
addStyle(wb,"Data",date,2:100000,4,gridExpand = T)


saveWorkbook(wb, file=output_name, overwrite = TRUE)
