INSERT INTO TABLE `proyecto-mi-dw.datawarehouse.ResultadosAutomotriz` AS
SELECT ROW_NUMBER() OVER()  as IDResultado,*
FROM (
-- FacebookAds
SELECT 
a.IDCampanaMedio,
c.IDTipoCambio,
'FacebookAds' as FuenteResultado,
a.HomologacionCampana,
a.HomologacionCampanaOriginal,
a.FechaResultado,
a.ResultadosImpresiones,
a.ResultadosClics,
b.ResultadosFormularios,
a.ResultadosValorNeto,
TRUNC(a.ResultadosValorNeto*c.CambioUSD,1) ResultadosValorNetoCalculado  FROM
((SELECT 
campaignName HomologacionCampana,
HomologacionCampanaOriginal,
date FechaResultado ,
IDCampanaMedio, 
SUM(impressions) ResultadosImpresiones,
TRUNC(SUM(adCost),1) ResultadosValorNeto,
SUM(adClicks) ResultadosClics,
FROM
(select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.campaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") campaignName,a.campaignName HomologacionCampanaOriginal,a.date,a.impressions,a.adCost,a.adClicks,b.IDCampanaMedio
from (select campaignName ,date,sum(impressions) impressions,sum(adCost) adCost,sum(adClicks) adClicks
FROM `proyecto-mi-dw.OWOXBI_CostData_6905379b709b276e50966834aefc6d7b.Ads_CostData` group by campaignName,date) a
left join `proyecto-mi-dw.datawarehouse.CampanaMediosAutomotriz` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.campaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") =b.Taxonomia and a.date=b.FechaMeta
)
GROUP BY 1,2,3,4) a
left join (select *
from `proyecto-mi-dw.datawarehouse.TiposCambio` a
where a.FechaActualizacion = (select max(FechaActualizacion) from `proyecto-mi-dw.datawarehouse.TiposCambio` b
where a.Cliente=b.Cliente and a.Industria=b.Industria and a.AnoTipoCambio=b.AnoTipoCambio and a.MesTipoCambio=b.MesTipoCambio
)) c 
on (EXTRACT(YEAR FROM a.FechaResultado) = c.AnoTipoCambio and EXTRACT(MONTH FROM a.FechaResultado) = c.MesTipoCambio and c.Industria='Automotriz' and c.Cliente='BMW')
left join 
(SELECT Homologacion CAMPANA_REGISTRO,
Fecha,
COUNT(Homologacion) ResultadosFormularios 
FROM `proyecto-mi-dw.datawarehouse.LeadsAutomotriz`
GROUP BY 1,2) b  
ON a.FechaResultado = b.Fecha AND a.HomologacionCampana =b.CAMPANA_REGISTRO)
UNION ALL
--Google Ads
SELECT 
a.IDCampanaMedio,
c.IDTipoCambio,
'GoogleAds' as FuenteResultado,
a.HomologacionCampana,
a.HomologacionCampanaOriginal,
a.FechaResultado,
a.ResultadosImpresiones,
a.ResultadosClics,
null ResultadosFormularios,
a.ResultadosValorNeto,
a.ResultadosValorNeto ResultadosValorNetoCalculado  FROM
(SELECT 
CampaignName HomologacionCampana,
HomologacionCampanaOriginal,
_DATA_DATE FechaResultado ,
IDCampanaMedio, 
SUM(Impressions) ResultadosImpresiones,
TRUNC(SUM(cast(Cost/1000000 as numeric)),1) ResultadosValorNeto,
SUM(Clicks) ResultadosClics,
FROM
(select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.CampaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") CampaignName,a.CampaignName HomologacionCampanaOriginal,a._DATA_DATE,a.Impressions,a.Cost,a.Clicks ,b.IDCampanaMedio
FROM (select c.CampaignName,c._DATA_DATE,SUM(cs.Impressions) Impressions,SUM(cs.Cost) Cost,SUM(cs.Clicks) Clicks
FROM `proyecto-mi-dw.datawarehouse.Campaign_4856933837` c 
INNER JOIN `proyecto-mi-dw.datawarehouse.CampaignBasicStats_4856933837` cs
ON (c.CampaignId = cs.CampaignId AND c._DATA_DATE=cs._DATA_DATE) group by c.CampaignName,c._DATA_DATE) a
LEFT JOIN `proyecto-mi-dw.datawarehouse.CampanaMediosAutomotriz` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.CampaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA")=b.Taxonomia and a._DATA_DATE=b.FechaMeta
UNION ALL
SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.TAXONOMIA, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") CampaignName,a.TAXONOMIA HomologacionCampanaOriginal,a.FECHA _DATA_DATE,a.IMPRESIONES Impressions,a.INVERSION Cost,a.CLICKS Clicks,b.IDCampanaMedio 
FROM `proyecto-mi-dw.datawarehouse.MAXIMORENDI_BMW` a
LEFT JOIN `proyecto-mi-dw.datawarehouse.CampanaMediosAutomotriz` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.TAXONOMIA, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA")=b.Taxonomia and a.FECHA=b.FechaMeta
WHERE a.TIPOCAMPANA='Performance Max'
)
GROUP BY 1,2,3,4) a
left join (select *
from `proyecto-mi-dw.datawarehouse.TiposCambio` a
where a.FechaActualizacion = (select max(FechaActualizacion) from `proyecto-mi-dw.datawarehouse.TiposCambio` b
where a.Cliente=b.Cliente and a.Industria=b.Industria and a.AnoTipoCambio=b.AnoTipoCambio and a.MesTipoCambio=b.MesTipoCambio
)) c 
on (EXTRACT(YEAR FROM a.FechaResultado) = c.AnoTipoCambio and EXTRACT(MONTH FROM a.FechaResultado) = c.MesTipoCambio and c.Industria='Automotriz' and c.Cliente='BMW'))