INSERT INTO `proyecto-mi-dw.datawarehouse.ClientesConsumos`(IDClienteConsumo,IDCliente,NombreCliente,TipoConsumo,BaseConsumo,TablaConsumo) 
VALUES ((select count(1) from `proyecto-mi-dw.datawarehouse.ClientesConsumos`)+1,2,'MINI','GoogleCampaign','MINI','Campaign_4595408874'),
((select count(1) from `proyecto-mi-dw.datawarehouse.ClientesConsumos`)+1,2,'MINI','GoogleCampaignBasicStats','MINI','CampaignBasicStats_4595408874'),
((select count(1) from `proyecto-mi-dw.datawarehouse.ClientesConsumos`)+1,2,'MINI','FacebookAds','MINI','facebook_AdCostData'),
((select count(1) from `proyecto-mi-dw.datawarehouse.ClientesConsumos`)+1,2,'MINI','MAXIMO','MINI','MAXIMORENDI_MINI');