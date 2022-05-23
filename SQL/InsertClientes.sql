INSERT INTO `proyecto-mi-dw.datawarehouse.Clientes`(IdCliente,Marca,Pais,Industria) 
VALUES ((select count(1) from `proyecto-mi-dw.datawarehouse.Clientes`)+1,'NUEVO_CLIENTE','Chile','Industria');