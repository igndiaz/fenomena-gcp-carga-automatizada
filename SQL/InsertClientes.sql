INSERT INTO `proyecto-mi-dw.datawarehouse.Clientes`(IdCliente,Marca,Pais) 
VALUES ((select count(1) from `proyecto-mi-dw.datawarehouse.Clientes`)+1,'CLIENTE_NUEVO','Chile');