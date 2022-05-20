INSERT INTO `proyecto-mi-dw.datawarehouse.Clientes`(IdCliente,Marca,Pais,Automotriz) 
VALUES ((select count(1) from `proyecto-mi-dw.datawarehouse.Clientes`)+1,'BMW','Chile','Automotriz');