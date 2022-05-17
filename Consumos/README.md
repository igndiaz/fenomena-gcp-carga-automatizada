# Consumos FacebookAds y GoogleAds

En esta sección, se especifican las consultas SQL tipo que soportan la carga diaria de los consumos de GoogleAds y FacebookAds en la tabla Resultados asociada a la industria del cliente. Además. se incorporan sentencias programadas para la tabla que consolida la información por industria para DataStudio y la ejecutada por el script automatizado con cada carga de Leads y Plan de Medios.

El contenido corresponde a:
- __ResultadosCargaAutomatizada.sql__: Es la sentencia SQL que se ejecuta cada vez que se realiza la carga de Leads o Plan de Medios. Dentro del script se encuentra parametrizada según la información de la tabla ConsumosCliente.
- __ResultadosCargaDataStudio.sql__: Es la sentencia SQL que se debe programar por industria con periodicidad diaria para alimentar la tabla de ResultadosDataEstudioIndustria que consume Data Studio.
- __ResultadosFacebookAds.sql__: Es la sentencia sql por cliente que se debe programar diaramiente para que alimente la tabla resultados de la industria correspondiente con los consumos del cliente en FacebookAds.
- __ResultadosGoogleAds.sql__: Es la sentencia sql por cliente que se debe programar diaramiente para que alimente la tabla resultados de la industria correspondiente con los consumos del cliente en GoogleAds.