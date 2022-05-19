# Carga Automatizada Plan de Medios

Este repositorio contiene el código y archivo de parámetros asociado para configurar en GCP el proceso de carga automatizado de Plan de Medios.

## Protocolo de Carga
### **1. Revisión archivo Excel**

Lo primero que debemos hacer es verificar lo siguiente en el archivo de Leads a cargar:

- El archivo debe ser de extensión .xlsx (archivo de Excel)
- El nombre del archivo debe tener la estructura “Plan [Nombre Cliente] [Información Adicional].xlsx. Por ejemplo “Plan BMW - AFS - Enero 2022_v3.xlsx”. (Es crítico que entre Plan, el nombre del cliente y la información adicional exista la separación por espacios).
- El archivo debe tener una hoja con la información de plan de medios llamada __PLAN + TAXONOMIA__(Es importante que este escrito de esta misma forma).
- La tabla de información del Plan de Medios, debe comenzar después de dos espacios al comienzo.
- La tabla de información debe finalizar y después de dos espacios, aparecer el cuadrado que contenga el texto _Estructura base del plan de medios_

### **2. Construcción de archivo Parámetros**

Para la correcta ejecución del proceso de carga de Plan de Medios, se debe construir un archivo tipo JSON asociado a los parámetros de carga. En este archivo se debe definir lo siguiente:

- La industria correspondiente al cliente (*Campo Industria*)
- Diccionario con el mapeo de campos y estructura base de tabla PlanMedios (*Campo PlanMedios*)
- Diccionario con el mapeo de campos y estructura base de tabla CampanaMedios (*Campo CampanaMedios*)
- Diccionario con el mapeo de campos y estructura base de tabla RegionCampana (*Campo RegionCampana*)
- Diccionario con el mapeo de campos y estructura base de tabla Campana (*Campo Campana*)
- Diccionario con el mapeo de campos y estructura base de tabla Medios (*Campo Medios*)
- Diccionario con el mapeo de campos, asociado a columnas del tipo String que pudieran tener valores nulos (*Campo CamposStringVacios*)
- Diccionario de reemplazos a ejecutar en la Taxonomía (*Campo REEMPLAZOS*)

  
A continuación un archivo *json* de ejemplo:

```json
{
"Industria":"Automotriz",
"PlanMedios": {"CAMPAÑA":"NombrePlan","AÑO":"AnoPlan","MES / PERIODO":"MesPlan","IMPRESIONES":"MetaPlanImpresiones","CLICS":"MetaPlanClics","CPC":"MetaPlanCPC","CTR":"MetaPlanCTR","CPM":"MetaPlanCPM","FORMULARIOS":"MetaPlanFormularios","CPL":"MetaPlanCPL","VIEWS":"MetaPlanViews","CPV":"MetaPlanCPV","VALOR NETO":"MetaPlanValorNeto"},
"CampanaMedios": {"CAMPAÑA":"NombrePlan","AÑO":"AnoPlan","MES / PERIODO":"MesPlan","REGION":"RegionCampana","PAIS":"PaisCampana","SUCURSAL":"SucursalCampana","SUBCATEGORIA":"Subcategoria","TARGET":"Target","FOCO":"Foco","SOPORTE":"Soporte","FORMATO":"Formato","UBICACION":"Ubicacion","TIPO DE COMPRA":"TipoCompra","INICIO CAMPAÑA":"InicioCampana","FIN CAMPAÑA":"FinCampana","TAXONOMÍA":"Taxonomia","IMPRESIONES":"MetaImpresiones","CLICS":"MetaClics","CPC":"MetaCPC","CTR":"MetaCTR","CPM":"MetaCPM","FORMULARIOS":"MetaFormularios","CPL":"MetaCPL","VIEWS":"MetaViews","CPV":"MetaCPV","VALOR NETO":"MetaValorNeto"},
"RegionCampana":{"REGION":"RegionCampana","PAIS":"PaisCampana","SUCURSAL":"SucursalCampana"},
"Campana":{"IDRegionCampana":"IDRegionCampana","SUBCATEGORIA":"Subcategoria","TARGET":"Target","FOCO":"Foco"},
"Medios":{"SOPORTE":"Soporte","FORMATO":"Formato","UBICACION":"Ubicacion","TIPO DE COMPRA":"TipoCompra"},
"CamposStringVacios":{"SUBCATEGORIA":"Subcategoria","TARGET":"Target","FOCO":"Foco","SUCURSAL":"SucursalCampana","UBICACION":"Ubicacion","TIPO DE COMPRA":"TipoCompra"},
"Reemplazos":{"Ñ":"N","__":"_","LEAD-ADS":"LEADADS","CAMPANA-COVID":"CAMPANACOVID","LINK-ADS":"LINKADS","\\..":"","NUEVA-SUCURSAL-VITACURA":"NUEVASUCURSALVITACURA"}
}    
```

>Es importante considerar que los diccionarios deben responder al nombre de los campos en el archivo de Plan de Medios (.xls). Esto debido a que permite hacer la transformación del nombre del campo en el archivo al nombre del campo en la tabla de BigQuery.

### **3. Proceso de Carga**

Ya con la verificación del archivo de Plan de Medios y con la construcción del archivo de parámetros. Se deben realizar los siguientes pasos:

1. Cargar el archivo de parámetros al bucket que corresponda a la industria del cliente. (*Por ejemplo para BMW el bucket esta asociado a la industria Automotriz, por lo que debiera cargarse a **bucket-plan-medios-automotriz-proyecto-mi-dw***)

2. Cargar el archivo de Plan de Medios (.xlsx) al mismo bucket donde cargamos el archivo de parametros.

3. Verificar en la bandeja de los correos configurados para las alertas que se reciba el correo con asunto **"Proceso de Carga de Datos Leads Exitoso"**

## Configuración de proceso
### Nueva industria

Para generar el proceso de carga de Plan de Medios asociado a una nueva industria, se deben generar los siguientes elementos:

1. Se debe crear un bucket en GCP, para esto nos vamos al apartado de Cloud Storage y nos vamos a *Create Bucket*. 
   
   La configuración del bucket a nivel general es la siguiente:
   - __Nombre bucket__: Que responda a la nomenclatura ***bucket-plan-medios-industria-proyecto-mi-dw***. (Por ejemplo, si la nueva industria a incorporar es Retail se debiera llamar bucket-plan-medios-retail-proyecto-mi-dw).
   - __Location Type__: Region (us-east1)
   - __Storage Class__: Standard
   - __Access Control__: Uniform (Es el que viene por default)

2. Debemos crear la Cloud Function asociada a la industria en particular. Para esto nos vamos al apartado Cloud Functions en GCP y nos vamos a *Create Function*, debiendo configurar lo siguiente:
   
   **Apartado Configuración**

   __Basics__
   - Environment: 1st gen
   - Function name: Que siga la nomenclatura __CargaPlanMediosIndustria__ (Por ejemplo si es Retail, debiera llamarse _CargaPlanMediosRetail_)
   - __Region__: us-east1 (Lo ideal es que sea igual a la del bucket asociado a la industria)
  
    __Trigger__

    - __Trigger type__: Cloud Storage
    - __Event type__: Finalize/Create
    - __Bucket__: Debe corresponder al bucket creado previamente para la industria (bucket-plan-medios-industria-proyecto-mi-dw)

    __Runtime, build, connections and security settings__
    - __Memory allocated__: 512 MB
    - __Timeout__: 540
    - __Runtime service account__: App Engine default service account
  
    __Autoscaling__
    - __Minimum number of instances__: 0
    - __Maximum number of instances__: 3

    __Runtime environment variables__

    En esta sección se deben crear cinco variables asociadas al entorno de ejecución, estas son:
    - __CORREO1, CORREO2 y CORREO3__: Estas direcciones están asociadas a las alertas ante un éxito o falla de la carga de leads. **Siempre se debe crear la variable CORREO1**, las demás son opcionales si queremos agregar otros correos que reciban las alertas.
    - __BUCKET__: Esta variable referencia en el script el bucket generado para la industria de esta Cloud Function.
    - __PARAMETROS__: Este valor corresponde al nombre del archivo de parámetros que será cargado en el bucket, lo idea es que se pueda seguir una nomenclatura del tipo __ParametrosMediosIndustria.json__
    - __SENDGRID__: Acá se debe ingresar la KEY que entrega el servicio de Sendgrid para poder realizar el envío de correos.

3. Creadas las componentes de GCP, se debe proceder a crear las siguientes tablas en BigQuery. 
- PlanMediosIndustria
- PlanMediosIndustriaHistorico
- CampanasMediosIndustria
- CampanaMediosIndustriaHistorico
- CampanaMediosIndustriaPeriodo
- MediosIndustria
- CampanaIndustria
- RegionCampanaIndustria
- ResultadosIndustria
  
Para crear estas tablas se pueden ayudar en los DDL dentro del directorio SQL de este repositorio. La idea es que las tablas estén creadas con la nomenclatura asociada a la industria (Por ejemplo para la industria del Retail, debieramos crear tablas llamadas PlanMediosRetail y CampanaMediosRetail)

>Parte importante de este proceso, es analizar que campos se debieran agregar, mantener o eliminar de la estructura base de estas tablas dada la información entregada en los archivos de Plan de Medios de esta nueva industria.

1. Con todo esto creado, el paso final es modificar el archivo de parámetros en función de lo siguiente:
- Estructura entregada en los campos PlanMedios, CampanaMedios, RegionCampana, Campana y Medios dentro del archivo (En función de la estructura creada dentro de BigQuery en las tablas de destino de la carga)
- Cambiar a la industria correspondiente en el campo Industria.
- En el campo StringVacio agregar el mapeo de los campos string que puedan tener valores vacíos en el Plan de Medios
- Modificar en el campo Reemplazos, los cambios necesarios en la Taxonomía en función de lo detectado para la industria.

### Nuevo cliente

Para incorporar un __Nuevo Cliente__, para una industria existente o recién creada, lo que debemos hacer es: 

1. Validar que los campos asociados a la información disponible en el archivo de Plan de Medios (.xls) correspondan a los diccionarios mapeados en el archivo de parámetros.
   
2. Asegurar que cliente se encuentre creado en la tabla de Clientes de BigQuery. A continuación el SQL de creación básico.

``` SQL
INSERT INTO `proyecto-mi-dw.datawarehouse.Clientes`(IdCliente,Marca,Pais) 
VALUES ((select count(1) from `proyecto-mi-dw.datawarehouse.Clientes`)+1,'CLIENTE_NUEVO','Chile');
```
4. Validar que el nuevo cliente se encuentre en la tabla ClientesConsumos, la información referente a:
   - Base y tabla consumos GoogleCampaign
   - Base y tabla consumos GoogleCampaignBasicStats
   - Base y tabla consumos FacebookAds
   - Base y tabla consumos Máximo Rendimiento (En caso de existir esta implementación para el cliente)
  
  Acá una ejemplo asociado a la incorporación del cliente MINI a esta tabla
``` SQL
INSERT INTO `proyecto-mi-dw.datawarehouse.ClientesConsumos`(IDClienteConsumo,IDCliente,NombreCliente,TipoConsumo,BaseConsumo,TablaConsumo) 
VALUES ((select count(1) from `proyecto-mi-dw.datawarehouse.ClientesConsumos`)+1,2,'MINI','GoogleCampaign','MINI','Campaign_4595408874'),
((select count(1) from `proyecto-mi-dw.datawarehouse.ClientesConsumos`)+1,2,'MINI','GoogleCampaignBasicStats','MINI','CampaignBasicStats_4595408874'),
((select count(1) from `proyecto-mi-dw.datawarehouse.ClientesConsumos`)+1,2,'MINI','FacebookAds','MINI','facebook_AdCostData'),
((select count(1) from `proyecto-mi-dw.datawarehouse.ClientesConsumos`)+1,2,'MINI','MAXIMO','MINI','MAXIMORENDI_MINI');
```
5. Validar que el nuevo cliente se encuentre en la tabla de Ponderaciones. Si no se encuentra, se debe generar registro con el siguiente script SQL.

``` SQL
INSERT INTO `proyecto-mi-dw.datawarehouse.Ponderaciones` (Cliente,Ponderador,FechaCarga)
VALUES ('NUEVO_CLIENTE',1.2,CURRENT_TIMESTAMP());
```
6. Finalmente, debemos validar que el cliente exista en la tabla de TipoCambio de BigQuery para el año y mes actual. En caso de no existir, se debe generar de la siguiente forma

``` SQL
INSERT INTO `proyecto-mi-dw.datawarehouse.TiposCambio` (IDTipoCambio,Industria,Cliente,AnoTipoCambio,MesTipoCambio,CambioUSD,FechaActualizacion)
VALUES ((select count(1) from `proyecto-mi-dw.datawarehouse.TiposCambio`)+1,'Industria','Nuevo_Cliente',2022,5,850,'2022-05-17');
```
7. El último paso a considerar tiene relación con la creación del Saved Query asociado al cliente y su programación diaria (Scheduled Query).