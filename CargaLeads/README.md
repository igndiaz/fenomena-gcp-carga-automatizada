# Carga Automatizada Leads

Este repositorio contiene el código y archivo de parámetros asociado para configurar en GCP el proceso de carga automatizado de Leads.

## Protocolo de Carga
### **1. Revisión archivo Excel**

Lo primero que debemos hacer es verificar lo siguiente en el archivo de Leads a cargar:

- El archivo debe ser de extensión .xlsx (archivo de Excel)
- El nombre del archivo debe tener la estructura “Leads [Nombre Cliente] [Información Adicional].xlsx. Por ejemplo “Leads BMW acumulado diciembre.xlsx”. (Es crítico que entre leads, el nombre del cliente y la información adicional exista la separación por espacios).
- El archivo debe mantener una separación de las campañas a cargar por hojas.
- Cada hoja dentro del archivo Excel asociadas a los Leads de las campañas a cargar deben contar con los campos **Homologacion** y **Fecha**.

### **2. Construcción de archivo Parámetros**

Para la correcta ejecución del proceso de carga de Leads, se debe construir un archivo tipo JSON asociado a los parámetros de carga. En este archivo se debe definir lo siguiente:

- La industria correspondiente al cliente (*Campo Industria*)
- Diccionario de reemplazos a ejecutar en la Taxonomía (*Campo REEMPLAZOS*)
- Columnas base de las tablas Leads y Dependencias en BigQuery. (*Campos BASE y DEPENDENCIAS respectivamente*)
- Indicar cliente y el mapeo de columnas de las campañas en Excel con la tabla de Leads en BigQuery. (*Campos cliente y campañas a cargar*)
  
A continuación un archivo *json* de ejemplo:
```json
{
    "Industria" : "Automotriz",
    "BASE":["DepartamentoDistribuidor","Marca","IDDependencia","LeadSource","TemperaturaOportunidad","Correo","Rut","InteractionDetail","UtmCampaign","UtmSource","UtmMedium","UtmContent","UtmTerm","Homologacion","Fecha","Nombre","Apellido","Telefono","UTM"],
    "DEPENDENCIAS":["IDDependencia","IDCliente","Ciudad","TamanoDependencia","ImportanciaRelativa","Direccion","TipoDependencia"],
    "REEMPLAZOS":{"Ñ":"N","__":"_","LEAD-ADS":"LEADADS","CAMPANA-COVID":"CAMPANACOVID","LINK-ADS":"LINKADS","\\..":"","NUEVA-SUCURSAL-VITACURA":"NUEVASUCURSALVITACURA"},
    "BMW":{
        "BMW MAR 2022":
        {
            "DepartamentoDistribuidor": "Departamento del distribuidor",
            "Marca": "Marca",
            "IDDependencia": "Nombre del distribuidor",
            "LeadSource": "Lead Source",
            "TemperaturaOportunidad": "Temperatura de la oportunidad",
            "Nombre": "Nombre de la oportunidad",
            "Correo": "Dirección de Email",
            "Rut": "Opportunity Identification Number",
            "InteractionDetail": "InteractionDetail",
            "UtmCampaign": "utm-campaign",
            "UtmSource": "utm-source",
            "UtmMedium": "utm-medium",
            "UtmContent": "utm-content",
            "UtmTerm": "utm-term",
            "Homologacion": "homologacion",
            "Fecha": "fecha"
        }
    }
}
```
>Es importante considerar que si indicamos en el archivo una campaña que no existe en el archivo de carga de Leads, esto generará un error. Por lo que, se debe revisar antes de cada carga el archivo de parámetros y eliminar las campañas que correspondan a la carga actual.

### Incorporación de nueva campaña a archivo de parámetros

Imaginemos que al archivo de parámetros ejemplificado, se deben incorporar los resultados de leads de la campaña Body&Pain Marca. (Siendo estos entregados en una hoja aparte de ka campaña BMW MAR 2022). Para esto se deben seguir los siguientes pasos.

1. Identificar dentro de la hoja que información asociada a la estructura base de Leads (definida dentro del archivo en el apartado “BASE”) es posible extraer. En este caso vemos que la información disponible está relacionada a los campos Nombre, Apellido, Rut, Correo, Telefono, InteractionDetail, IDDependencia, UTM, Homologacion y Fecha.

2. A partir de lo identificado, comenzamos a crear la estructura asociada a Body&Pain Marca. Mapeando los campos base de la estructura de Leads (que cuenten con información dentro de la campaña) con el nombre de los campos en la hoja asociada a la campaña.

```json
       "Body&Pain Marca":
        {
           "IDDependencia":"sucursal-990",
           "Rut":"ru-440",
           "Correo":"your-email",
           "InteractionDetail": "modelo-796",
           "Homologacion": "homologacion",
           "Fecha": "fecha",
           "Nombre": "nombre-334",
           "Apellido": "apellido-335",
           "Telefono": "cel-671",
           "UTM": "dynamichidden-001"
        }
```

3. Generada la estructura se debe incorporar al archivo de parámetros, dentro de la estructura de cliente correspondiente.

```json
{
    "Industria" : "Automotriz",
    "BASE":["DepartamentoDistribuidor","Marca","IDDependencia","LeadSource","TemperaturaOportunidad","Correo","Rut","InteractionDetail","UtmCampaign","UtmSource","UtmMedium","UtmContent","UtmTerm","Homologacion","Fecha","Nombre","Apellido","Telefono","UTM"],
    "DEPENDENCIAS":["IDDependencia","IDCliente","Ciudad","TamanoDependencia","ImportanciaRelativa","Direccion","TipoDependencia"],
    "REEMPLAZOS":{"Ñ":"N","__":"_","LEAD-ADS":"LEADADS","CAMPANA-COVID":"CAMPANACOVID","LINK-ADS":"LINKADS","\\..":"","NUEVA-SUCURSAL-VITACURA":"NUEVASUCURSALVITACURA"},
    "BMW":{
        "BMW MAR 2022":
        {
            "DepartamentoDistribuidor": "Departamento del distribuidor",
            "Marca": "Marca",
            "IDDependencia": "Nombre del distribuidor",
            "LeadSource": "Lead Source",
            "TemperaturaOportunidad": "Temperatura de la oportunidad",
            "Nombre": "Nombre de la oportunidad",
            "Correo": "Dirección de Email",
            "Rut": "Opportunity Identification Number",
            "InteractionDetail": "InteractionDetail",
            "UtmCampaign": "utm-campaign",
            "UtmSource": "utm-source",
            "UtmMedium": "utm-medium",
            "UtmContent": "utm-content",
            "UtmTerm": "utm-term",
            "Homologacion": "homologacion",
            "Fecha": "fecha"
        },
       "Body&Pain Marca":
        {
           "IDDependencia":"sucursal-990",
           "Rut":"ru-440",
           "Correo":"your-email",
           "InteractionDetail": "modelo-796",
           "Homologacion": "homologacion",
           "Fecha": "fecha",
           "Nombre": "nombre-334",
           "Apellido": "apellido-335",
           "Telefono": "cel-671",
           "UTM": "dynamichidden-001"
        }

    }
}
```

### **3. Proceso de Carga**

Ya con la verificación del archivo de Leads y con la construcción del archivo de parámetros. Se deben realizar los siguientes pasos:

1. Cargar el archivo de parámetros al bucket que corresponda a la industria del cliente. (*Por ejemplo para BMW el bucket esta asociado a la industria Automotriz, por lo que debiera cargarse a **bucket-leads-automotriz-proyecto-mi-dw***)

2. Cargar el archivo de Leads (.xlsx) al mismo bucket donde cargamos el archivo de parametros.

3. Verificar en la bandeja de los correos configurados para las alertas que se reciba el correo con asunto **"Proceso de Carga de Datos Leads Exitoso"**

4. También se puede verificar con algún lead especifico del archivo, que la carga se haya realizado de forma correcta en la tabla Leads.

## Configuración de proceso
### Nueva industria

Para generar el proceso de carga de Leads asociada a una nueva industria, se deben generar los siguientes elementos:

1. Se debe crear un bucket en GCP, para esto nos vamos al apartado de Cloud Storage y nos vamos a *Create Bucket*. 
   
   La configuración del bucket a nivel general es la siguiente:
   - __Nombre bucket__: Que responda a la nomenclatura ***bucket-leads-industria-proyecto-mi-dw***. (Por ejemplo, si la nueva industria a incorporar es Retail se debiera llamar bucket-leads-retail-proyecto-mi-dw).
   - __Location Type__: Region (us-east1)
   - __Storage Class__: Standard
   - __Access Control__: Uniform (Es el que viene por default)

2. Debemos crear la Cloud Function asociada a la industria en particular. Para esto nos vamos al apartado Cloud Functions en GCP y nos vamos a *Create Function*, debiendo configurar lo siguiente:
   
   **Apartado Configuración**

   __Basics__
   - Environment: 1st gen
   - Function name: Que siga la nomenclatura __CargaLeadsIndustria__ (Por ejemplo si es Retail, debiera llamarse _CargaLeadsRetail_)
   - __Region__: us-east1 (Lo ideal es que sea igual a la del bucket asociado a la industria)
  
    __Trigger__

    - __Trigger type__: Cloud Storage
    - __Event type__: Finalize/Create
    - __Bucket__: Debe corresponder al bucket creado previamente para la industria (bucket-leads-industria-proyecto-mi-dw)

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
    - __PARAMETROS__: Este valor corresponde al nombre del archivo de parámetros que será cargado en el bucket, lo idea es que se pueda seguir una nomenclatura del tipo __ParametrosLeadsIndustria.json__
    - __SENDGRID__: Acá se debe ingresar la KEY que entrega el servicio de Sendgrid para poder realizar el envío de correos.

3. Creadas las componentes de GCP, se debe proceder a crear la tabla Leads y Dependencias en BigQuery. Para crear estas tablas se pueden ayudar en los DDL dentro del directorio SQL de este repositorio. La idea es que las tablas estén creadas con la nomenclatura asociada a la industria (Por ejemplo para la industria del Retail, debieramos crear tablas llamadas LeadsRetail y DependenciasRetail)

>Parte importante de este proceso, es analizar que campos se debieran agregar, mantener o eliminar de la estructura base de estas tablas dada la información entregada en los archivos de Leads de esta nueva industria.

4. Con todo esto creado, el paso final es modificar el archivo de parámetros en función de lo siguiente:
- Estructura entregada en los campos BASE y DEPENDENCIAS dentro del archivo (En función de la estructura creada dentro de BigQuery en las tablas de destino de la carga)
- Cambiar a la industria correspondiente en el campo Industria.
- Asociar el cliente correspondiente y generar el mapeo de campos asociadas a las nuevas campañas a cargar.
- Modificar en el campo REEMPLAZOS, los cambios necesarios en la Taxonomía en función de lo detectado para la industria.

### Nuevo cliente

Para incorporar un __Nuevo Cliente__, para una industria existente o recién creada, lo que debemos hacer es: 

1. Modificar el cliente asociado a la  definición de las campañas en el archivo de parámetros. (Identificado en el siguiente ejemplo como NUEVO_CLIENTE)

```json
{ (Para ejemplificar omitimos parte del archivo json)...
    "REEMPLAZOS":{"Ñ":"N","__":"_","LEAD-ADS":"LEADADS","CAMPANA-COVID":"CAMPANACOVID","LINK-ADS":"LINKADS","\\..":"","NUEVA-SUCURSAL-VITACURA":"NUEVASUCURSALVITACURA"},
    "NUEVO_CLIENTE":{
        "NUEVO_CLIENTE MAR 2022":
        {
            "DepartamentoDistribuidor": "Departamento del distribuidor",
            "Marca": "Marca",
            "IDDependencia": "Nombre del distribuidor",
            "LeadSource": "Lead Source",
            "TemperaturaOportunidad": "Temperatura de la oportunidad",
            "Nombre": "Nombre de la oportunidad",
            "Correo": "Dirección de Email",
            "Rut": "Opportunity Identification Number",
            "InteractionDetail": "InteractionDetail",
            "UtmCampaign": "utm-campaign",
            "UtmSource": "utm-source",
            "UtmMedium": "utm-medium",
            "UtmContent": "utm-content",
            "UtmTerm": "utm-term",
            "Homologacion": "homologacion",
            "Fecha": "fecha"
        }
    }
}
```
2. Mapear las campañas y los campos asociados a la información disponible en el archivo de Leads (.xls) del nuevo cliente.
   
3. Asegurar que cliente se encuentre creado en la tabla de Clientes de BigQuery. A continuación el SQL de creación básico.

``` SQL
INSERT INTO `proyecto-mi-dw.datawarehouse.Clientes`(IdCliente,Marca,Pais,Industria) 
VALUES ((select count(1) from `proyecto-mi-dw.datawarehouse.Clientes`)+1,'NUEVO_CLIENTE','Chile','Industria');
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