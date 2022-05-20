--Plan de Medios
CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.MediosIndustria` (
    IDMedio INTEGER NOT NULL,
    Soporte STRING,
    Formato STRING,
    Ubicacion STRING,
    TipoCompra STRING
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.CampanasIndustria` (
    IDCampana INTEGER NOT NULL,
    IDRegionCampana INTEGER NOT NULL,
    Subcategoria STRING,
    Target STRING,
    Foco STRING
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.CampanaMediosIndustria` (
    IDCampanaMedio INTEGER NOT NULL,
    IDPlan INTEGER NOT NULL,
    IDMedio INTEGER NOT NULL,
    IDCampana INTEGER NOT NULL,
    InicioCampana DATE NOT NULL,
    FinCampana DATE NOT NULL,
    FechaMeta DATE NOT NULL,
    Version INTEGER NOT NULL,
    FechaCargaPlan DATETIME NOT NULL,
    Taxonomia STRING NOT NULL,
    MetaImpresiones FLOAT64,
    MetaCPM FLOAT64,
    MetaClics FLOAT64,
    MetaCPC FLOAT64,
    MetaCTR FLOAT64, 
    MetaFormularios FLOAT64,
    MetaCPL FLOAT64,
    MetaViews FLOAT64,
    MetaCPV FLOAT64,
    MetaValorNeto FLOAT64
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.CampanaMediosPeriodoIndustria` (
    IDCampanaMedio INTEGER NOT NULL,
    IDPlan INTEGER NOT NULL,
    IDMedio INTEGER NOT NULL,
    IDCampana INTEGER NOT NULL,
    InicioCampana DATE NOT NULL,
    FinCampana DATE NOT NULL,
    Version INTEGER NOT NULL,
    FechaCargaPlan DATETIME NOT NULL,
    Taxonomia STRING NOT NULL,
    MetaImpresiones FLOAT64,
    MetaCPM FLOAT64,
    MetaClics FLOAT64,
    MetaCPC FLOAT64,
    MetaCTR FLOAT64, 
    MetaFormularios FLOAT64,
    MetaCPL FLOAT64,
    MetaViews FLOAT64,
    MetaCPV FLOAT64,
    MetaValorNeto FLOAT64
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.CampanaMediosHistoricoIndustria` (
    IDCampanaMedioHistorico INTEGER NOT NULL,
    IDCampanaMedio INTEGER NOT NULL,
    IDPlan INTEGER NOT NULL,
    IDMedio INTEGER NOT NULL,
    IDCampana INTEGER NOT NULL,
    InicioCampana DATE NOT NULL,
    FinCampana DATE NOT NULL,
    FechaMeta DATE NOT NULL,
    Version INTEGER NOT NULL,
    FechaCargaPlan DATETIME NOT NULL,
    Taxonomia STRING NOT NULL,
    MetaImpresiones FLOAT64,
    MetaCPM FLOAT64,
    MetaClics FLOAT64,
    MetaCPC FLOAT64,
    MetaCTR FLOAT64, 
    MetaFormularios FLOAT64,
    MetaCPL FLOAT64,
    MetaViews FLOAT64,
    MetaCPV FLOAT64,
    MetaValorNeto FLOAT64
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.ResultadosIndustria` (
    IDResultado INTEGER  NOT NULL,
    Cliente STRING  NOT NULL,
    IDCampanaMedio INTEGER  NOT NULL,
    IDTipoCambio INTEGER  NOT NULL,
    FuenteResultado STRING  NOT NULL,
    HomologacionCampana	STRING  NOT NULL,
    HomologacionCampanaOriginal	STRING  NOT NULL,
    FechaResultado DATE  NOT NULL,
    ResultadosImpresiones FLOAT64,
    ResultadosClics	FLOAT64,
    ResultadosFormularios INTEGER,
    ResultadosValorNeto	FLOAT64,
    ResultadosValorNetoCalculado FLOAT64	
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.PlanMediosIndustria` (
    IDPlan INTEGER NOT NULL,
    IDCliente INTEGER NOT NULL,
    NombrePlan STRING NOT NULL,
    Version	INTEGER NOT NULL,
    FechaCargaPlan DATETIME NOT NULL,
    AnoPlan	INTEGER NOT NULL,
    MesPlan	STRING NOT NULL,
    MetaPlanImpresiones FLOAT64,
    MetaPlanCPM	FLOAT64,
    MetaPlanClics FLOAT64,
    MetaPlanCPC	FLOAT64,
    MetaPlanCTR	FLOAT64,
    MetaPlanFormularios	FLOAT64,
    MetaPlanCPL	FLOAT64,
    MetaPlanViews FLOAT64,
    MetaPlanCPV	FLOAT64,
    MetaPlanValorNeto FLOAT64	
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.PlanMediosHistoricoIndustria` (
    IDPlanMediosHistorico INTEGER NOT NULL,
    IDPlan INTEGER NOT NULL,
    IDCliente INTEGER NOT NULL,
    NombrePlan STRING NOT NULL,
    Version	INTEGER NOT NULL,
    FechaCargaPlan DATETIME NOT NULL,
    AnoPlan	INTEGER NOT NULL,
    MesPlan	STRING NOT NULL,
    MetaPlanImpresiones FLOAT64,
    MetaPlanCPM	FLOAT64,
    MetaPlanClics FLOAT64,
    MetaPlanCPC	FLOAT64,
    MetaPlanCTR	FLOAT64,
    MetaPlanFormularios	FLOAT64,
    MetaPlanCPL	FLOAT64,
    MetaPlanViews FLOAT64,
    MetaPlanCPV	FLOAT64,
    MetaPlanValorNeto FLOAT64	
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.RegionCampanaIndustria` (
    IDRegionCampana INTEGER NOT NULL,
    RegionCampana STRING NOT NULL,	
    PaisCampana	STRING NOT NULL,
    SucursalCampana	STRING	
);

--Leads
CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.DependenciasIndustria` (
    IDDependencia STRING NOT NULL,
    IDCliente INTEGER NOT NULL,
    Ciudad STRING,
    TamanoDependencia STRING,
    ImportanciaRelativa	STRING,
    Direccion STRING,
    TipoDependencia	STRING,	
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.LeadsIndustria` (
    IDLead	INTEGER NOT NULL,
    IDCliente INTEGER NOT NULL,
    IDDependencia STRING,
    IDCampanaMedio INTEGER,
    Rut	STRING,
    Correo STRING,
    DepartamentoDistribuidor STRING,
    Marca STRING,
    LeadSource STRING,
    TemperaturaOportunidad STRING,
    InteractionDetail STRING,
    UtmCampaign	STRING,
    UtmSource STRING,
    UtmMedium STRING,
    UtmContent STRING,
    UtmTerm	STRING,
    Homologacion STRING,
    Fecha DATE,
    Nombre STRING,
    Apellido STRING,
    Telefono STRING,
    UTM	STRING
);

--Tablas carga manual

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.Clientes` (
    IDCliente INTEGER  NOT NULL,
    Marca STRING  NOT NULL,
    Pais STRING  NOT NULL,
    Industria STRING NOT NULL,
    Tipo STRING,
    Tamano STRING
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.ClientesConsumos` (
    IDClienteConsumos INTEGER NOT NULL,
    IDCliente INTEGER NOT NULL,	
    NombreCliente STRING NOT NULL,
    TipoConsumo	STRING NOT NULL,
    BaseConsumo	STRING NOT NULL,
    TablaConsumo STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.TiposCambio` (
    IDTipoCambio INTEGER NOT NULL,
    Industria STRING NOT NULL,	
    Cliente STRING NOT NULL,
    AnoTipoCambio INTEGER NOT NULL,
    MesTipoCambio INTEGER NOT NULL,
    CambioUSD INTEGER NOT NULL,
    FechaActualizacion DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.Ponderaciones` (
    Cliente STRING NOT NULL,
    Ponderador FLOAT64 NOT NULL,	
    FechaCarga TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.PonderacionesHistoricas` (
    IDPonderacionesHist INTEGER NOT NULL,
    Cliente STRING NOT NULL,
    Ponderador FLOAT64 NOT NULL,	
    FechaCarga TIMESTAMP NOT NULL
);