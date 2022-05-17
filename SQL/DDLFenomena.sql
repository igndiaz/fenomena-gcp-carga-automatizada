CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.MediosAutomotriz` (
    IDMedio INTEGER NOT NULL,
    Soporte STRING,
    Formato STRING,
    Ubicacion STRING,
    TipoCompra STRING
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.CampanasAutomotriz` (
    IDCampana INTEGER NOT NULL,
    IDRegionCampana INTEGER NOT NULL,
    Subcategoria STRING,
    Target STRING,
    Foco STRING
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.CampanaMediosAutomotriz` (
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

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.CampanaMediosPeriodoAutomotriz` (
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

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.CampanaMediosHistoricoAutomotriz` (
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

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.ResultadosAutomotriz` (
    IDResultado INTEGER,
    Cliente STRING,
    IDCampanaMedio INTEGER,
    IDTipoCambio INTEGER,
    FuenteResultado STRING,
    HomologacionCampana	STRING,
    HomologacionCampanaOriginal	STRING,
    FechaResultado DATE,
    ResultadosImpresiones FLOAT64,
    ResultadosClics	FLOAT64,
    ResultadosFormularios INTEGER,
    ResultadosValorNeto	FLOAT64,
    ResultadosValorNetoCalculado FLOAT64	
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.Clientes` (
    IDCliente INTEGER,
    Marca STRING,
    Pais STRING,
    Industria STRING,
    Tipo STRING,
    Tamano STRING
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.PlanMediosAutomotriz` (
    IDPlan INTEGER,
    IDCliente INTEGER,
    NombrePlan STRING,
    Version	INTEGER,
    FechaCargaPlan DATETIME,
    AnoPlan	INTEGER,
    MesPlan	STRING,
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

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.PlanMediosHistoricoAutomotriz` (
    IDPlanMediosHistorico INTEGER,
    IDPlan INTEGER,
    IDCliente INTEGER,
    NombrePlan STRING,
    Version	INTEGER,
    FechaCargaPlan DATETIME,
    AnoPlan	INTEGER,
    MesPlan	STRING,
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

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.DependenciasAutomotriz` (
    IDDependencia STRING NOT NULL,
    IDCliente INTEGER NOT NULL,
    Ciudad STRING,
    TamanoDependencia STRING,
    ImportanciaRelativa	STRING,
    Direccion STRING,
    TipoDependencia	STRING,	
);

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.LeadsAutomotriz` (
    IDLead	INTEGER,
    IDCliente INTEGER,
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

CREATE TABLE IF NOT EXISTS `proyecto-mi-dw.datawarehouse.RegionCampanaAutomotriz` (
    IDRegionCampana INTEGER,
    RegionCampana STRING,	
    PaisCampana	STRING,
    SucursalCampana	STRING	
);