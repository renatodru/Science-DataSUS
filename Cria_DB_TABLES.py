import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'DATASUS'
TABLES = {}
TABLES['RDPR'] = (
"  CREATE TABLE `RDPR` ("
" `UF_ZI` varchar(6) NOT NULL,"
" `ANO_CMPT` varchar(4) NOT NULL,"
" `MÊS_CMPT` varchar(2) NOT NULL,"
" `ESPEC` varchar(2) NOT NULL,"
" `CGC_HOSP` varchar(14) NOT NULL,"
" `N_AIH` varchar(13) NOT NULL,"
" `IDENT` varchar(1) NOT NULL,"
" `CEP` varchar(8) NOT NULL,"
" `MUNIC_RES` varchar(6) NOT NULL,"
" `NASC` varchar(8) NOT NULL,"
" `SEXO` varchar(1) NOT NULL,"
" `UTI_MES_IN` int NULL,"
" `UTI_MES_AN` int NULL,"
" `UTI_MES_AL` int NULL,"
" `UTI_MES_TO` int NOT NULL,"
" `MARCA_UTI` varchar(2) NOT NULL,"
" `UTI_INT_IN` int NULL,"
" `UTI_INT_AN` int NULL,"
" `UTI_INT_AL` int NULL,"
" `UTI_INT_TO` int NOT NULL,"
" `DIAR_ACOM` int NOT NULL,"
" `QT_DIARIAS` int NOT NULL,"
" `PROC_SOLIC` varchar(10) NOT NULL,"
" `PROC_REA` varchar(10) NOT NULL,"
" `VAL_SH` float NOT NULL,"
" `VAL_SP` float NOT NULL,"
" `VAL_SADT` float NULL,"
" `VAL_RN` float NULL,"
" `VAL_ACOMP` float NULL,"
" `VAL_ORTP` float NULL,"
" `VAL_SANGUE` float NULL,"
" `VAL_SADTSR` float NULL,"
" `VAL_TRANSP` float NULL,"
" `VAL_OBSANG` float NULL,"
" `VAL_PED1AC` float NULL,"
" `VAL_TOT` float NOT NULL,"
" `VAL_UTI` float NOT NULL,"
" `US_TOT` float NOT NULL,"
" `DI_INTER` varchar(8) NOT NULL,"
" `DT_SAIDA` varchar(8) NOT NULL,"
" `DIAG_PRINC` varchar(4) NOT NULL,"
" `DIAG_SECUN` varchar(4) NOT NULL,"
" `COBRANCA` varchar(2) NOT NULL,"
" `NATUREZA` varchar(2) NOT NULL,"
" `NAT_JUR` varchar(4) NOT NULL,"
" `DESTAO` varchar(1) NOT NULL,"
" `RUBRICA` int NULL,"
" `IND_VDRL` varchar(1) NOT NULL,"
" `MUNIC_MOV` varchar(6) NOT NULL,"
" `COD_IDADE` varchar(1) NOT NULL,"
" `IDADE` int NOT NULL,"
" `DIAS_PERM` int NOT NULL,"
" `MORTE` int NOT NULL,"
" `NACIONAL` varchar(2) NOT NULL,"
" `NUM_PROC` varchar(4) NULL,"
" `CAR_INT` varchar(2) NOT NULL,"
" `TOT_PT_SP` int NULL,"
" `CPF_AUT` varchar(11) NULL,"
" `HOMONIMO` varchar(1) NOT NULL,"
" `NUM_FILHOS` int NOT NULL,"
" `INSTRU` varchar(1) NOT NULL,"
" `CID_NOTIF` varchar(4) NOT NULL,"
" `CONTRACEP1` varchar(2) NOT NULL,"
" `CONTRACEP2` varchar(2) NOT NULL,"
" `GESTRISCO` varchar(1) NOT NULL,"
" `INSC_PN` varchar(12) NOT NULL,"
" `SEQ_AIH5` varchar(3) NOT NULL,"
" `CBOR` varchar(3) NOT NULL,"
" `CNAER` varchar(3) NOT NULL,"
" `VINCPREV` varchar(1) NOT NULL,"
" `GESTOR_COD` varchar(3) NOT NULL,"
" `GESTOR_TP` varchar(1) NOT NULL,"
" `GESTOR_CPF` varchar(11) NOT NULL,"
" `GESTOR_DT` varchar(8) NOT NULL,"
" `CNES` varchar(7) NOT NULL,"
" `CNPJ_MANT` varchar(14) NOT NULL,"
" `INFEHOSP` varchar(1) NOT NULL,"
" `CID_ASSO` varchar(4) NOT NULL,"
" `CID_MORTE` varchar(4) NOT NULL,"
" `COMPLEX` varchar(2) NOT NULL,"
" `FINANC` varchar(2) NOT NULL,"
" `FAEC_TP` varchar(6) NOT NULL,"
" `REGCT` varchar(4) NOT NULL,"
" `RACA_COR` varchar(4) NOT NULL,"
" `ETNIA` varchar(4) NOT NULL,"
" `SEQUENCIA` int NOT NULL,"
" `REMESSA` varchar(21) NOT NULL,"
" `AUD_JUST` varchar(50) NOT NULL,"
" `SIS_JUST` varchar(50) NOT NULL,"
" `VAL_SH_FED` int NOT NULL,"
" `VAL_SP_FED` int NOT NULL,"
" `VAL_SH_GES` int NOT NULL,"
" `VAL_SP_GES` int NOT NULL,"
" `VAL_UCI` int NOT NULL,"
" `MARCA_UCI` varchar(2) NOT NULL,"
" `DIAGSEC1` varchar(4) NOT NULL,"
" `DIAGSEC2` varchar(4) NOT NULL,"
" `DIAGSEC3` varchar(4) NOT NULL,"
" `DIAGSEC4` varchar(4) NOT NULL,"
" `DIAGSEC5` varchar(4) NOT NULL,"
" `DIAGSEC6` varchar(4) NOT NULL,"
" `DIAGSEC7` varchar(4) NOT NULL,"
" `DIAGSEC8` varchar(4) NOT NULL,"
" `DIAGSEC9` varchar(4) NOT NULL,"
" `TPDISEC1` varchar(1) NOT NULL,"
" `TPDISEC2` varchar(1) NOT NULL,"
" `TPDISEC3` varchar(1) NOT NULL,"
" `TPDISEC4` varchar(1) NOT NULL,"
" `TPDISEC5` varchar(1) NOT NULL,"
" `TPDISEC6` varchar(1) NOT NULL,"
" `TPDISEC7` varchar(1) NOT NULL,"
" `TPDISEC8` varchar(1) NOT NULL,"
" `TPDISEC9` varchar(1) NOT NULL"
")")


TABLES['SPPR'] = (
"  CREATE TABLE `SPPR` ("
" `SP_GESTOR` varchar(6) NOT NULL,"
" `SP_UF` varchar(2) NOT NULL,"
" `SP_AA` varchar(4) NOT NULL,"
" `SP_MM` varchar(2) NOT NULL,"
" `SP_CNES` varchar(7) NOT NULL,"
" `SP_NAIH` varchar(13) NOT NULL,"
" `SP_PROCREA` varchar(10) NOT NULL,"
" `SP_DTINTER` varchar(8) NOT NULL,"
" `SP_DTSAIDA` varchar(8) NOT NULL,"
" `SP_NUM_PR` varchar(8) NULL,"
" `SP_TIPO` varchar(2) NULL,"
" `SP_CPFCGC` varchar(14) NOT NULL,"
" `SP_ATOPROF` varchar(10) NOT NULL,"
" `SP_TP_ATO` varchar(2) NOT NULL,"
" `SP_QTD_ATO` int NOT NULL,"
" `SP_PTSP` varchar(6) NOT NULL,"
" `SP_NF` varchar(8) NOT NULL,"
" `SP_VALATO` float NOT NULL,"
" `SP_M_HOSP` varchar(6) NOT NULL,"
" `SP_M_PAC` varchar(6) NOT NULL,"
" `SP_DES_HOS` varchar(1) NOT NULL,"
" `SP_DES_PAC` varchar(1) NOT NULL,"
" `SP_COMPLEX` varchar(2) NOT NULL,"
" `SP_FINANC` varchar(2) NOT NULL,"
" `SP_CO_FAEC` varchar(6) NOT NULL,"
" `SP_PF_CBO` varchar(6) NOT NULL,"
" `SP_PF_DOC` varchar(15) NOT NULL,"
" `SP_PJ_DOC` varchar(14) NOT NULL,"
" `IN_TP_VAL` varchar(1) NOT NULL,"
" `SEQUENCIA` varchar(9) NOT NULL,"
" `REMESSA` varchar(21) NOT NULL,"
" `SERV_CLA` varchar(6) NOT NULL,"
" `SP_CIDPRI` varchar(4) NOT NULL,"
" `SP_CIDSEC` varchar(4) NOT NULL,"
" `SP_QT_PROC` int NOT NULL,"
" `SP_U_AIH` varchar(1) NOT NULL"
")")

TABLES['PAPR'] = (
"  CREATE TABLE `PAPR` ("
" `PA_CODUNI` varchar(7) NOT NULL,"
" `PA_GESTAO` varchar(6) NOT NULL,"
" `PA_CONDIC` varchar(2) NOT NULL,"
" `PA_UFMUN` varchar(6) NOT NULL,"
" `PA_REGCT` varchar(4) NOT NULL,"
" `PA_INCOUT` varchar(4) NOT NULL,"
" `PA_INCURG` varchar(4) NOT NULL,"
" `PA_TPUPS` varchar(2) NOT NULL,"
" `PA_TIPPRE` varchar(2) NOT NULL,"
" `PA_MN_IND` varchar(1) NOT NULL,"
" `PA_CNPJCPF` varchar(14) NOT NULL,"
" `PA_CNPJMNT` varchar(14) NOT NULL,"
" `PA_CNPJ_CC` varchar(14) NOT NULL,"
" `PA_MVM` varchar(6) NOT NULL,"
" `PA_CMP` varchar(6) NOT NULL,"
" `PA_PROC_ID` varchar(10) NOT NULL,"
" `PA_TPFIN` varchar(2) NOT NULL,"
" `PA_SUBFIN` varchar(4) NOT NULL,"
" `PA_NIVCPL` varchar(1) NOT NULL,"
" `PA_DOCORIG` varchar(1) NOT NULL,"
" `PA_AUTORIZ` varchar(13) NOT NULL,"
" `PA_CNSMED` varchar(15) NOT NULL,"
" `PA_CBOCOD` varchar(6) NOT NULL,"
" `PA_MOTSAI` varchar(2) NOT NULL,"
" `PA_OBITO` varchar(1) NOT NULL,"
" `PA_ENCERR` varchar(1) NOT NULL,"
" `PA_PERMAN` varchar(1) NOT NULL,"
" `PA_ALTA` varchar(1) NOT NULL,"
" `PA_TRANSF` varchar(1) NOT NULL,"
" `PA_CIDPRI` varchar(4) NOT NULL,"
" `PA_CIDSEC` varchar(4) NOT NULL,"
" `PA_CIDCAS` varchar(4) NOT NULL,"
" `PA_CATEND` varchar(2) NOT NULL,"
" `PA_IDADE` varchar(3) NOT NULL,"
" `IDADEMIN` varchar(3) NOT NULL,"
" `IDADEMAX` varchar(3) NOT NULL,"
" `PA_FLIDADE` varchar(1) NOT NULL,"
" `PA_SEXO` varchar(1) NOT NULL,"
" `PA_RACACOR` varchar(2) NOT NULL,"
" `PA_MUNPCN` varchar(6) NOT NULL,"
" `PA_QTDPRO` int NOT NULL,"
" `PA_QTDAPR` int NOT NULL,"
" `PA_VALPRO` float NOT NULL,"
" `PA_VALAPR` float NOT NULL,"
" `PA_UFDIF` varchar(1) NOT NULL,"
" `PA_MNDIF` varchar(1) NOT NULL,"
" `PA_DIF_VAL` float NOT NULL,"
" `NU_VPA_TOT` float NOT NULL,"
" `NU_PA_TOT` float NOT NULL,"
" `PA_INDICA` varchar(1) NOT NULL,"
" `PA_CODOCO` varchar(1) NOT NULL,"
" `PA_FLQT` varchar(1) NOT NULL,"
" `PA_FLER` varchar(1) NOT NULL,"
" `PA_ETNIA` varchar(4) NOT NULL,"
" `PA_VL_CF` float NOT NULL,"
" `PA_VL_CL` float NOT NULL,"
" `PA_VL_INC` float NOT NULL,"
" `PA_SRC_C` varchar(6) NOT NULL,"
" `PA_INE` varchar(10) NOT NULL,"
" `PA_NAT_JUR` varchar(4) NOT NULL"
")")


config = {'user':'root','password':'root','host':'127.0.0.1','port':'3306','raise_on_warnings':True}

try:cnx = mysql.connector.connect(**config)#user='root',password='root',host='127.0.0.1',port='3306',raise_on_warnings=True
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:print("Database does not exist")
    else:print(err)
else:print("Conectado em {}:{}, usuario{}".format(config['host'],config['port'],config['user']))

cursor = cnx.cursor()


def create_database(cursor):
    try:cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'UTF8MB4'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)
        
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:print("already exists.")
        else:print(err.msg)
    else:print("OK")

cursor.close()
cnx.close()
