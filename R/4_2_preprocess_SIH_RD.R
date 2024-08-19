
#' Returns Reduced Data (RD) by SIH-AIH in a structured and filtered way.
#'
#' @description Estrutura e filtra os dados da Autorização de Internação Hospitalar (AIH) Reduzida (RD) e combina as informações do CNES, SIGTAP e da base de dados `counties`.
#'
#' @param cbo É a tabela retornada pela função `get_details` quando o parâmetro `detail_name='CBO'`
#' @param cid É a tabela retornada pela função `get_details` quando o parâmetro `detail_name='CID'`
#' @param raw_SIH_RD Dados de Autorização de Internação Hospitalar (AIH) Reduzida (RD) do Sistema de Informação Hospitalar (SIH)
#' @param county_id string or a vector of strings. Código do Município de Atendimento.
#' @param procedure_details São os dados retornados pelo funcão `get_procedure_details`
#' @param health_establishment_id string or a vector of strings. Código do estabelecimento de saúde.
#'
#' @return Retorna a tabela da Autorização de Internação Hospitalar (AIH) Reduzida (RD) já filtrada e tratada.
#'
#' @export
preprocess_SIH_RD <-
  function(cbo,
           cid,
           raw_SIH_RD,
           county_id,
           procedure_details,
           health_establishment_id){
  `%>%` <- dplyr::`%>%`

  municipios = counties %>%
    dplyr::select(id_municipio, nome_municipio) %>%
    dplyr::rename(municipio_estabelecimento = nome_municipio)

  outputSIH_RD <- raw_SIH_RD %>%
    tibble::as_tibble() %>%
    check_filter(var_value = county_id, var_name = "MUNIC_MOV") %>%
    check_filter(var_value=health_establishment_id, var_name="CNES") %>%
    dplyr::mutate(TIPO = "Aprovada",
                  DT_CMPT = lubridate::ym(stringr::str_c(ANO_CMPT, MES_CMPT, sep="-")),
                  NM_MES_CMPT = stringr::str_to_title(lubridate::month(DT_CMPT, label=TRUE, abbr=FALSE)),
                  NM_MES_CMPT = stringr::str_glue("{sprintf('%02d', lubridate::month(DT_CMPT))} - {NM_MES_CMPT}"),
                  QTD_AIH = 1,
                  ANOMES_CMPT = format(DT_CMPT, "%Y%m"),
                  UF_RES = stringr::str_sub(MUNIC_RES, 1, 2),
                  UF_GESTOR = stringr::str_sub(UF_ZI, 1, 2)
    ) %>%
    dplyr::left_join(counties, by=c("MUNIC_RES" = "id_municipio")) %>%
    dplyr::left_join(municipios, by = c("MUNIC_MOV" = "id_municipio")) %>%
    dplyr::left_join(procedure_details,
                     c("PROC_REA" = "CO_PROCEDIMENTO", "ANOMES_CMPT" = "file_version_id")) %>%
    dplyr::left_join(cid, by=c("DIAG_PRINC" = "CO_CID","ANOMES_CMPT" = "file_version_id")) %>%
    dplyr::left_join(health_establishment, by="CNES") %>%
    dplyr::mutate(NO_CID = dplyr::if_else(is.na(NO_CID), "0000-Nao informado", NO_CID),
                  dplyr::across(c(nome_estado, nome_microrregiao, nome_mesorregiao, nome_municipio),
                                ~ dplyr::case_when(UF_GESTOR == UF_RES ~ .x,
                                                   UF_GESTOR != UF_RES ~ NA,
                                                   UF_GESTOR == 99 | UF_RES == 99 ~ "Nao informado"
                                ))) %>%

    dplyr::select(`Situacao da AIH` = TIPO,
                  `Ano processamento` = ANO_CMPT,
                  `Mes processamento (Numero)` = MES_CMPT,
                  `Mes processamento` = NM_MES_CMPT,
                  `Mes/Ano processamento` = DT_CMPT,
                  `Ano/Mes processamento` = ANOMES_CMPT,
                  `Procedimentos realizados` = NO_PROCEDIMENTO,
                  `Grupo de Procedimentos` = NO_GRUPO,
                  `SubGrupo de Procedimentos` = NO_SUB_GRUPO,
                  `Forma de organizacao` = NO_FORMA_ORGANIZACAO,
                  `Complexidade do Procedimento` = COMPLEXIDADE,
                  `Financiamento` = NO_FINANCIAMENTO,
                  `SubTp FAEC` = NO_SUB_FINANCIAMENTO,
                  `Nome da Mobirdade (CID)` = NO_CID,
                  `Valor Total` = VAL_TOT,
                  `Frequencia` = QTD_AIH,
                  `Sequencial` = N_AIH,
                  `Municipio de Residencia` = nome_municipio,
                  `Estado de Residencia` = nome_estado,
                  `Mesorregiao IBGE de Resid.` = nome_mesorregiao,
                  `Microrregiao IBGE de Resid.` = nome_microrregiao,
                  `Hospital (CNES)` = NO_ESTABELECIMENTO,
                  `Cod do Municipio do Estabelecimento` = MUNIC_MOV,
                  `Municipio do Estabelecimento` = municipio_estabelecimento)

  return(outputSIH_RD)
}
