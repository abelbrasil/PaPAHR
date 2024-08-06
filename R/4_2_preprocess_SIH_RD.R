
#' preprocess_SIH_RD: Estrutura os dados de Producao Hospitalar (SIH)
#'
#' @param cbo E a tabela retornada pela funcao `get_details` com o parametro detail_name igual a "CBO"
#' @param cid E a tabela retornada pela funcao `get_details` com o parametro detail_name igual a "CID".
#' @param raw_SIH_RD Dados do DataSUS de Producao Hospitalar (SIH)
#' @param county_id Codigo(s) do Municipio de Atendimento
#' @param procedure_details Sao os dados retornados pelo funcao `get_procedure_details`
#' @param health_establishment_id Código(s) do estabelecimento de saúde
#'
#' @return Um dataset outputSIH_RD
#' @export
#'
preprocess_SIH_RD <- function(cbo,
                           cid,
                           raw_SIH_RD,
                           county_id,
                           procedure_details,
                           health_establishment_id) {
  `%>%` <- dplyr::`%>%`
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
                  `Hospital (CNES)` = NO_ESTABELECIMENTO,
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
                  `Mesorregiao IBGE de Resid.` = nome_mesorregiao,
                  `Microrregiao IBGE de Resid.` = nome_microrregiao,
                  `Cod do Municipio do Estabelecimento` = MUNIC_RES,
                  `Municipio do Estabelecimento` = nome_municipio,
                  `Estado de Residencia` = nome_estado)

  return(outputSIH_RD)
}
