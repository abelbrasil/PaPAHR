
#' preprocess_SIH: Estrutura os dados de Producao Hospitalar (SIH)
#'
#' @param cbo E a tabela retornada pela funcao `get_details` com o parametro detail_name igual a "CBO"
#' @param cid E a tabela retornada pela funcao `get_details` com o parametro detail_name igual a "CID".
#' @param raw_SIH Dados do DataSUS de Producao Hospitalar (SIH)
#' @param file_type tibble, nomes de arquivos .dbc, e algumas descricoes desses arquivos
#' @param county_id Codigo(s) do Municipio de Atendimento
#' @param procedure_details Sao os dados retornados pelo funcao `get_procedure_details`
#' @param health_establishment_id Código(s) do estabelecimento de saúde
#'
#' @return Um dataset outputSIH
#' @export
#'
preprocess_SIH <- function(cbo,
                           cid,
                           raw_SIH,
                           file_type,
                           county_id,
                           procedure_details,
                           health_establishment_id) {
  `%>%` <- dplyr::`%>%`
  outputSIH <- raw_SIH %>%
    tibble::as_tibble() %>%
    dplyr::left_join(file_type, by="file_id") %>%
    dplyr::mutate(TIPO = ifelse(file_type == "RD", "Aprovada", "Rejeitada"),
                  DT_CMPT = lubridate::ym(stringr::str_c(ANO_CMPT, MES_CMPT, sep="-")),
                  NM_MES_CMPT = stringr::str_to_title(lubridate::month(DT_CMPT, label=TRUE, abbr=FALSE)),
                  NM_MES_CMPT = stringr::str_glue("{sprintf('%02d', lubridate::month(DT_CMPT))} - {NM_MES_CMPT}"),
                  QTD_AIH = 1,
                  ANOMES_CMPT = format(DT_CMPT, "%Y%m"),
                  UF_RES = stringr::str_sub(MUNIC_RES, 1, 2),
                  UF_GESTOR = stringr::str_sub(UF_ZI, 1, 2)
    ) %>%
    check_filter(var_value=health_establishment_id, var_name="CNES") %>%
    dplyr::left_join(counties, by=c("MUNIC_RES" = "id_municipio")) %>%
    dplyr::left_join(procedure_details,
                     c("PROC_REA" = "CO_PROCEDIMENTO", "ANOMES_CMPT" = "file_version_id")) %>%
    dplyr::left_join(cid, by=c("DIAG_PRINC" = "CO_CID","ANOMES_CMPT" = "file_version_id")) %>%
    dplyr::left_join(health_establishment, by="CNES") %>%
    dplyr::mutate(NO_CID = dplyr::if_else(is.na(NO_CID), "0000-Nao informado", NO_CID),
                  dplyr::across(c(nome_estado, nome_microrregiao, nome_mesorregiao, nome_municipio),
                                ~ dplyr::case_when(UF_GESTOR == UF_RES ~ .x,
                                                   UF_GESTOR != UF_RES ~ "Outros",
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
                  `Estado de Residencia` = nome_estado)

  return(outputSIH)
}
