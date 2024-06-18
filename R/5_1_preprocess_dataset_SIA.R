
#' preprocess_SIA: Estrutura os dados de Producao Ambulatorial (SIA)
#'
#' @param cbo E a tabela retornada pela funcao `get_details` com o parametro detail_name igual a "CBO"
#' @param cid E a tabela retornada pela funcao `get_details` com o parametro detail_name igual a "CID".
#' @param raw_SIA Dados do DataSUS de Producao Ambulatorial (SIA)
#' @param county_id Código(s) do Município de Atendimento
#' @param procedure_details Sao os dados retornados pelo funcao `get_procedure_details`
#' @param publication_date_start E a data de inicio para o download dos dados, formatada no padrao AAAA-MM-DD
#' @param health_establishment_id Código(s) do estabelecimento de saúde
#'
#' @return Um dataset outputSIA
#' @export
#'
preprocess_SIA <- function(cbo,
                           cid,
                           raw_SIA,
                           county_id,
                           procedure_details,
                           publication_date_start,
                           health_establishment_id) {
  `%>%` <- dplyr::`%>%`
  outputSIA <- raw_SIA %>%
    tibble::as_tibble() %>%
    dplyr::rename(CNES = PA_CODUNI) %>%
    check_filter(var_value=health_establishment_id, var_name="CNES") %>%
    check_filter(var_value=county_id, var_name="PA_UFMUN") %>%
    dplyr::filter(lubridate::ym(PA_CMP) >= publication_date_start) %>%
    dplyr::left_join(counties, by=c("PA_MUNPCN" = "id_municipio")) %>%
    dplyr::left_join(procedure_details, c("PA_PROC_ID" = "CO_PROCEDIMENTO","PA_CMP" = "file_version_id")) %>%
    dplyr::left_join(cbo, by=c("PA_CBOCOD" = "CO_OCUPACAO","PA_CMP" = "file_version_id")) %>%
    dplyr::left_join(cid, by=c("PA_CIDPRI" = "CO_CID","PA_CMP" = "file_version_id")) %>%
    dplyr::left_join(health_establishment, by="CNES") %>%

    dplyr::mutate(DT_CMP = lubridate::ym(PA_CMP),
                  ANO_CMP = lubridate::year(DT_CMP),
                  MES_CMP = lubridate::month(DT_CMP),
                  NM_MES_CMP = stringr::str_to_title(lubridate::month(DT_CMP, label=TRUE, abbr = FALSE)),
                  NM_MES_CMP = stringr::str_glue("{sprintf('%02d', MES_CMP)} - {NM_MES_CMP}"),
                  DT_MVM = lubridate::ym(PA_MVM),
                  ANO_MVM = lubridate::year(DT_MVM),
                  MES_MVM = lubridate::month(DT_MVM),
                  NM_MES_MVM = stringr::str_to_title(lubridate::month(DT_MVM, label=TRUE, abbr = FALSE)),
                  NM_MES_MVM = stringr::str_glue("{sprintf('%02d', MES_MVM)} - {NM_MES_MVM}"),
                  TIPO_REGISTRO = dplyr::case_when(PA_DOCORIG == "C" ~ "BPA - Consolidado",
                                                   PA_DOCORIG == "I" ~ "BPA - Individualizado",
                                                   PA_DOCORIG == "P" ~ "APAC - Procedimento Principal",
                                                   PA_DOCORIG == "S" ~ "APAC - Procedimento Secundario"),
                  PA_UFUNI = stringr::str_sub(PA_UFMUN, 1, 2),
                  PA_UFPCN = stringr::str_sub(PA_MUNPCN, 1, 2),
                  NO_CID = dplyr::if_else(is.na(NO_CID), "0000-Nao informado", NO_CID),
                  dplyr::across(c(nome_estado, nome_microrregiao, nome_mesorregiao, nome_municipio),
                                ~ dplyr::case_when(PA_UFUNI == PA_UFPCN ~ .x,
                                                   PA_UFUNI != PA_UFPCN ~ "Outros",
                                                   PA_UFUNI == 99 | PA_UFPCN == 99 ~ "Nao informado"))) %>%

    dplyr::select(`Mes/Ano de Atendimento` = DT_CMP,
                  `Ano de Atendimento` = ANO_CMP,
                  `Mes de Atendimento (Numero)` = MES_CMP,
                  `Mes de Atendimento` = NM_MES_CMP,
                  `Ano/Mes de Atendimento` = PA_CMP,
                  `Ano/Mes de Processamento` = PA_MVM,
                  `Mes/Ano de Processamento` = DT_MVM,
                  `Ano de Processamento` = ANO_MVM,
                  `Mes de Processamento (Numero)` = MES_MVM,
                  `Mes de Processamento` = NM_MES_MVM,
                  `Nome da Morbidade (CID)` = NO_CID,
                  `Procedimentos realizados` = NO_PROCEDIMENTO,
                  `Grupo de Procedimentos` = NO_GRUPO,
                  `SubGrupo de Procedimentos` = NO_SUB_GRUPO,
                  `Forma de organizacao` = NO_FORMA_ORGANIZACAO,
                  `Tipo de Financiamento` = NO_FINANCIAMENTO,
                  `TipoFin/Subtipo Financiamento` = NO_SUB_FINANCIAMENTO,
                  `Profissional-CBO` = NO_OCUPACAO,
                  `Instrumento de registro` = TIPO_REGISTRO,
                  `Complexidade Procedimento` = COMPLEXIDADE,
                  `Frequencia` = PA_QTDAPR,
                  `Quantidade Apresentada` = PA_QTDPRO,
                  `Valor Aprovado` = PA_VALAPR,
                  `Valor Apresentado` = PA_VALPRO,
                  `Estabelecimento CNES` = NO_ESTABELECIMENTO,
                  `Municipio Residencia` = nome_municipio,
                  `Micro IBGE Residencia` = nome_microrregiao,
                  `Meso IBGE Residencia` = nome_mesorregiao,
                  `Estado Residencia` = nome_estado)

  return(outputSIA)
}
