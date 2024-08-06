
#' preprocess_SIA: Estrutura os dados de Producao Ambulatorial (SIA)
#'
#' @param cbo E a tabela retornada pela funcao `get_details` com o parametro detail_name igual a "CBO"
#' @param cid E a tabela retornada pela funcao `get_details` com o parametro detail_name igual a "CID".
#' @param raw_SIA Dados do DataSUS de Producao Ambulatorial (SIA)
#' @param county_id Código(s) do Município de Atendimento
#' @param procedure_details Sao os dados retornados pelo funcao `get_procedure_details`
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
                           health_establishment_id) {
  `%>%` <- dplyr::`%>%`
  municipios = counties %>%
    dplyr::select(id_municipio, nome_municipio) %>%
    dplyr::rename(municipio_estabelecimento = nome_municipio)

  outputSIA <- raw_SIA %>%
    tibble::as_tibble() %>%
    dplyr::rename(CNES = PA_CODUNI) %>%
    check_filter(var_value = county_id, var_name = "PA_UFMUN") %>%
    check_filter(var_value = health_establishment_id, var_name = "CNES")%>%
    dplyr::left_join(counties, by=c("PA_MUNPCN" = "id_municipio")) %>%
    dplyr::left_join(municipios, by = c("PA_UFMUN" = "id_municipio")) %>%
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
                                                   PA_UFUNI != PA_UFPCN ~ NA,
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
                  `Nome da Morbidade (CID)` = NO_CID, #cid
                  `Procedimentos realizados` = NO_PROCEDIMENTO, #procedure_details
                  `Grupo de Procedimentos` = NO_GRUPO,#procedure_details
                  `SubGrupo de Procedimentos` = NO_SUB_GRUPO,#procedure_details
                  `Forma de organizacao` = NO_FORMA_ORGANIZACAO,#procedure_details
                  `Tipo de Financiamento` = NO_FINANCIAMENTO, #procedure_details
                  `TipoFin/Subtipo Financiamento` = NO_SUB_FINANCIAMENTO, #procedure_details
                  `Profissional-CBO` = NO_OCUPACAO,#cbo
                  `Instrumento de registro` = TIPO_REGISTRO,
                  `Complexidade Procedimento` = COMPLEXIDADE,#procedure_details
                  `Frequencia` = PA_QTDAPR,
                  `Quantidade Apresentada` = PA_QTDPRO,
                  `Valor Aprovado` = PA_VALAPR,
                  `Valor Apresentado` = PA_VALPRO,
                  `Municipio Residencia` = nome_municipio, #counties
                  `Micro IBGE Residencia` = nome_microrregiao, #counties
                  `Meso IBGE Residencia` = nome_mesorregiao, #counties
                  `Estabelecimento CNES` = NO_ESTABELECIMENTO,
                  `Cod do Municipio do Estabelecimento` = PA_UFMUN,
                  `Municipio do Estabelecimento` = municipio_estabelecimento,
                  `Estado Residencia` = nome_estado)  #counties

  return(outputSIA)
}
