
#' Returns data from Professional Services (SP-SIH) in a structured and filtered form.
#'
#' @description Estrutura e filtra os dados dos Serviços Profissionais (SP) e combina as informações do CNES, SIGTAP e da base de dados `counties`.
#'
#' @param cbo É a tabela retornada pela função `get_details` quando o parâmetro `detail_name='CBO'`
#' @param cid É a tabela retornada pela função `get_details` quando o parâmetro `detail_name='CID'`
#' @param raw_SIH_SP Dados dos Serviços Profissionais (SP) do Sistema de Informação Hospitalar (SIH)
#' @param county_id string ou vetor de strings. Código do Município de Atendimento.
#' @param procedure_details São os dados retornados pelo função `get_procedure_details`
#' @param health_establishment_id string ou vetor de strings. Código do estabelecimento de saúde.
#'
#' @return Retorna a tabela dos Serviços Profissionais (SP)  já filtrada e tratada.
#'
#' @export
preprocess_SIH_SP <-
  function(cbo,
           cid,
           raw_SIH_SP,
           county_id,
           procedure_details,
           health_establishment_id){
  `%>%` <- dplyr::`%>%`

  municipios = counties %>%
    dplyr::select(id_municipio, nome_municipio, nome_regiao) %>%
    dplyr::rename(municipio_estabelecimento = nome_municipio,
                  nome_regiao_estabelecimento = nome_regiao)

  cols_to_convert = c(
    "Quantidade Aprovada",
    "Valor Aprovado",
    "Quantidade de Ato",
    "Frequencia"
  )

  outputSIH_SP <- raw_SIH_SP %>%
    tibble::as_tibble() %>%
    dplyr::rename(CNES = SP_CNES) %>%
    check_filter(var_value = county_id, var_name = "SP_M_HOSP") %>%
    check_filter(var_value=health_establishment_id, var_name="CNES") %>%
    dplyr::mutate(ANOMES_MVM = stringr::str_c(SP_AA, SP_MM),
                  MESANO_MVM = stringr::str_c(SP_MM, SP_AA, sep="-"),
                  DT_MVM = lubridate::ym(stringr::str_c(SP_AA, SP_MM, sep="-")),
                  NM_MES_MVM = stringr::str_to_title(lubridate::month(DT_MVM, label=TRUE, abbr=FALSE)),
                  NM_MES_MVM = stringr::str_glue("{sprintf('%02d', lubridate::month(DT_MVM))} - {NM_MES_MVM}"),
                  DT_INTER = lubridate::ymd(SP_DTINTER),
                  ANO_INT = stringr::str_sub(SP_DTINTER, 1, 4),
                  MES_INT = stringr::str_sub(SP_DTINTER, 5, 6),
                  ANOMES_INT = stringr::str_c(ANO_INT, MES_INT),
                  MESANO_INT = stringr::str_c(MES_INT, ANO_INT, sep="-")) %>%
    dplyr::left_join(counties, by=c("SP_M_PAC" = "id_municipio")) %>%
    dplyr::left_join(municipios, by = c("SP_M_HOSP" = "id_municipio")) %>%
    dplyr::left_join(procedure_details,
                     c("SP_PROCREA" = "CO_PROCEDIMENTO", "ANOMES_MVM" = "file_version_id")) %>%
    dplyr::left_join(cbo, by=c("SP_PF_CBO" = "CO_OCUPACAO", "ANOMES_MVM" = "file_version_id")) %>%
    dplyr::left_join(health_establishment, by="CNES") %>%
    dplyr::left_join(cid, by=c("SP_CIDPRI" = "CO_CID", "ANOMES_MVM" = "file_version_id")) %>%
    dplyr::mutate(IN_TP_VAL = dplyr::case_when(IN_TP_VAL == 1 ~ "Servico Hospitalar",
                                               IN_TP_VAL == 2 ~ "Servico Profissional"),
                  NO_OCUPACAO = stringr::str_c(SP_PF_CBO, NO_OCUPACAO, sep="-"),
                  NO_CID = dplyr::if_else(is.na(NO_CID), "0000-Nao informado", NO_CID))%>%

    dplyr::select(`Data Internacao` = DT_INTER,
                  `Mes/Ano Internacao` = MESANO_INT,
                  `Ano/Mes Internacao` = ANOMES_INT,
                  `Ano Internacao` = ANO_INT,
                  `Mes Internacao` = MES_INT,
                  `Mes/Ano Processamento` = MESANO_MVM,
                  `Ano/Mes Processamento` = ANOMES_MVM,
                  `Ano Processamento` = SP_AA,
                  `Mes Processamento (Numero)` = SP_MM,
                  `Mes Processamento` = NM_MES_MVM,
                  `Quantidade Aprovada` = SP_QT_PROC,
                  `Valor Aprovado` = SP_VALATO,
                  `Quantidade de Ato` = SP_QTD_ATO,
                  `Frequencia` = SP_U_AIH,
                  `No da AIH` = SP_NAIH,
                  `Procedimentos Secundarios` = NO_PROCEDIMENTO,
                  `Grupo Proc. Secundario` = NO_GRUPO,
                  `Sub-grupo Proc. Secundario` = NO_SUB_GRUPO,
                  `Forma de organizacao Secundaria` = NO_FORMA_ORGANIZACAO,
                  `Tipo de financiamento do ato profissional` = NO_FINANCIAMENTO,
                  `Subtipo FAEC do ato profissional` = NO_SUB_FINANCIAMENTO,
                  `Complexidade do ato profissional` = COMPLEXIDADE,
                  `Tipo de Valor` = IN_TP_VAL,
                  `Ocupacao` = NO_OCUPACAO,
                  `Nome da Mobirdade (CID)` = NO_CID,
                  `Municipio Residencia` = nome_municipio,
                  `Regiao Residencia` = nome_regiao,
                  `Microrregião IBGE de Residência` = nome_microrregiao,
                  `Mesorregião IBGE de Residência` = nome_mesorregiao,
                  `Estado Residencia` = nome_estado,
                  `Estabelecimento CNES` = NO_ESTABELECIMENTO,
                  `Cod do Municipio do Estabelecimento` = SP_M_HOSP,
                  `Municipio do Estabelecimento` = municipio_estabelecimento,
                  `Regiao do Estabelecimento` = nome_regiao_estabelecimento) %>%

    dplyr::mutate_all(~ stringr::str_trim(., side = "right")) %>% #Remove espaços em branco no final dos valores
    dplyr::mutate(dplyr::across(dplyr::all_of(cols_to_convert), ~ as.numeric(.)))#Converte algumas colunas para numerico

  return(outputSIH_SP)
}
