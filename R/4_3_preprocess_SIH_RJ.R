
#' Returns SIH-AIH Rejected(RJ) data in a structured and filtered form.
#'
#' @description Estrutura e filtra os dados da Autorização de Internação Hospitalar (AIH) Rejeitadas (RJ) e combina as informações do CNES, SIGTAP e da base de dados `counties`.
#'
#' @param cid É a tabela retornada pela função `get_details` quando o parâmetro `detail_name='CID'`
#' @param raw_SIH_RJ Dados de Autorização de Internação Hospitalar (AIH) Rejeitadas (RJ) do Sistema de Informação Hospitalar (SIH)
#' @param county_id string ou vetor de strings. Código do Município de Atendimento.
#' @param procedure_details São os dados retornados pelo função `get_procedure_details`
#' @param health_establishment_id string ou vetor de strings. Código do estabelecimento de saúde.
#'
#' @return Retorna a tabela da Autorização de Internação Hospitalar (AIH) Rejeitadas (RJ) já filtrada e tratada.
#'
#' @export
preprocess_SIH_RJ <-
  function(cid,
           raw_SIH_RJ,
           county_id,
           procedure_details,
           health_establishment_id){
  `%>%` <- dplyr::`%>%`

  municipios = counties %>%
    dplyr::select(id_municipio, nome_municipio) %>%
    dplyr::rename(municipio_estabelecimento = nome_municipio)

  cols_to_convert = c(
    "Frequencia",
    "Valor Total"
  )

  outputSIH_RJ <- raw_SIH_RJ %>%
    tibble::as_tibble() %>%
    check_filter(var_value = county_id, var_name = "MUNIC_MOV") %>%
    check_filter(var_value=health_establishment_id, var_name="CNES") %>%
    dplyr::mutate(TIPO = "Rejeitada",
                  DT_CMPT = lubridate::ym(stringr::str_c(ANO_CMPT, MES_CMPT, sep="-")),
                  NM_MES_CMPT = stringr::str_to_title(lubridate::month(DT_CMPT, label=TRUE, abbr=FALSE)),
                  NM_MES_CMPT = stringr::str_glue("{sprintf('%02d', lubridate::month(DT_CMPT))} - {NM_MES_CMPT}"),
                  QTD_AIH = 1,
                  ANOMES_CMPT = format(DT_CMPT, "%Y%m")
    ) %>%
    dplyr::left_join(counties, by=c("MUNIC_RES" = "id_municipio")) %>%
    dplyr::left_join(municipios, by = c("MUNIC_MOV" = "id_municipio")) %>%
    dplyr::left_join(procedure_details,
                     c("PROC_REA" = "CO_PROCEDIMENTO", "ANOMES_CMPT" = "file_version_id")) %>%
    dplyr::left_join(cid, by=c("DIAG_PRINC" = "CO_CID","ANOMES_CMPT" = "file_version_id")) %>%
    dplyr::left_join(health_establishment, by="CNES") %>%
    dplyr::mutate(NO_CID = dplyr::if_else(is.na(NO_CID), "0000-Nao informado", NO_CID),
                  dplyr::across(c(nome_estado, nome_microrregiao, nome_mesorregiao, nome_municipio),
                                ~ dplyr::case_when(MUNIC_RES != 999999 ~ .x,
                                                   MUNIC_RES == 999999 ~ "Nao informado"))) %>%

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
                  `Regiao Residencia` = nome_regiao,
                  `Estado de Residencia` = nome_estado,
                  `Mesorregiao IBGE de Resid.` = nome_mesorregiao,
                  `Microrregiao IBGE de Resid.` = nome_microrregiao,
                  `Hospital (CNES)` = NO_ESTABELECIMENTO,
                  `Cod do Municipio do Estabelecimento` = MUNIC_MOV,
                  `Municipio do Estabelecimento` = municipio_estabelecimento)%>%

    dplyr::mutate_all(~ stringr::str_trim(., side = "right")) %>%  #Remove espaços em branco no final dos valores

  dplyr::mutate(dplyr::across(dplyr::all_of(cols_to_convert), ~ as.numeric(.)))#Converte algumas colunas para numerico

  return(outputSIH_RJ)
}
