
#' get_procedure_details: Estrutura os dados baixados pelo `download_sigtap_files`
#'
#' @description Esta funcao agrupa alguns arquivos baixados previamente pela funcao `download_sigtap_files`, consolidando todos os dados em uma unica tabela.
#'
#' @return Uma unica tabela contendo alguns dados baixados do SIGTAP
#' @export
#'
#' @examples
#' \dontrun{get_procedure_details()}
get_procedure_details <- function() {
  `%>%` <- dplyr::`%>%`

  # TO-DO ---------------------------------------------------------------------#

  # 1. Verificar se os arquivos SIGTAP existem


  procedure_main_details <- get_detail("Procedimento")
  procedure_group <- get_detail("Grupo")
  procedure_sub_group <- get_detail("Subgrupo")
  procedure_organization_form <- get_detail("Forma de organização")
  procedure_funding <- get_detail("Financiamento")
  procedure_rubric <- get_detail("Rubrica")

  procedure_details <- procedure_main_details %>%
    #A coluna CO_PROCEDIMENTO contem strings de 10 caracteres cada, como por exemplo: "0101010028".
    #A string da coluna CO_PROCEDIMENTO e dividida em 3 partes menores, e cada parte se transforma em uma nova coluna.
    dplyr::mutate(CO_GRUPO = stringr::str_sub(CO_PROCEDIMENTO, 1, 2),
           CO_SUB_GRUPO = stringr::str_sub(CO_PROCEDIMENTO, 3, 4),
           CO_FORMA_ORGANIZACAO = stringr::str_sub(CO_PROCEDIMENTO, 5, 6)
           ) %>%
    dplyr::left_join(procedure_group, by=c("file_version_id", "CO_GRUPO")) %>%
    dplyr::left_join(procedure_sub_group, by=c("file_version_id", "CO_GRUPO", "CO_SUB_GRUPO")) %>%
    dplyr::left_join(procedure_organization_form,
                     by=c("file_version_id", "CO_GRUPO", "CO_SUB_GRUPO", "CO_FORMA_ORGANIZACAO")) %>%
    dplyr::left_join(procedure_funding, by=c("file_version_id", "CO_FINANCIAMENTO")) %>%
    dplyr::left_join(procedure_rubric, by=c("file_version_id", "CO_RUBRICA")) %>%
    dplyr::select(-dplyr::starts_with("DT_COMPETENCIA")) %>%
    dplyr::group_by(CO_PROCEDIMENTO) %>%

    dplyr::mutate(CO_GRUPO = stringr::str_sub(CO_PROCEDIMENTO, 1, 2),
           CO_SUB_GRUPO = stringr::str_sub(CO_PROCEDIMENTO, 1, 4),
           CO_FORMA_ORGANIZACAO = stringr::str_sub(CO_PROCEDIMENTO, 1, 6),
           NO_PROCEDIMENTO = stringr::str_c(CO_PROCEDIMENTO, NO_PROCEDIMENTO, sep="-"),
           NO_GRUPO = stringr::str_c(CO_GRUPO, NO_GRUPO, sep="-"),
           NO_SUB_GRUPO = stringr::str_c(CO_SUB_GRUPO, NO_SUB_GRUPO, sep="-"),
           NO_FINANCIAMENTO = stringr::str_c(CO_FINANCIAMENTO, NO_FINANCIAMENTO, sep="-"),
           NO_FORMA_ORGANIZACAO = stringr::str_c(CO_FORMA_ORGANIZACAO, NO_FORMA_ORGANIZACAO, sep="-"),
           NO_SUB_FINANCIAMENTO = stringr::str_c(CO_RUBRICA, NO_RUBRICA, sep="-"),
           COMPLEXIDADE = dplyr::case_when(TP_COMPLEXIDADE == 0 ~ "Nao se Aplica",
                                    TP_COMPLEXIDADE == 1 ~ "Atencao Basica",
                                    TP_COMPLEXIDADE == 2 ~ "Media Complexidade",
                                    TP_COMPLEXIDADE == 3 ~ "Alta Complexidade"),
           COMPLEXIDADE = stringr::str_c(TP_COMPLEXIDADE, COMPLEXIDADE, sep="-"))

  return(procedure_details)
}

