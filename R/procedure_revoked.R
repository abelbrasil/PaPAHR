
#' Retorna informações sobre procedimentos que foram revogados
#'
#' @description Alguns procedimentos são revogados, e a partir do mês de revogação, as bases do SIGTAP deixam de fornecer informações sobre eles. Para acessar dados referentes a esses procedimentos, é necessário consultar as informações disponíveis nos meses anteriores à sua revogação.
#'
#' @param year_start numeric. Ano inicial para o download dos dados, no formato yyyy.
#' @param month_start numeric. Mês inicial para o download dos dados, no formato mm.
#' @param year_end numeric. Ano final para o download dos dados, no formato yyyy.
#' @param month_end numeric. Mês final para o download dos dados, no formato mm.
#' @param dados É o conjunto de dados retornado por uma das funções principais.
#'
#' @return A função retorna um subconjunto da base de dados de entrada, contendo exclusivamente as linhas nas quais a coluna 'Procedimentos realizados' possuía valores nulos. Os valores nulos nestas linhas são imputados com os valores correspondentes.
#'
#' @examples
#'   \dontrun{
#'     dados = procedure_revoked(
#'       year_start = 2023,
#'       month_start = 1,
#'       year_end = 2023,
#'       month_end = 3,
#'       dados = outputSIH_RD
#'     )
#'   }
#'
#' @export
procedure_revoked <-
  function(year_start,
           month_start,
           year_end,
           month_end,
           dados) {

  `%>%` <- dplyr::`%>%`

  download_sigtap_files(year_start,
                        month_start,
                        year_end,
                        month_end,
                        newer = FALSE)

  procedure_details <- get_procedure_details()

  procedure_details <-
    procedure_details %>% dplyr::select(
      CO_PROCEDIMENTO,
      NO_PROCEDIMENTO,
      NO_GRUPO,
      NO_SUB_GRUPO,
      NO_FORMA_ORGANIZACAO,
      COMPLEXIDADE,
      NO_FINANCIAMENTO,
      NO_SUB_FINANCIAMENTO
    ) %>%  dplyr::rename(
      `Procedimentos realizados` = NO_PROCEDIMENTO,
      `Grupo de Procedimentos` = NO_GRUPO,
      `SubGrupo de Procedimentos` = NO_SUB_GRUPO,
      `Forma de organizacao` = NO_FORMA_ORGANIZACAO,
      `Complexidade do Procedimento` = COMPLEXIDADE,
      `Financiamento` = NO_FINANCIAMENTO,
      `SubTp FAEC` = NO_SUB_FINANCIAMENTO
    )


  dados_na = dados[is.na(dados$`Procedimentos realizados`), ]
  dados_na = dados_na %>% dplyr::select(-`Procedimentos realizados`,
                                        -`Grupo de Procedimentos`,
                                        -`SubGrupo de Procedimentos`,
                                        -`Forma de organizacao`,
                                        -`Complexidade do Procedimento`,
                                        -`Financiamento`,
                                        -`SubTp FAEC`)

  outputSIH_RD = dados_na %>%  dplyr::left_join(
    procedure_details %>% dplyr::distinct(CO_PROCEDIMENTO, .keep_all = TRUE),
    c("CO Procedimentos realizados" = "CO_PROCEDIMENTO")
  )

  return(outputSIH_RD)

}
