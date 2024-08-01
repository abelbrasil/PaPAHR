
#' check_file_list: Verifica se uma lista de nomes de arquivos está completa
#'
#' @description
#' A função check_file_list verifica se a função list_SIA_SIH_files retornou um arquivo para cada mês. Se algum mês estiver sem arquivo, a função imprime no console as datas dos meses faltantes.
#'
#' @param dir_files Um DataFrame contendo os nomes dos arquivos e a data correspondente a cada arquivo
#' @param data_type String. Valores aceitos "PA", "RD", "RJ", "SP", ou um vetor `c("RD", "RJ")`
#' @param state_abbr String. Sigla da Unidade Federativa
#' @param publication_date_start Uma string no formato "AAAA-MM-01", indicando o mes de inicio para o download dos dados.
#' @param publication_date_end Uma string no formato "AAAA-MM-01", indicando o mes de termino para o download dos dados.
#'
#' @examples \dontrun{
#'   check_file_list(
#'     dir_files = dir_files,
#'     data_type = "PA",
#'     state_abbr = "CE",
#'     publication_date_start = "2023-01-01",
#'     publication_date_end = "2023-03-01"
#'   )
#' }
#'
#' @export
check_file_list <-
  function(dir_files,
           data_type,
           state_abbr,
           publication_date_start,
           publication_date_end) {

    # Crie um vetor com todos os meses do intervalo
    meses <-
      seq.Date(
        from = lubridate::floor_date(publication_date_start, "month"),
        to = lubridate::ceiling_date(publication_date_end, "month") - lubridate::days(1),
        by = "month"
      )

    meses_baixados = unique(dir_files$publication_date)
    faltando <- setdiff(as.character(meses), as.character(meses_baixados))

    if (length(faltando) > 0) {
      cat("Não foi possível baixar os dados de", data_type,"do estado de", state_abbr,
        "para os seguinte(s) mes(es):\n")
      print(faltando)
    }
  }
