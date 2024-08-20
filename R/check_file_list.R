
#' Checks whether the value returned by the `list_SIA_SIH_files` function is correct.
#'
#' @description A função verifica se a função `list_SIA_SIH_files` retornou um arquivo para cada mês. Se algum mês estiver sem um arquivo, a função imprime no console as datas dos meses faltantes.
#'
#' @param dir_files Um DataFrame contendo os nomes dos arquivos e a data correspondente a cada arquivo
#' @param data_type string. Valores aceitos "PA", "RD", "RJ", "SP"
#' @param state_abbr string ou vetor de strings. Sigla da Unidade Federativa
#' @param publication_date_start string. Mês de inicio para o download dos dados. No formato "AAAA-MM-01",
#' @param publication_date_end string. Mês de termino para o download dos dados. No formato "AAAA-MM-01".
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

    # Cria um vetor com todos os meses dentro do intervalo especificado.
    meses <-
      seq.Date(
        from = lubridate::floor_date(publication_date_start, "month"),
        to = lubridate::ceiling_date(publication_date_end, "month") - lubridate::days(1),
        by = "month"
      )

    meses_baixados = unique(dir_files$publication_date)
    faltando <- setdiff(as.character(meses), as.character(meses_baixados))

    if (length(faltando) > 0) {
      cat("Não foi possível baixar os dados de", data_type,"do(s) estado(s) ", state_abbr,
        "para os seguinte(s) mes(es):\n")
      print(faltando)
    }
  }
