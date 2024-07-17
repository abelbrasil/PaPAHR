
#' check_filter
#'
#' @description Funcao auxiliar para filtrar uma lista ou todos; Funcao para filtrar pelo CNES ou pelo municipio
#'
#' @param df um DataFrame que sera filtrado.
#' @param var_value String, Codigo(s) do estabelecimento de saude
#' @param var_name String, nome da coluna no data frame que sera usada para a filtragem.
#'
#' @return filtered_df
#' @export
check_filter <- function(df, var_value, var_name) {

  if (length(var_value) == 1) {
    if (var_value == "all") {

      return(df)
    } else {
      filtered_df <- dplyr::filter(df, .data[[var_name]] == var_value)

      return(filtered_df)
    }
  } else {
    filtered_df <- dplyr::filter(df, .data[[var_name]] %in% var_value)

    return(filtered_df)
  }
}


